// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/utils"
)

const (
	defaultBatcherOutputChannelSize = 1024
	midChBufferSize                 = 8
)

// TableSink is the 'sink' of restored data by a sender.
type TableSink interface {
	EmitTables(tables ...CreatedTable)
	EmitError(error)
	Close()
}

type chanTableSink struct {
	outCh chan<- []CreatedTable
	errCh chan<- error
}

func (sink chanTableSink) EmitTables(tables ...CreatedTable) {
	sink.outCh <- tables
}

func (sink chanTableSink) EmitError(err error) {
	sink.errCh <- err
}

func (sink chanTableSink) Close() {
	// ErrCh may has multi sender part, don't close it.
	close(sink.outCh)
}

// ContextManager is the struct to manage a TiKV 'context' for restore.
// Batcher will call Enter when any table should be restore on batch,
// so you can do some prepare work here(e.g. set placement rules for online restore).
type ContextManager interface {
	// Enter make some tables 'enter' this context(a.k.a., prepare for restore).
	Enter(ctx context.Context, tables []CreatedTable) error
	// Leave make some tables 'leave' this context(a.k.a., restore is done, do some post-works).
	Leave(ctx context.Context, tables []CreatedTable) error
}

// NewBRContextManager makes a BR context manager, that is,
// set placement rules for online restore when enter(see <splitPrepareWork>),
// unset them when leave.
func NewBRContextManager(client *Client) ContextManager {
	return &brContextManager{
		client: client,

		hasTable: make(map[int64]bool),
	}
}

type brContextManager struct {
	client *Client

	// This 'set' of table ID allow us handle each table just once.
	hasTable map[int64]bool
}

func (manager *brContextManager) Enter(ctx context.Context, tables []CreatedTable) error {
	placementRuleTables := make([]*model.TableInfo, 0, len(tables))

	for _, tbl := range tables {
		if !manager.hasTable[tbl.Table.ID] {
			placementRuleTables = append(placementRuleTables, tbl.Table)
		}
		manager.hasTable[tbl.Table.ID] = true
	}

	return splitPrepareWork(ctx, manager.client, placementRuleTables)
}

func (manager *brContextManager) Leave(ctx context.Context, tables []CreatedTable) error {
	placementRuleTables := make([]*model.TableInfo, 0, len(tables))

	for _, table := range tables {
		placementRuleTables = append(placementRuleTables, table.Table)
	}

	splitPostWork(ctx, manager.client, placementRuleTables)
	log.Info("restore table done", ZapTables(tables))
	return nil
}

func splitPostWork(ctx context.Context, client *Client, tables []*model.TableInfo) {
	err := client.ResetPlacementRules(ctx, tables)
	if err != nil {
		log.Warn("reset placement rules failed", zap.Error(err))
		return
	}
}

func splitPrepareWork(ctx context.Context, client *Client, tables []*model.TableInfo) error {
	err := client.SetupPlacementRules(ctx, tables)
	if err != nil {
		log.Error("setup placement rules failed", zap.Error(err))
		return errors.Trace(err)
	}

	err = client.WaitPlacementSchedule(ctx, tables)
	if err != nil {
		log.Error("wait placement schedule failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// CreatedTable is a table created on restore process,
// but not yet filled with data.
type CreatedTable struct {
	RewriteRule *RewriteRules
	Table       *model.TableInfo
	OldTable    *utils.Table
}

// TableWithRange is a CreatedTable that has been bind to some of key ranges.
type TableWithRange struct {
	CreatedTable

	Range []rtree.Range
}

// Exhaust drains all remaining errors in the channel, into a slice of errors.
func Exhaust(ec <-chan error) []error {
	out := make([]error, 0, len(ec))
	for {
		select {
		case err := <-ec:
			out = append(out, err)
		default:
			// errCh will NEVER be closed(ya see, it has multi sender-part),
			// so we just consume the current backlog of this channel, then return.
			return out
		}
	}
}

// BatchSender is the abstract of how the batcher send a batch.
type BatchSender interface {
	// PutSink sets the sink of this sender, user to this interface promise
	// call this function at least once before first call to `RestoreBatch`.
	PutSink(sink TableSink)
	// RestoreBatch will send the restore request.
	RestoreBatch(ranges DrainResult)
	Close()
}

type tikvSender struct {
	client         *Client
	updateCh       glue.Progress
	rejectStoreMap map[uint64]bool

	sink TableSink
	inCh chan<- DrainResult

	wg *sync.WaitGroup
}

func (b *tikvSender) PutSink(sink TableSink) {
	// don't worry about visibility, since we will call this before first call to
	// RestoreBatch, which is a sync point.
	b.sink = sink
}

func (b *tikvSender) RestoreBatch(ranges DrainResult) {
	b.inCh <- ranges
}

// NewTiKVSender make a sender that send restore requests to TiKV.
func NewTiKVSender(
	ctx context.Context,
	cli *Client,
	updateCh glue.Progress,
	// TODO remove this field after we support TiFlash.
	removeTiFlash bool,
) (BatchSender, error) {
	rejectStoreMap := make(map[uint64]bool)
	if removeTiFlash {
		tiflashStores, err := conn.GetAllTiKVStores(ctx, cli.GetPDClient(), conn.TiFlashOnly)
		if err != nil {
			log.Error("failed to get and remove TiFlash replicas", zap.Error(err))
			return nil, err
		}
		for _, store := range tiflashStores {
			rejectStoreMap[store.GetId()] = true
		}
	}
	inCh := make(chan DrainResult, 1)
	midCh := make(chan DrainResult, midChBufferSize)

	sender := &tikvSender{
		client:         cli,
		updateCh:       updateCh,
		rejectStoreMap: rejectStoreMap,
		inCh:           inCh,
		wg:             new(sync.WaitGroup),
	}

	sender.wg.Add(2)
	go sender.splitWorker(ctx, inCh, midCh)
	go sender.restoreWorker(ctx, midCh)
	return sender, nil
}

func (b *tikvSender) splitWorker(ctx context.Context, ranges <-chan DrainResult, next chan<- DrainResult) {
	defer log.Debug("split worker closed")
	defer func() {
		b.wg.Done()
		close(next)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-ranges:
			if !ok {
				return
			}
			if err := SplitRanges(ctx, b.client, result.Ranges, result.RewriteRules, b.updateCh); err != nil {
				log.Error("failed on split range",
					zap.Any("ranges", ranges),
					zap.Error(err),
				)
				b.sink.EmitError(err)
				return
			}
			next <- result
		}
	}
}

func (b *tikvSender) restoreWorker(ctx context.Context, ranges <-chan DrainResult) {
	defer func() {
		log.Debug("restore worker closed")
		b.wg.Done()
		b.sink.Close()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-ranges:
			if !ok {
				return
			}
			files := result.Files()
			if err := b.client.RestoreFiles(files, result.RewriteRules, b.rejectStoreMap, b.updateCh); err != nil {
				b.sink.EmitError(err)
				return
			}

			log.Info("restore batch done",
				ZapRanges(result.Ranges),
				zap.Int("file count", len(files)),
			)
			b.sink.EmitTables(result.BlankTablesAfterSend...)
		}
	}
}

func (b *tikvSender) Close() {
	close(b.inCh)
	b.wg.Wait()
	log.Debug("tikv sender closed")
}
