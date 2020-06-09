// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
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
)

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
		if manager.hasTable[tbl.Table.ID] {
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
	return nil
}

func splitPostWork(ctx context.Context, client *Client, tables []*model.TableInfo) {
	err := client.ResetPlacementRules(ctx, tables)
	if err != nil {
		log.Warn("reset placement rules failed", zap.Error(err))
		return
	}

	err = client.ResetRestoreLabels(ctx)
	if err != nil {
		log.Warn("reset store labels failed", zap.Error(err))
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

// SetThreshold sets the threshold that how big the batch size reaching need to send batch.
// note this function isn't goroutine safe yet,
// just set threshold before anything starts(e.g. EnableAutoCommit), please.
func (b *Batcher) SetThreshold(newThreshold int) {
	b.batchSizeThreshold = newThreshold
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
			// so we just consume the current backlog of this cannel, then return.
			return out
		}
	}
}

// BatchSender is the abstract of how the batcher send a batch.
type BatchSender interface {
	// RestoreBatch will send the restore request.
	RestoreBatch(ctx context.Context, ranges []rtree.Range, rewriteRules *RewriteRules) error
	Close()
}

type tikvSender struct {
	client         *Client
	updateCh       glue.Progress
	rejectStoreMap map[uint64]bool
}

// NewTiKVSender make a sender that send restore requests to TiKV.
func NewTiKVSender(ctx context.Context, cli *Client, updateCh glue.Progress) (BatchSender, error) {
	tiflashStores, err := conn.GetAllTiKVStores(ctx, cli.GetPDClient(), conn.TiFlashOnly)
	if err != nil {
		// After TiFlash support restore, we can remove this panic.
		// The origin of this panic is at RunRestore, and its semantic is nearing panic, don't worry about it.
		log.Error("failed to get and remove TiFlash replicas", zap.Error(errors.Trace(err)))
		return nil, err
	}
	rejectStoreMap := make(map[uint64]bool)
	for _, store := range tiflashStores {
		rejectStoreMap[store.GetId()] = true
	}

	return &tikvSender{
		client:         cli,
		updateCh:       updateCh,
		rejectStoreMap: rejectStoreMap,
	}, nil
}

func (b *tikvSender) RestoreBatch(ctx context.Context, ranges []rtree.Range, rewriteRules *RewriteRules) error {
	if err := SplitRanges(ctx, b.client, ranges, rewriteRules, b.updateCh); err != nil {
		log.Error("failed on split range",
			zap.Any("ranges", ranges),
			zap.Error(err),
		)
		return err
	}

	files := []*backup.File{}
	for _, fs := range ranges {
		files = append(files, fs.Files...)
	}

	if err := b.client.RestoreFiles(files, rewriteRules, b.rejectStoreMap, b.updateCh); err != nil {
		return err
	}
	totalKV := uint64(0)
	totalSize := uint64(0)
	for _, f := range files {
		totalKV += f.GetTotalKvs()
		totalSize += f.GetTotalBytes()
	}

	log.Debug("send batch done",
		zap.Int("ranges", len(ranges)),
		zap.Int("file count", len(files)),
		zap.Uint64("total kv", totalKV),
		zap.Uint64("total size", totalSize),
	)

	return nil
}

func (b *tikvSender) Close() {
	// don't close update channel here, since we may need it then.
}
