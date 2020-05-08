// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"fmt"
	"time"

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

// CreatedTable is a table is created on restore process,
// but not yet filled by data.
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

// Batcher collectes ranges to restore and send batching split/ingest request.
type Batcher struct {
	cachedTables []TableWithRange
	rewriteRules *RewriteRules

	ctx                context.Context
	sendTrigger        chan<- struct{}
	sendErr            chan<- error
	outCh              chan<- CreatedTable
	sender             BatchSender
	BatchSizeThreshold int
}

// Exhasut drains all remaining errors in the channel, into a slice of errors.
func Exhasut(ec <-chan error) []error {
	out := make([]error, 0, len(ec))
	select {
	case err := <-ec:
		out = append(out, err)
	default:
	}
	return out
}

// Len calculate the current size of this batcher.
func (b *Batcher) Len() int {
	result := 0
	for _, tbl := range b.cachedTables {
		result += len(tbl.Range)
	}
	return result
}

// BatchSender is the abstract of how the batcher send a batch.
type BatchSender interface {
	// RestoreBatch will backup all ranges and tables
	RestoreBatch(ranges []rtree.Range, rewriteRules *RewriteRules, tbs []CreatedTable) error
	Close()
}

type tikvSender struct {
	client         *Client
	updateCh       glue.Progress
	ctx            context.Context
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
		ctx:            ctx,
		rejectStoreMap: rejectStoreMap,
	}, nil
}

func (b *tikvSender) RestoreBatch(ranges []rtree.Range, rewriteRules *RewriteRules, tbs []CreatedTable) error {
	if err := SplitRanges(b.ctx, b.client, ranges, rewriteRules, b.updateCh); err != nil {
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

	var tableNames []string
	for _, t := range tbs {
		tableNames = append(tableNames, fmt.Sprintf("%s.%s", t.OldTable.Db.Name, t.OldTable.Info.Name))
	}
	log.Debug("split range by tables done", zap.Strings("tables", tableNames))

	if err := b.client.RestoreFiles(files, rewriteRules, b.rejectStoreMap, b.updateCh); err != nil {
		return err
	}
	log.Debug("send batch done",
		zap.Int("range count", len(ranges)),
		zap.Int("file count", len(files)),
	)

	return nil
}

func (b *tikvSender) Close() {
	b.updateCh.Close()
}

// NewBatcher creates a new batcher by client and updateCh.
// this batcher will work background, send batches per second, or batch size reaches limit.
// and it will emit full-restored tables to the output channel returned.
func NewBatcher(
	ctx context.Context,
	sender BatchSender,
	errCh chan<- error,
) (*Batcher, <-chan CreatedTable) {
	// use block channel here for forbid send table unexpectly.
	mailbox := make(chan struct{})
	output := make(chan CreatedTable, defaultBatcherOutputChannelSize)
	b := &Batcher{
		rewriteRules:       EmptyRewriteRule(),
		sendTrigger:        mailbox,
		sendErr:            errCh,
		outCh:              output,
		sender:             sender,
		ctx:                ctx,
		BatchSizeThreshold: 1,
	}
	go b.workLoop(mailbox)
	return b, output
}

func (b *Batcher) workLoop(mailbox <-chan struct{}) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-b.ctx.Done():
			b.sendErr <- b.ctx.Err()
			return
		case _, ok := <-mailbox:
			if !ok {
				return
			}
			log.Info("sending batch because batcher is full", zap.Int("size", b.Len()))
			b.asyncSend()
		case <-tick.C:
			if b.Len() > 0 {
				log.Info("sending batch because time limit exceed", zap.Int("size", b.Len()))
				b.asyncSend()
			}
		}
	}
}

func (b *Batcher) asyncSend() {
	tbls, err := b.Send()
	if err != nil {
		b.sendErr <- err
		return
	}
	for _, t := range tbls {
		b.outCh <- t
	}
}

func (b *Batcher) splitPoint() int {
	splitPoint := b.BatchSizeThreshold
	if splitPoint > b.Len() {
		return b.Len()
	}
	return splitPoint
}

func (b *Batcher) drainRanges() (ranges []rtree.Range, emptyTables []CreatedTable) {
	for offset, thisTable := range b.cachedTables {
		thisTableLen := len(thisTable.Range)
		collected := len(ranges)

		// the batch is full, we should stop here!
		// we use strictly greater than because when we send a batch at equal, the offset should plus one.
		// (because the last table is sent, we should put it in emptyTables), and this will intrduce extra complex.
		if thisTableLen+collected > b.BatchSizeThreshold {
			drainSize := b.BatchSizeThreshold - collected
			thisTableRanges := thisTable.Range

			var drained []rtree.Range
			drained, b.cachedTables[offset].Range = thisTableRanges[:drainSize], thisTableRanges[drainSize:]
			log.Debug("draining partial table to batch",
				zap.Stringer("table", thisTable.Table.Name),
				zap.Stringer("database", thisTable.OldTable.Db.Name),
				zap.Int("size", thisTableLen),
				zap.Int("drained", drainSize),
			)
			ranges = append(ranges, drained...)
			b.cachedTables = b.cachedTables[offset:]
			return
		}

		emptyTables = append(emptyTables, b.cachedTables[offset].CreatedTable)
		// let 'drain' the ranges of current table. This op must not make the batch full.
		ranges = append(ranges, b.cachedTables[offset].Range...)
		// clear the table
		b.cachedTables[offset].Range = []rtree.Range{}
		log.Debug("draining table to batch",
			zap.Stringer("table", thisTable.Table.Name),
			zap.Stringer("database", thisTable.OldTable.Db.Name),
			zap.Int("size", thisTableLen),
		)
	}

	// all tables are drained.
	b.cachedTables = []TableWithRange{}
	return
}

// Send sends all pending requests in the batcher.
// returns tables sent in the current batch.
func (b *Batcher) Send() ([]CreatedTable, error) {
	ranges, tbs := b.drainRanges()
	if err := b.sender.RestoreBatch(ranges, b.rewriteRules, tbs); err != nil {
		return nil, err
	}
	return tbs, nil
}

func (b *Batcher) sendIfFull() {
	if b.Len() >= b.BatchSizeThreshold {
		b.sendTrigger <- struct{}{}
	}
}

// Add addes a task to batcher.
func (b *Batcher) Add(tbs TableWithRange) {
	log.Debug("adding table to batch",
		zap.Stringer("table", tbs.Table.Name),
		zap.Stringer("database", tbs.OldTable.Db.Name),
		zap.Int64("old id", tbs.OldTable.Info.ID),
		zap.Int64("new id", tbs.Table.ID),
		zap.Int("table size", len(tbs.Range)),
		zap.Int("batch size", b.Len()),
	)
	b.cachedTables = append(b.cachedTables, tbs)
	b.rewriteRules.Append(*tbs.RewriteRule)
}

// Close closes the batcher, sending all pending requests, close updateCh.
func (b *Batcher) Close() {
	log.Info("sending batch lastly on close.", zap.Int("size", b.Len()))
	b.asyncSend()
	close(b.outCh)
	close(b.sendTrigger)
	b.sender.Close()
}
