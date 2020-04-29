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
	currentBatch []rtree.Range
	cachedTables []TableWithRange
	rewriteRules *RewriteRules

	ctx                context.Context
	client             *Client
	sender             chan<- struct{}
	sendErr            chan<- error
	outCh              chan<- TableWithRange
	rejectStoreMap     map[uint64]bool
	updateCh           glue.Progress
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
	return len(b.currentBatch)
}

// NewBatcher creates a new batcher by client and updateCh.
// this batcher will work background, send batches per second, or batch size reaches limit.
func NewBatcher(
	ctx context.Context,
	client *Client,
	updateCh glue.Progress,
	errCh chan<- error,
) (*Batcher, <-chan TableWithRange) {
	tiflashStores, err := conn.GetAllTiKVStores(ctx, client.GetPDClient(), conn.TiFlashOnly)
	if err != nil {
		// After TiFlash support restore, we can remove this panic.
		// The origin of this panic is at RunRestore, and its semantic is nearing panic, don't worry about it.
		log.Panic("failed to get and remove TiFlash replicas", zap.Error(errors.Trace(err)))
	}
	rejectStoreMap := make(map[uint64]bool)
	for _, store := range tiflashStores {
		rejectStoreMap[store.GetId()] = true
	}
	// use block channel here for forbid send table unexpectly.
	mailbox := make(chan struct{})
	output := make(chan TableWithRange, defaultBatcherOutputChannelSize)
	b := &Batcher{
		rewriteRules:       EmptyRewriteRule(),
		client:             client,
		rejectStoreMap:     rejectStoreMap,
		updateCh:           updateCh,
		sender:             mailbox,
		sendErr:            errCh,
		outCh:              output,
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

// drainSentTables drains the table just sent.
// note that this function assumes you call it only after a sent of bench.
// WARN: we make a very strong assertion here: any time we will just 'split' at the last table.
// NOTE: if you meet a problem like 'failed to checksum' when everything is alright, check this.
// TODO: remove Batcher::currentBatch, collect currentBatch each time when call this.
func (b *Batcher) drainSentTables() (drained []TableWithRange) {
	if b.Len() == 0 {
		drained, b.cachedTables = b.cachedTables, []TableWithRange{}
		return
	}
	cachedLen := len(b.cachedTables)
	drained, b.cachedTables = b.cachedTables[:cachedLen-1], b.cachedTables[cachedLen-1:]
	return
}

// Send sends all pending requests in the batcher.
// returns tables sent in the current batch.
func (b *Batcher) Send() ([]TableWithRange, error) {
	var ranges []rtree.Range
	ranges, b.currentBatch = b.currentBatch[:b.splitPoint()], b.currentBatch[b.splitPoint():]
	tbs := b.drainSentTables()
	var tableNames []string

	if err := SplitRanges(b.ctx, b.client, ranges, b.rewriteRules, b.updateCh); err != nil {
		log.Error("failed on split range",
			zap.Any("ranges", ranges),
			zap.Error(err),
		)
		return nil, err
	}

	files := []*backup.File{}
	for _, fs := range ranges {
		files = append(files, fs.Files...)
	}

	for _, t := range tbs {
		tableNames = append(tableNames, fmt.Sprintf("%s.%s", t.OldTable.Db.Name, t.OldTable.Info.Name))
	}
	log.Debug("split range by tables done", zap.Strings("tables", tableNames))

	if err := b.client.RestoreFiles(files, b.rewriteRules, b.rejectStoreMap, b.updateCh); err != nil {
		return nil, err
	}
	log.Debug("send batch done",
		zap.Int("range count", len(ranges)),
		zap.Int("file count", len(files)),
	)
	return tbs, nil
}

func (b *Batcher) sendIfFull() {
	if b.Len() >= b.BatchSizeThreshold {
		b.sender <- struct{}{}
	}
}

// Add addes a task to bather.
func (b *Batcher) Add(tbs TableWithRange) {
	log.Debug("adding table to batch",
		zap.Stringer("table", tbs.Table.Name),
		zap.Stringer("database", tbs.OldTable.Db.Name),
		zap.Int64("old id", tbs.OldTable.Info.ID),
		zap.Int64("new id", tbs.Table.ID),
		zap.Int("batch size", b.Len()),
	)
	b.currentBatch = append(b.currentBatch, tbs.Range...)
	b.cachedTables = append(b.cachedTables, tbs)
	b.rewriteRules.Append(*tbs.RewriteRule)
}

// Close closes the batcher, sending all pending requests, close updateCh.
func (b *Batcher) Close() {
	b.asyncSend()
	close(b.outCh)
	close(b.sender)
	b.updateCh.Close()
}
