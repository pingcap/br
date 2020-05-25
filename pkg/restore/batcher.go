// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/rtree"
)

// Batcher collects ranges to restore and send batching split/ingest request.
type Batcher struct {
	cachedTables   []TableWithRange
	cachedTablesMu *sync.Mutex
	rewriteRules   *RewriteRules

	// joiner is for joining the background batch sender.
	joiner             chan<- struct{}
	sendErr            chan<- error
	outCh              chan<- CreatedTable
	sender             BatchSender
	batchSizeThreshold int
	size               int32
}

// Len calculate the current size of this batcher.
func (b *Batcher) Len() int {
	return int(atomic.LoadInt32(&b.size))
}

// NewBatcher creates a new batcher by client and updateCh.
// this batcher will work background, send batches per second, or batch size reaches limit.
// and it will emit full-restored tables to the output channel returned.
func NewBatcher(
	sender BatchSender,
	errCh chan<- error,
) (*Batcher, <-chan CreatedTable) {
	output := make(chan CreatedTable, defaultBatcherOutputChannelSize)
	b := &Batcher{
		rewriteRules:       EmptyRewriteRule(),
		sendErr:            errCh,
		outCh:              output,
		sender:             sender,
		cachedTablesMu:     new(sync.Mutex),
		batchSizeThreshold: 1,
	}
	return b, output
}

// EnableAutoCommit enables the batcher commit batch periodicity even batcher size isn't big enough.
// we make this function for disable AutoCommit in some case.
func (b *Batcher) EnableAutoCommit(ctx context.Context, delay time.Duration) {
	if b.joiner != nil {
		log.Warn("enable auto commit on a batcher that is enabled auto commit, nothing will happen")
		log.Info("if desire, please disable auto commit firstly")
	}
	joiner := make(chan struct{})
	go b.workLoop(ctx, joiner, delay)
	b.joiner = joiner
}

// DisableAutoCommit blocks the current goroutine until the worker can gracefully stop,
// and then disable auto commit.
func (b *Batcher) DisableAutoCommit(ctx context.Context) {
	b.joinWorker()
	b.joiner = nil
}

// joinWorker blocks the current goroutine until the worker can gracefully stop.
// return immediately when auto commit disabled.
func (b *Batcher) joinWorker() {
	if b.joiner != nil {
		log.Info("gracefully stoping worker goroutine")
		b.joiner <- struct{}{}
		log.Info("gracefully stopped worker goroutine")
	}
}

func (b *Batcher) workLoop(ctx context.Context, joiner <-chan struct{}, delay time.Duration) {
	tick := time.NewTicker(delay)
	defer tick.Stop()
	for {
		select {
		case <-joiner:
			log.Debug("graceful stop signal received")
			return
		case <-ctx.Done():
			b.sendErr <- ctx.Err()
			return
		case <-tick.C:
			if b.Len() > 0 {
				log.Info("sending batch because time limit exceed", zap.Int("size", b.Len()))
				b.asyncSend(ctx)
			}
		}
	}
}

func (b *Batcher) asyncSend(ctx context.Context) {
	tbls, err := b.Send(ctx)
	if err != nil {
		b.sendErr <- err
		return
	}
	for _, t := range tbls {
		b.outCh <- t
	}
}

type drainResult struct {
	// TablesToSend are tables that would be send at this batch.
	TablesToSend []CreatedTable
	// BlankTablesAfterSend are tables that will be full-restored after this batch send.
	BlankTablesAfterSend []CreatedTable
	RewriteRules         *RewriteRules
	Ranges               []rtree.Range
}

func newDrainResult() drainResult {
	return drainResult{
		TablesToSend:         make([]CreatedTable, 0),
		BlankTablesAfterSend: make([]CreatedTable, 0),
		RewriteRules:         EmptyRewriteRule(),
		Ranges:               make([]rtree.Range, 0),
	}
}

func (b *Batcher) drainRanges() drainResult {
	result := newDrainResult()

	b.cachedTablesMu.Lock()
	defer b.cachedTablesMu.Unlock()

	for offset, thisTable := range b.cachedTables {
		thisTableLen := len(thisTable.Range)
		collected := len(result.Ranges)

		result.RewriteRules.Append(*thisTable.RewriteRule)
		result.TablesToSend = append(result.TablesToSend, thisTable.CreatedTable)

		// the batch is full, we should stop here!
		// we use strictly greater than because when we send a batch at equal, the offset should plus one.
		// (because the last table is sent, we should put it in emptyTables), and this will intrduce extra complex.
		if thisTableLen+collected > b.batchSizeThreshold {
			drainSize := b.batchSizeThreshold - collected
			thisTableRanges := thisTable.Range

			var drained []rtree.Range
			drained, b.cachedTables[offset].Range = thisTableRanges[:drainSize], thisTableRanges[drainSize:]
			log.Debug("draining partial table to batch",
				zap.Stringer("table", thisTable.Table.Name),
				zap.Stringer("database", thisTable.OldTable.Db.Name),
				zap.Int("size", thisTableLen),
				zap.Int("drained", drainSize),
			)
			result.Ranges = append(result.Ranges, drained...)
			b.cachedTables = b.cachedTables[offset:]
			atomic.AddInt32(&b.size, -int32(len(drained)))
			return result
		}

		result.BlankTablesAfterSend = append(result.BlankTablesAfterSend, thisTable.CreatedTable)
		// let's 'drain' the ranges of current table. This op must not make the batch full.
		result.Ranges = append(result.Ranges, thisTable.Range...)
		// let's reduce the batcher size each time, to make a consistence of batcher's size.
		atomic.AddInt32(&b.size, -int32(len(thisTable.Range)))
		// clear the table length.
		b.cachedTables[offset].Range = []rtree.Range{}
		log.Debug("draining table to batch",
			zap.Stringer("table", thisTable.Table.Name),
			zap.Stringer("database", thisTable.OldTable.Db.Name),
			zap.Int("size", thisTableLen),
		)
	}

	// all tables are drained.
	b.cachedTables = []TableWithRange{}
	return result
}

// Send sends all pending requests in the batcher.
// returns tables sent FULLY in the current batch.
func (b *Batcher) Send(ctx context.Context) ([]CreatedTable, error) {
	drainResult := b.drainRanges()
	tbs := drainResult.TablesToSend
	ranges := drainResult.Ranges
	tableNames := make([]string, 0, len(tbs))
	for _, t := range tbs {
		tableNames = append(tableNames, fmt.Sprintf("%s.%s", t.OldTable.Db.Name, t.OldTable.Info.Name))
	}
	log.Debug("do batch send",
		zap.Strings("tables", tableNames),
		zap.Int("ranges", len(ranges)),
	)
	if err := b.sender.RestoreBatch(ctx, ranges, drainResult.RewriteRules); err != nil {
		return nil, err
	}
	return drainResult.BlankTablesAfterSend, nil
}

func (b *Batcher) sendIfFull(ctx context.Context) {
	// never collect the send batch request message.
	for b.Len() >= b.batchSizeThreshold {
		log.Info("sending batch because batcher is full", zap.Int("size", b.Len()))
		b.asyncSend(ctx)
	}
}

// Add adds a task to the Batcher.
func (b *Batcher) Add(ctx context.Context, tbs TableWithRange) {
	b.cachedTablesMu.Lock()
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
	atomic.AddInt32(&b.size, int32(len(tbs.Range)))
	b.cachedTablesMu.Unlock()

	b.sendIfFull(ctx)
}

// Close closes the batcher, sending all pending requests, close updateCh.
func (b *Batcher) Close(ctx context.Context) {
	log.Info("sending batch lastly on close.", zap.Int("size", b.Len()))
	for b.Len() > 0 {
		b.asyncSend(ctx)
	}
	b.DisableAutoCommit(ctx)
	close(b.outCh)
	b.sender.Close()
}
