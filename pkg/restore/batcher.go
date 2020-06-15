// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/rtree"
)

// SendType is the 'type' of a send.
// when we make a 'send' command to worker, we may want to flush all pending ranges (when auto commit enabled),
// or, we just want to clean overflowing ranges(when just adding a table to batcher).
type SendType int

const (
	// SendUntilLessThanBatch will make the batcher send batch until
	// its remaining range is less than its batchSizeThreshold.
	SendUntilLessThanBatch SendType = iota
	// SendAll will make the batcher send all pending ranges.
	SendAll
	// SendAllThenClose will make the batcher send all pending ranges and then close itself.
	SendAllThenClose
)

// Batcher collects ranges to restore and send batching split/ingest request.
type Batcher struct {
	cachedTables   []TableWithRange
	cachedTablesMu *sync.Mutex
	rewriteRules   *RewriteRules

	// autoCommitJoiner is for joining the background batch sender.
	autoCommitJoiner chan<- struct{}
	// everythingIsDone is for waiting for worker done: that is, after we send a
	// signal to autoCommitJoiner, we must give it enough time to get things done.
	// Then, it should notify us by this waitgroup.
	// Use waitgroup instead of a trivial channel for further extension.
	everythingIsDone *sync.WaitGroup
	// sendErr is for output error information.
	sendErr chan<- error
	// sendCh is for communiate with sendWorker.
	sendCh chan<- SendType
	// outCh is for output the restored table, so it can be sent to do something like checksum.
	outCh chan<- CreatedTable

	sender             BatchSender
	manager            ContextManager
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
	ctx context.Context,
	sender BatchSender,
	manager ContextManager,
	errCh chan<- error,
) (*Batcher, <-chan CreatedTable) {
	output := make(chan CreatedTable, defaultBatcherOutputChannelSize)
	sendChan := make(chan SendType, 2)
	b := &Batcher{
		rewriteRules:       EmptyRewriteRule(),
		sendErr:            errCh,
		outCh:              output,
		sender:             sender,
		manager:            manager,
		sendCh:             sendChan,
		cachedTablesMu:     new(sync.Mutex),
		everythingIsDone:   new(sync.WaitGroup),
		batchSizeThreshold: 1,
	}
	b.everythingIsDone.Add(1)
	go b.sendWorker(ctx, sendChan)
	return b, output
}

// EnableAutoCommit enables the batcher commit batch periodically even batcher size isn't big enough.
// we make this function for disable AutoCommit in some case.
func (b *Batcher) EnableAutoCommit(ctx context.Context, delay time.Duration) {
	if b.autoCommitJoiner != nil {
		log.Warn("enable auto commit on a batcher that auto commit is enabled, nothing will happen")
		log.Info("if desire(e.g. change the peroid of auto commit), please disable auto commit firstly")
	}
	joiner := make(chan struct{})
	go b.autoCommitWorker(ctx, joiner, delay)
	b.autoCommitJoiner = joiner
}

// DisableAutoCommit blocks the current goroutine until the worker can gracefully stop,
// and then disable auto commit.
func (b *Batcher) DisableAutoCommit() {
	b.joinWorker()
	b.autoCommitJoiner = nil
}

func (b *Batcher) waitUntilSendDone() {
	b.sendCh <- SendAllThenClose
	b.everythingIsDone.Wait()
}

// joinWorker blocks the current goroutine until the worker can gracefully stop.
// return immediately when auto commit disabled.
func (b *Batcher) joinWorker() {
	if b.autoCommitJoiner != nil {
		log.Debug("gracefully stopping worker goroutine")
		b.autoCommitJoiner <- struct{}{}
		close(b.autoCommitJoiner)
		log.Debug("gracefully stopped worker goroutine")
	}
}

// sendWorker is the 'worker' that send all ranges to TiKV.
func (b *Batcher) sendWorker(ctx context.Context, send <-chan SendType) {
	sendUntil := func(lessOrEqual int) {
		for b.Len() > lessOrEqual {
			tbls, err := b.Send(ctx)
			if err != nil {
				b.sendErr <- err
				return
			}
			for _, t := range tbls {
				b.outCh <- t
			}
		}
	}

	for sendType := range send {
		switch sendType {
		case SendUntilLessThanBatch:
			sendUntil(b.batchSizeThreshold)
		case SendAll:
			sendUntil(0)
		case SendAllThenClose:
			sendUntil(0)
			b.everythingIsDone.Done()
			return
		}
	}
}

func (b *Batcher) autoCommitWorker(ctx context.Context, joiner <-chan struct{}, delay time.Duration) {
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
				log.Debug("sending batch because time limit exceed", zap.Int("size", b.Len()))
				b.asyncSend(SendAll)
			}
		}
	}
}

func (b *Batcher) asyncSend(t SendType) {
	// add a check here so we won't replica sending.
	if len(b.sendCh) == 0 {
		b.sendCh <- t
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
				zap.Stringer("db", thisTable.OldTable.Db.Name),
				zap.Stringer("table", thisTable.Table.Name),
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
		atomic.AddInt32(&b.size, -int32(len(thisTable.Range)))
		// clear the table length.
		b.cachedTables[offset].Range = []rtree.Range{}
		log.Debug("draining table to batch",
			zap.Stringer("db", thisTable.OldTable.Db.Name),
			zap.Stringer("table", thisTable.Table.Name),
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

	log.Info("restore batch start",
		append(
			ZapRanges(ranges),
			ZapTables(tbs),
		)...,
	)

	if err := b.manager.Enter(ctx, drainResult.TablesToSend); err != nil {
		return nil, err
	}
	if err := b.sender.RestoreBatch(ctx, ranges, drainResult.RewriteRules); err != nil {
		return nil, err
	}
	if err := b.manager.Leave(ctx, drainResult.BlankTablesAfterSend); err != nil {
		return nil, err
	}
	if len(drainResult.BlankTablesAfterSend) > 0 {
		log.Debug("table fully restored",
			ZapTables(drainResult.BlankTablesAfterSend),
			zap.Int("ranges", len(ranges)),
		)
	}
	return drainResult.BlankTablesAfterSend, nil
}

func (b *Batcher) sendIfFull() {
	if b.Len() >= b.batchSizeThreshold {
		log.Debug("sending batch because batcher is full", zap.Int("size", b.Len()))
		b.asyncSend(SendUntilLessThanBatch)
	}
}

// Add adds a task to the Batcher.
func (b *Batcher) Add(tbs TableWithRange) {
	b.cachedTablesMu.Lock()
	log.Debug("adding table to batch",
		zap.Stringer("db", tbs.OldTable.Db.Name),
		zap.Stringer("table", tbs.Table.Name),
		zap.Int64("old id", tbs.OldTable.Info.ID),
		zap.Int64("new id", tbs.Table.ID),
		zap.Int("table size", len(tbs.Range)),
		zap.Int("batch size", b.Len()),
	)
	b.cachedTables = append(b.cachedTables, tbs)
	b.rewriteRules.Append(*tbs.RewriteRule)
	atomic.AddInt32(&b.size, int32(len(tbs.Range)))
	b.cachedTablesMu.Unlock()

	b.sendIfFull()
}

// Close closes the batcher, sending all pending requests, close updateCh.
func (b *Batcher) Close() {
	log.Info("sending batch lastly on close", zap.Int("size", b.Len()))
	b.DisableAutoCommit()
	b.waitUntilSendDone()
	close(b.outCh)
	close(b.sendCh)
	b.sender.Close()
}

// SetThreshold sets the threshold that how big the batch size reaching need to send batch.
// note this function isn't goroutine safe yet,
// just set threshold before anything starts(e.g. EnableAutoCommit), please.
func (b *Batcher) SetThreshold(newThreshold int) {
	b.batchSizeThreshold = newThreshold
}
