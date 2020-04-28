// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"fmt"

	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"
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
	rejectStoreMap     map[uint64]bool
	updateCh           glue.Progress
	BatchSizeThreshold int
}

// Len calculate the current size of this batcher.
func (b *Batcher) Len() int {
	return len(b.currentBatch)
}

// NewBatcher creates a new batcher by client and updateCh.
func NewBatcher(
	ctx context.Context,
	client *Client,
	updateCh glue.Progress,
) *Batcher {
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

	return &Batcher{
		rewriteRules:       EmptyRewriteRule(),
		client:             client,
		rejectStoreMap:     rejectStoreMap,
		updateCh:           updateCh,
		ctx:                ctx,
		BatchSizeThreshold: 1,
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
	for _, t := range tbs {
		tableNames = append(tableNames, fmt.Sprintf("%s.%s", t.OldTable.Db.Name, t.OldTable.Info.Name))
	}
	log.Info("prepare split range by tables", zap.Strings("tables", tableNames))

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
	log.Info("send batch",
		zap.Int("range count", len(ranges)),
		zap.Int("file count", len(files)),
	)
	if err := b.client.RestoreFiles(files, b.rewriteRules, b.rejectStoreMap, b.updateCh); err != nil {
		return nil, err
	}

	return tbs, nil
}

func (b *Batcher) sendIfFull() ([]TableWithRange, error) {
	if b.Len() >= b.BatchSizeThreshold {
		return b.Send()
	}
	return []TableWithRange{}, nil
}

// Add addes a task to bather.
func (b *Batcher) Add(tbs TableWithRange) ([]TableWithRange, error) {
	log.Info("adding table to batch",
		zap.Stringer("table", tbs.Table.Name),
		zap.Stringer("database", tbs.OldTable.Db.Name),
		zap.Int64("old id", tbs.OldTable.Info.ID),
		zap.Int64("new id", tbs.Table.ID),
		zap.Int("batch size", b.Len()),
	)
	b.currentBatch = append(b.currentBatch, tbs.Range...)
	b.cachedTables = append(b.cachedTables, tbs)
	b.rewriteRules.Append(*tbs.RewriteRule)
	return b.sendIfFull()
}

// Close closes the batcher, sending all pending requests, close updateCh.
func (b *Batcher) Close() ([]TableWithRange, error) {
	defer b.updateCh.Close()
	return b.Send()
}
