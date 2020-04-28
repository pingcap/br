// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"

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
		currentBatch:       []rtree.Range{},
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

// Send sends all pending requests in the batcher.
func (b *Batcher) Send() error {
	var ranges []rtree.Range
	ranges, b.currentBatch = b.currentBatch[:b.splitPoint()], b.currentBatch[b.splitPoint():]

	if err := SplitRanges(b.ctx, b.client, ranges, b.rewriteRules, b.updateCh); err != nil {
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
	log.Info("send batch",
		zap.Int("range count", len(ranges)),
		zap.Int("file count", len(files)),
	)
	if err := b.client.RestoreFiles(files, b.rewriteRules, b.rejectStoreMap, b.updateCh); err != nil {
		return err
	}
	return nil
}

func (b *Batcher) sendIfFull() error {
	if b.Len() >= b.BatchSizeThreshold {
		return b.Send()
	}
	return nil
}

// Add addes a task to bather.
func (b *Batcher) Add(tbs TableWithRange) error {
	log.Info("adding table to batch",
		zap.Stringer("table", tbs.Table.Name),
		zap.Stringer("database", tbs.OldTable.Db.Name),
		zap.Int64("old id", tbs.OldTable.Info.ID),
		zap.Int64("new id", tbs.Table.ID),
		zap.Int("batch size", b.Len()),
	)
	b.currentBatch = append(b.currentBatch, tbs.Range...)
	b.rewriteRules.Append(*tbs.RewriteRule)
	return b.sendIfFull()
}

// Close closes the batcher, sending all pending requests, close updateCh.
func (b *Batcher) Close() error {
	defer b.updateCh.Close()
	return b.Send()
}
