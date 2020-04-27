// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"fmt"

	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
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
// TODO: support cross-table batching. (i.e. one table can occur in two batches)
type Batcher struct {
	currentBatch []TableWithRange
	currentSize  int

	ctx                context.Context
	client             *Client
	rejectStoreMap     map[uint64]bool
	updateCh           glue.Progress
	BatchSizeThreshold int
}

// NewBatcher creates a new batcher by client and updateCh.
func NewBatcher(
	ctx context.Context,
	client *Client,
	rejectStoreMap map[uint64]bool,
	updateCh glue.Progress,
) *Batcher {
	return &Batcher{
		currentBatch:       []TableWithRange{},
		client:             client,
		rejectStoreMap:     rejectStoreMap,
		updateCh:           updateCh,
		ctx:                ctx,
		BatchSizeThreshold: 1,
	}
}

// Send sends all pending requests in the batcher.
func (b *Batcher) Send() error {
	ranges := []rtree.Range{}
	rewriteRules := &RewriteRules{
		Table: []*import_sstpb.RewriteRule{},
		Data:  []*import_sstpb.RewriteRule{},
	}
	for _, t := range b.currentBatch {
		ranges = append(ranges, t.Range...)
		rewriteRules.Append(*t.RewriteRule)
	}

	tableNames := []string{}
	for _, t := range b.currentBatch {
		tableNames = append(tableNames, fmt.Sprintf("%s.%s", t.OldTable.Db.Name, t.OldTable.Info.Name))
	}
	log.Info("sending batch restore request",
		zap.Int("range count", len(ranges)),
		zap.Int("table count", len(b.currentBatch)),
		zap.Strings("tables", tableNames),
	)

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
	if err := b.client.RestoreFiles(files, rewriteRules, b.rejectStoreMap, b.updateCh); err != nil {
		return err
	}
	b.currentBatch = []TableWithRange{}
	b.currentSize = 0
	return nil
}

func (b *Batcher) sendIfFull() error {
	if b.currentSize >= b.BatchSizeThreshold {
		return b.Send()
	}
	return nil
}

// AddRangeCount add current size of the Batcher,
// and sends cached requests if current size is greater than BatchThreshold.
func (b *Batcher) AddRangeCount(by int) error {
	b.currentSize += by
	return b.sendIfFull()
}

// Add addes a task to bather.
func (b *Batcher) Add(tbs TableWithRange) error {
	b.currentBatch = append(b.currentBatch, tbs)
	return b.AddRangeCount(len(tbs.Range))
}
