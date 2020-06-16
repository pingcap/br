// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"

	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/utils"
)

type testBatcherSuite struct{}

type drySender struct {
	mu *sync.Mutex

	rewriteRules *RewriteRules
	ranges       []rtree.Range
	nBatch       int
}

func (d *drySender) RestoreBatch(
	_ctx context.Context,
	ranges []rtree.Range,
	rewriteRules *RewriteRules,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	log.Info("fake restore range", ZapRanges(ranges)...)
	d.nBatch++
	d.rewriteRules.Append(*rewriteRules)
	d.ranges = append(d.ranges, ranges...)
	return nil
}

func (d *drySender) Close() {}

func waitForSend() {
	time.Sleep(10 * time.Millisecond)
}

func (d *drySender) Ranges() []rtree.Range {
	return d.ranges
}

func newDrySender() *drySender {
	return &drySender{
		rewriteRules: EmptyRewriteRule(),
		ranges:       []rtree.Range{},
		mu:           new(sync.Mutex),
	}
}

type recordCurrentTableManager map[int64]bool

func newMockManager() recordCurrentTableManager {
	return make(recordCurrentTableManager)
}

func (manager recordCurrentTableManager) Enter(_ context.Context, tables []CreatedTable) error {
	for _, t := range tables {
		log.Info("entering", zap.Int64("table ID", t.Table.ID))
		manager[t.Table.ID] = true
	}
	return nil
}

func (manager recordCurrentTableManager) Leave(_ context.Context, tables []CreatedTable) error {
	for _, t := range tables {
		if !manager[t.Table.ID] {
			return errors.Errorf("Table %d is removed before added", t.Table.ID)
		}
		log.Info("leaving", zap.Int64("table ID", t.Table.ID))
		manager[t.Table.ID] = false
	}
	return nil
}

func (manager recordCurrentTableManager) Has(tables ...TableWithRange) bool {
	ids := make([]int64, 0, len(tables))
	currentIDs := make([]int64, 0, len(manager))
	for _, t := range tables {
		ids = append(ids, t.Table.ID)
	}
	for id, contains := range manager {
		if contains {
			currentIDs = append(currentIDs, id)
		}
	}
	log.Info("testing", zap.Int64s("should has ID", ids), zap.Int64s("has ID", currentIDs))
	for _, i := range ids {
		if !manager[i] {
			return false
		}
	}
	return true
}

func (d *drySender) HasRewriteRuleOfKey(prefix string) bool {
	for _, rule := range d.rewriteRules.Table {
		if bytes.Equal([]byte(prefix), rule.OldKeyPrefix) {
			return true
		}
	}
	return false
}

func (d *drySender) RangeLen() int {
	return len(d.ranges)
}

func (d *drySender) BatchCount() int {
	return d.nBatch
}

var (
	_ = Suite(&testBatcherSuite{})
)

func fakeTableWithRange(id int64, rngs []rtree.Range) TableWithRange {
	tbl := &utils.Table{
		Db: &model.DBInfo{},
		Info: &model.TableInfo{
			ID: id,
		},
	}
	tblWithRng := TableWithRange{
		CreatedTable: CreatedTable{
			RewriteRule: EmptyRewriteRule(),
			Table:       tbl.Info,
			OldTable:    tbl,
		},
		Range: rngs,
	}
	return tblWithRng
}

func fakeRewriteRules(oldPrefix string, newPrefix string) *RewriteRules {
	return &RewriteRules{
		Table: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: []byte(oldPrefix),
				NewKeyPrefix: []byte(newPrefix),
			},
		},
	}
}

func fakeRange(startKey, endKey string) rtree.Range {
	return rtree.Range{
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
	}
}

func join(nested [][]rtree.Range) (plain []rtree.Range) {
	for _, ranges := range nested {
		plain = append(plain, ranges...)
	}
	return plain
}

// TestBasic tests basic workflow of batcher.
func (*testBatcherSuite) TestBasic(c *C) {
	ctx := context.Background()
	errCh := make(chan error, 8)
	sender := newDrySender()
	manager := newMockManager()
	batcher, _ := NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(2)

	tableRanges := [][]rtree.Range{
		{fakeRange("aaa", "aab")},
		{fakeRange("baa", "bab"), fakeRange("bac", "bad")},
		{fakeRange("caa", "cab"), fakeRange("cac", "cad")},
	}

	simpleTables := []TableWithRange{}
	for i, ranges := range tableRanges {
		simpleTables = append(simpleTables, fakeTableWithRange(int64(i), ranges))
	}
	for _, tbl := range simpleTables {
		batcher.Add(tbl)
	}

	batcher.Close()
	rngs := sender.Ranges()

	c.Assert(join(tableRanges), DeepEquals, rngs)
	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}

func (*testBatcherSuite) TestAutoSend(c *C) {
	ctx := context.Background()
	errCh := make(chan error, 8)
	sender := newDrySender()
	manager := newMockManager()
	batcher, _ := NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(1024)

	simpleTable := fakeTableWithRange(1, []rtree.Range{fakeRange("caa", "cab"), fakeRange("cac", "cad")})

	batcher.Add(simpleTable)
	c.Assert(batcher.Len(), Greater, 0)

	// enable auto commit.
	batcher.EnableAutoCommit(ctx, 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	c.Assert(sender.RangeLen(), Greater, 0)
	c.Assert(batcher.Len(), Equals, 0)

	batcher.Close()

	rngs := sender.Ranges()
	c.Assert(rngs, DeepEquals, simpleTable.Range)
	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}

func (*testBatcherSuite) TestSplitRangeOnSameTable(c *C) {
	ctx := context.Background()
	errCh := make(chan error, 8)
	sender := newDrySender()
	manager := newMockManager()
	batcher, _ := NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(2)

	simpleTable := fakeTableWithRange(1, []rtree.Range{
		fakeRange("caa", "cab"), fakeRange("cac", "cad"),
		fakeRange("cae", "caf"), fakeRange("cag", "cai"),
		fakeRange("caj", "cak"), fakeRange("cal", "cam"),
		fakeRange("can", "cao"), fakeRange("cap", "caq")})

	batcher.Add(simpleTable)
	batcher.Close()
	c.Assert(sender.BatchCount(), Equals, 4)

	rngs := sender.Ranges()
	c.Assert(rngs, DeepEquals, simpleTable.Range)
	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}

func (*testBatcherSuite) TestRewriteRules(c *C) {
	tableRanges := [][]rtree.Range{
		{fakeRange("aaa", "aab")},
		{fakeRange("baa", "bab"), fakeRange("bac", "bad")},
		{fakeRange("caa", "cab"), fakeRange("cac", "cad"),
			fakeRange("cae", "caf"), fakeRange("cag", "cai"),
			fakeRange("caj", "cak"), fakeRange("cal", "cam"),
			fakeRange("can", "cao"), fakeRange("cap", "caq")},
	}
	rewriteRules := []*RewriteRules{
		fakeRewriteRules("a", "ada"),
		fakeRewriteRules("b", "bob"),
		fakeRewriteRules("c", "cpp"),
	}

	tables := make([]TableWithRange, 0, len(tableRanges))
	for i, ranges := range tableRanges {
		table := fakeTableWithRange(int64(i), ranges)
		table.RewriteRule = rewriteRules[i]
		tables = append(tables, table)
	}

	ctx := context.Background()
	errCh := make(chan error, 8)
	sender := newDrySender()
	manager := newMockManager()
	batcher, _ := NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(2)

	batcher.Add(tables[0])
	waitForSend()
	c.Assert(sender.RangeLen(), Equals, 0)

	batcher.Add(tables[1])
	waitForSend()
	c.Assert(sender.HasRewriteRuleOfKey("a"), IsTrue)
	c.Assert(sender.HasRewriteRuleOfKey("b"), IsTrue)
	c.Assert(manager.Has(tables[1]), IsTrue)
	c.Assert(sender.RangeLen(), Equals, 2)

	batcher.Add(tables[2])
	batcher.Close()
	c.Assert(sender.HasRewriteRuleOfKey("c"), IsTrue)
	c.Assert(sender.Ranges(), DeepEquals, join(tableRanges))

	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}

func (*testBatcherSuite) TestBatcherLen(c *C) {
	ctx := context.Background()
	errCh := make(chan error, 8)
	sender := newDrySender()
	manager := newMockManager()
	batcher, _ := NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(15)

	simpleTable := fakeTableWithRange(1, []rtree.Range{
		fakeRange("caa", "cab"), fakeRange("cac", "cad"),
		fakeRange("cae", "caf"), fakeRange("cag", "cai"),
		fakeRange("caj", "cak"), fakeRange("cal", "cam"),
		fakeRange("can", "cao"), fakeRange("cap", "caq")})

	simpleTable2 := fakeTableWithRange(2, []rtree.Range{
		fakeRange("caa", "cab"), fakeRange("cac", "cad"),
		fakeRange("cae", "caf"), fakeRange("cag", "cai"),
		fakeRange("caj", "cak"), fakeRange("cal", "cam"),
		fakeRange("can", "cao"), fakeRange("cap", "caq")})

	batcher.Add(simpleTable)
	waitForSend()
	c.Assert(batcher.Len(), Equals, 8)
	c.Assert(manager.Has(simpleTable), IsFalse)
	c.Assert(manager.Has(simpleTable2), IsFalse)

	batcher.Add(simpleTable2)
	waitForSend()
	c.Assert(batcher.Len(), Equals, 1)
	c.Assert(manager.Has(simpleTable2), IsTrue)
	c.Assert(manager.Has(simpleTable), IsFalse)
	batcher.Close()
	c.Assert(batcher.Len(), Equals, 0)

	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}
