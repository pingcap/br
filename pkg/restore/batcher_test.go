// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/import_sstpb"

	"github.com/pingcap/br/pkg/restore"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"

	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/utils"
)

type testBatcherSuite struct{}

type drySender struct {
	mu *sync.Mutex

	rewriteRules *restore.RewriteRules
	ranges       []rtree.Range
	nBatch       int
}

func (d *drySender) RestoreBatch(
	_ctx context.Context,
	ranges []rtree.Range,
	rewriteRules *restore.RewriteRules,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nBatch++
	d.rewriteRules.Append(*rewriteRules)
	d.ranges = append(d.ranges, ranges...)
	return nil
}

func (d *drySender) Close() {}

func (d *drySender) Ranges() []rtree.Range {
	return d.ranges
}

func newDrySender() *drySender {
	return &drySender{
		rewriteRules: restore.EmptyRewriteRule(),
		ranges:       []rtree.Range{},
		mu:           new(sync.Mutex),
	}
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

func fakeTableWithRange(id int64, rngs []rtree.Range) restore.TableWithRange {
	tbl := &utils.Table{
		Db: &model.DBInfo{},
		Info: &model.TableInfo{
			ID: id,
		},
	}
	tblWithRng := restore.TableWithRange{
		CreatedTable: restore.CreatedTable{
			RewriteRule: restore.EmptyRewriteRule(),
			Table:       tbl.Info,
			OldTable:    tbl,
		},
		Range: rngs,
	}
	return tblWithRng
}

func fakeRewriteRules(oldPrefix string, newPrefix string) *restore.RewriteRules {
	return &restore.RewriteRules{
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
	errCh := make(chan error, 8)
	sender := newDrySender()
	batcher, _ := restore.NewBatcher(sender, errCh)
	batcher.SetThreshold(2)

	tableRanges := [][]rtree.Range{
		{fakeRange("aaa", "aab")},
		{fakeRange("baa", "bab"), fakeRange("bac", "bad")},
		{fakeRange("caa", "cab"), fakeRange("cac", "cad")},
	}

	simpleTables := []restore.TableWithRange{}
	for i, ranges := range tableRanges {
		simpleTables = append(simpleTables, fakeTableWithRange(int64(i), ranges))
	}

	for _, tbl := range simpleTables {
		batcher.Add(context.TODO(), tbl)
	}

	batcher.Close(context.TODO())
	rngs := sender.Ranges()

	c.Assert(join(tableRanges), DeepEquals, rngs)
	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}

func (*testBatcherSuite) TestAutoSend(c *C) {
	errCh := make(chan error, 8)
	sender := newDrySender()
	batcher, _ := restore.NewBatcher(sender, errCh)
	batcher.SetThreshold(1024)

	simpleTable := fakeTableWithRange(1, []rtree.Range{fakeRange("caa", "cab"), fakeRange("cac", "cad")})

	batcher.Add(context.TODO(), simpleTable)
	c.Assert(batcher.Len(), Greater, 0)

	// enable auto commit.
	batcher.EnableAutoCommit(context.TODO(), 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	c.Assert(sender.RangeLen(), Greater, 0)
	c.Assert(batcher.Len(), Equals, 0)

	batcher.Close(context.TODO())

	rngs := sender.Ranges()
	c.Assert(rngs, DeepEquals, simpleTable.Range)
	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}

func (*testBatcherSuite) TestSplitRangeOnSameTable(c *C) {
	errCh := make(chan error, 8)
	sender := newDrySender()
	batcher, _ := restore.NewBatcher(sender, errCh)
	batcher.SetThreshold(2)

	simpleTable := fakeTableWithRange(1, []rtree.Range{
		fakeRange("caa", "cab"), fakeRange("cac", "cad"),
		fakeRange("cae", "caf"), fakeRange("cag", "cai"),
		fakeRange("caj", "cak"), fakeRange("cal", "cam"),
		fakeRange("can", "cao"), fakeRange("cap", "caq")})

	batcher.Add(context.TODO(), simpleTable)
	c.Assert(sender.BatchCount(), Equals, 4)

	batcher.Close(context.TODO())

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
	rewriteRules := []*restore.RewriteRules{
		fakeRewriteRules("a", "ada"),
		fakeRewriteRules("b", "bob"),
		fakeRewriteRules("c", "cpp"),
	}

	tables := make([]restore.TableWithRange, 0, len(tableRanges))
	for i, ranges := range tableRanges {
		table := fakeTableWithRange(int64(i), ranges)
		table.RewriteRule = rewriteRules[i]
		tables = append(tables, table)
	}

	ctx := context.TODO()
	errCh := make(chan error, 8)
	sender := newDrySender()
	batcher, _ := restore.NewBatcher(sender, errCh)
	batcher.SetThreshold(2)

	batcher.Add(ctx, tables[0])
	c.Assert(sender.RangeLen(), Equals, 0)
	batcher.Add(ctx, tables[1])
	c.Assert(sender.HasRewriteRuleOfKey("a"), IsTrue)
	c.Assert(sender.HasRewriteRuleOfKey("b"), IsTrue)
	c.Assert(sender.RangeLen(), Equals, 2)
	batcher.Add(ctx, tables[2])
	batcher.Close(ctx)
	c.Assert(sender.HasRewriteRuleOfKey("c"), IsTrue)
	c.Assert(sender.Ranges(), DeepEquals, join(tableRanges))

	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}

func (*testBatcherSuite) TestBatcherLen(c *C) {
	errCh := make(chan error, 8)
	sender := newDrySender()
	batcher, _ := restore.NewBatcher(sender, errCh)
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

	batcher.Add(context.TODO(), simpleTable)
	c.Assert(batcher.Len(), Equals, 8)
	batcher.Add(context.TODO(), simpleTable2)
	c.Assert(batcher.Len(), Equals, 1)
	batcher.Close(context.TODO())
	c.Assert(batcher.Len(), Equals, 0)

	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}
