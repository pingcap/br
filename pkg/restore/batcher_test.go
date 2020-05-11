// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"time"

	"github.com/pingcap/br/pkg/restore"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/utils"
)

type testBatcherSuite struct{}

type drySender struct {
	tbls   chan restore.CreatedTable
	ranges chan rtree.Range
	nBatch int
}

func (d *drySender) RestoreBatch(
	ranges []rtree.Range,
	rewriteRules *restore.RewriteRules,
	tbs []restore.CreatedTable,
) error {
	d.nBatch++
	for _, tbl := range tbs {
		log.Info("dry restore", zap.Int64("table ID", tbl.Table.ID))
		d.tbls <- tbl
	}
	for _, rng := range ranges {
		d.ranges <- rng
	}
	return nil
}

func (d *drySender) Close() {
	close(d.tbls)
	close(d.ranges)
}

func (d *drySender) exhaust() (tbls []restore.CreatedTable, rngs []rtree.Range) {
	for tbl := range d.tbls {
		tbls = append(tbls, tbl)
	}
	for rng := range d.ranges {
		rngs = append(rngs, rng)
	}
	return
}

func newDrySender() *drySender {
	return &drySender{
		tbls:   make(chan restore.CreatedTable, 4096),
		ranges: make(chan rtree.Range, 4096),
	}
}

func (d *drySender) RangeLen() int {
	return len(d.ranges)
}

func (d *drySender) TableLen() int {
	return len(d.tbls)
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

func fakeRange(startKey, endKey string) rtree.Range {
	return rtree.Range{
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
	}
}

// TestBasic tests basic workflow of batcher.
func (*testBatcherSuite) TestBasic(c *C) {
	errCh := make(chan error, 8)
	sender := newDrySender()
	batcher, _ := restore.NewBatcher(context.Background(), sender, errCh)
	batcher.BatchSizeThreshold = 2

	simpleTables := []restore.TableWithRange{
		fakeTableWithRange(1, []rtree.Range{fakeRange("aaa", "aab")}),
		fakeTableWithRange(2, []rtree.Range{fakeRange("baa", "bab"), fakeRange("bac", "bad")}),
		fakeTableWithRange(3, []rtree.Range{fakeRange("caa", "cab"), fakeRange("cac", "cad")}),
	}

	for _, tbl := range simpleTables {
		batcher.Add(tbl)
	}

	batcher.Close()
	tbls, rngs := sender.exhaust()
	totalRngs := []rtree.Range{}

	c.Assert(len(tbls), Equals, len(simpleTables))
	for i, tbl := range simpleTables {
		c.Assert(tbls[i], DeepEquals, tbl.CreatedTable)
		totalRngs = append(totalRngs, tbl.Range...)
	}

	c.Assert(totalRngs, DeepEquals, rngs)
	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}

func (*testBatcherSuite) TestAutoSend(c *C) {
	errCh := make(chan error, 8)
	sender := newDrySender()
	batcher, _ := restore.NewBatcher(context.Background(), sender, errCh)
	batcher.BatchSizeThreshold = 1024

	simpleTable := fakeTableWithRange(1, []rtree.Range{fakeRange("caa", "cab"), fakeRange("cac", "cad")})

	batcher.Add(simpleTable)
	// wait until auto send.
	time.Sleep(1300 * time.Millisecond)
	c.Assert(sender.RangeLen(), Greater, 0)
	c.Assert(sender.TableLen(), Greater, 0)
	c.Assert(batcher.Len(), Equals, 0)

	batcher.Close()

	tbls, rngs := sender.exhaust()
	c.Assert(len(tbls), Greater, 0)
	c.Assert(rngs, DeepEquals, simpleTable.Range)
	c.Assert(tbls[0], DeepEquals, simpleTable.CreatedTable)
	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}

func (*testBatcherSuite) TestSplitRangeOnSameTable(c *C) {
	errCh := make(chan error, 8)
	sender := newDrySender()
	batcher, _ := restore.NewBatcher(context.Background(), sender, errCh)
	batcher.BatchSizeThreshold = 2

	simpleTable := fakeTableWithRange(1, []rtree.Range{
		fakeRange("caa", "cab"), fakeRange("cac", "cad"),
		fakeRange("cae", "caf"), fakeRange("cag", "cai"),
		fakeRange("caj", "cak"), fakeRange("cal", "cam"),
		fakeRange("can", "cao"), fakeRange("cap", "caq")})

	batcher.Add(simpleTable)
	c.Assert(sender.BatchCount(), Equals, 4)

	batcher.Close()

	tbls, rngs := sender.exhaust()
	c.Assert(len(tbls), Greater, 0)
	c.Assert(rngs, DeepEquals, simpleTable.Range)
	c.Assert(tbls[0], DeepEquals, simpleTable.CreatedTable)
	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}

func (*testBatcherSuite) TestBatcherLen(c *C) {
	errCh := make(chan error, 8)
	sender := newDrySender()
	batcher, _ := restore.NewBatcher(context.Background(), sender, errCh)
	batcher.BatchSizeThreshold = 1024

	simpleTable := fakeTableWithRange(1, []rtree.Range{
		fakeRange("caa", "cab"), fakeRange("cac", "cad"),
		fakeRange("cae", "caf"), fakeRange("cag", "cai"),
		fakeRange("caj", "cak"), fakeRange("cal", "cam"),
		fakeRange("can", "cao"), fakeRange("cap", "caq")})

	batcher.Add(simpleTable)
	c.Assert(batcher.Len(), Equals, 8)
	batcher.Close()
	c.Assert(batcher.Len(), Equals, 0)

	select {
	case err := <-errCh:
		c.Fatal(errors.Trace(err))
	default:
	}
}
