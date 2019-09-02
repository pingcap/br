package raw

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
)

type testBackup struct {
}

var _ = Suite(&testBackup{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (r *testBackup) TestBuildTableRange(c *C) {
	type Case struct {
		ids []int64
		trs []tableRange
	}
	cases := []Case{
		{ids: []int64{1}, trs: []tableRange{{startID: 1, endID: 2}}},
		{ids: []int64{1, 2, 3}, trs: []tableRange{{startID: 1, endID: 4}}},
		{ids: []int64{1, 3}, trs: []tableRange{{startID: 1, endID: 2}, {startID: 3, endID: 4}}},
		{ids: []int64{1, 2, 3, 6}, trs: []tableRange{{startID: 1, endID: 4}, {startID: 6, endID: 7}}},
		{ids: []int64{1, 2, 6, 7, 9, 10}, trs: []tableRange{
			{startID: 1, endID: 3}, {startID: 6, endID: 8}, {startID: 9, endID: 11},
		}},
	}
	for _, cs := range cases {
		c.Log(cs)
		tbl := &model.TableInfo{Partition: &model.PartitionInfo{Enable: true}}
		for _, id := range cs.ids {
			tbl.Partition.Definitions = append(tbl.Partition.Definitions,
				model.PartitionDefinition{ID: id})
		}
		ranges := buildTableRanges(tbl)
		c.Assert(ranges, DeepEquals, cs.trs)
	}

	tbl := &model.TableInfo{ID: 7}
	ranges := buildTableRanges(tbl)
	c.Assert(ranges, DeepEquals, []tableRange{{startID: 7, endID: 8}})

}
