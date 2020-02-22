package restore

import (
	"bytes"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/tablecodec"

	"github.com/pingcap/br/pkg/utils/rtree"
)

type testRangeSuite struct{}

var _ = Suite(&testRangeSuite{})

type rangeEquals struct {
	*CheckerInfo
}

var RangeEquals Checker = &rangeEquals{
	&CheckerInfo{Name: "RangeEquals", Params: []string{"obtained", "expected"}},
}

func (checker *rangeEquals) Check(params []interface{}, names []string) (result bool, error string) {
	obtained := params[0].([]rtree.Range)
	expected := params[1].([]rtree.Range)
	if len(obtained) != len(expected) {
		return false, ""
	}
	for i := range obtained {
		if !bytes.Equal(obtained[i].StartKey, expected[i].StartKey) ||
			!bytes.Equal(obtained[i].EndKey, expected[i].EndKey) {
			return false, ""
		}
	}
	return true, ""
}

func (s *testRangeSuite) TestSortRange(c *C) {
	dataRules := []*import_sstpb.RewriteRule{
		{OldKeyPrefix: tablecodec.GenTableRecordPrefix(1), NewKeyPrefix: tablecodec.GenTableRecordPrefix(4)},
		{OldKeyPrefix: tablecodec.GenTableRecordPrefix(2), NewKeyPrefix: tablecodec.GenTableRecordPrefix(5)},
	}
	rewriteRules := &RewriteRules{
		Table: make([]*import_sstpb.RewriteRule, 0),
		Data:  dataRules,
	}
	ranges1 := []rtree.Range{
		{append(tablecodec.GenTableRecordPrefix(1), []byte("aaa")...),
			append(tablecodec.GenTableRecordPrefix(1), []byte("bbb")...), nil},
	}
	rs1, err := sortRanges(ranges1, rewriteRules)
	c.Assert(err, IsNil, Commentf("sort range1 failed: %v", err))
	c.Assert(rs1, RangeEquals, []rtree.Range{
		{append(tablecodec.GenTableRecordPrefix(4), []byte("aaa")...),
			append(tablecodec.GenTableRecordPrefix(4), []byte("bbb")...), nil},
	})

	ranges2 := []rtree.Range{
		{append(tablecodec.GenTableRecordPrefix(1), []byte("aaa")...),
			append(tablecodec.GenTableRecordPrefix(2), []byte("bbb")...), nil},
	}
	_, err = sortRanges(ranges2, rewriteRules)
	c.Assert(err, ErrorMatches, ".*table id does not match.*")

	ranges3 := initRanges()
	rewriteRules1 := initRewriteRules()
	rs3, err := sortRanges(ranges3, rewriteRules1)
	c.Assert(err, IsNil, Commentf("sort range1 failed: %v", err))
	c.Assert(rs3, RangeEquals, []rtree.Range{
		{[]byte("bbd"), []byte("bbf"), nil},
		{[]byte("bbf"), []byte("bbj"), nil},
		{[]byte("xxa"), []byte("xxe"), nil},
		{[]byte("xxe"), []byte("xxz"), nil},
	})
}
