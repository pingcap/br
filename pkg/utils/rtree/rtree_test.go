// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package rtree

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testRangeTreeSuite{})

type testRangeTreeSuite struct{}

func newRange(start, end []byte) *Range {
	return &Range{
		StartKey: start,
		EndKey:   end,
	}
}

func (s *testRangeTreeSuite) TestRangeTree(c *C) {
	rangeTree := NewRangeTree()
	c.Assert(rangeTree.Get(newRange([]byte(""), []byte(""))), IsNil)

	search := func(key []byte) *Range {
		rg := rangeTree.Get(newRange(key, []byte("")))
		if rg == nil {
			return nil
		}
		return rg.(*Range)
	}
	assertIncomplete := func(startKey, endKey []byte, ranges []Range) {
		incomplete := rangeTree.GetIncompleteRange(startKey, endKey)
		c.Logf("%#v %#v\n%#v\n%#v\n", startKey, endKey, incomplete, ranges)
		c.Assert(len(incomplete), Equals, len(ranges))
		for idx, rg := range incomplete {
			c.Assert(rg.StartKey, DeepEquals, ranges[idx].StartKey)
			c.Assert(rg.EndKey, DeepEquals, ranges[idx].EndKey)
		}
	}
	assertAllComplete := func() {
		for s := 0; s < 0xfe; s++ {
			for e := s + 1; e < 0xff; e++ {
				start := []byte{byte(s)}
				end := []byte{byte(e)}
				assertIncomplete(start, end, []Range{})
			}
		}
	}

	range0 := newRange([]byte(""), []byte("a"))
	rangeA := newRange([]byte("a"), []byte("b"))
	rangeB := newRange([]byte("b"), []byte("c"))
	rangeC := newRange([]byte("c"), []byte("d"))
	rangeD := newRange([]byte("d"), []byte(""))

	rangeTree.Update(*rangeA)
	c.Assert(rangeTree.Len(), Equals, 1)
	assertIncomplete([]byte("a"), []byte("b"), []Range{})
	assertIncomplete([]byte(""), []byte(""),
		[]Range{
			{StartKey: []byte(""), EndKey: []byte("a")},
			{StartKey: []byte("b"), EndKey: []byte("")},
		})

	rangeTree.Update(*rangeC)
	c.Assert(rangeTree.Len(), Equals, 2)
	assertIncomplete([]byte("a"), []byte("c"), []Range{
		{StartKey: []byte("b"), EndKey: []byte("c")},
	})
	assertIncomplete([]byte("b"), []byte("c"), []Range{
		{StartKey: []byte("b"), EndKey: []byte("c")},
	})
	assertIncomplete([]byte(""), []byte(""),
		[]Range{
			{StartKey: []byte(""), EndKey: []byte("a")},
			{StartKey: []byte("b"), EndKey: []byte("c")},
			{StartKey: []byte("d"), EndKey: []byte("")},
		})

	c.Assert(search([]byte{}), IsNil)
	c.Assert(search([]byte("a")), DeepEquals, rangeA)
	c.Assert(search([]byte("b")), IsNil)
	c.Assert(search([]byte("c")), DeepEquals, rangeC)
	c.Assert(search([]byte("d")), IsNil)

	rangeTree.Update(*rangeB)
	c.Assert(rangeTree.Len(), Equals, 3)
	c.Assert(search([]byte("b")), DeepEquals, rangeB)
	assertIncomplete([]byte(""), []byte(""),
		[]Range{
			{StartKey: []byte(""), EndKey: []byte("a")},
			{StartKey: []byte("d"), EndKey: []byte("")},
		})

	rangeTree.Update(*rangeD)
	c.Assert(rangeTree.Len(), Equals, 4)
	c.Assert(search([]byte("d")), DeepEquals, rangeD)
	assertIncomplete([]byte(""), []byte(""), []Range{
		{StartKey: []byte(""), EndKey: []byte("a")},
	})

	// None incomplete for any range after insert range 0
	rangeTree.Update(*range0)
	c.Assert(rangeTree.Len(), Equals, 5)

	// Overwrite range B and C.
	rangeBD := newRange([]byte("b"), []byte("d"))
	rangeTree.Update(*rangeBD)
	c.Assert(rangeTree.Len(), Equals, 4)
	assertAllComplete()

	// Overwrite range BD, c-d should be empty
	rangeTree.Update(*rangeB)
	c.Assert(rangeTree.Len(), Equals, 4)
	assertIncomplete([]byte(""), []byte(""), []Range{
		{StartKey: []byte("c"), EndKey: []byte("d")},
	})

	rangeTree.Update(*rangeC)
	c.Assert(rangeTree.Len(), Equals, 5)
	assertAllComplete()
}

func (s *testRangeTreeSuite) TestRangeIntersect(c *C) {
	rg := newRange([]byte("a"), []byte("c"))

	start, end, isIntersect := rg.Intersect([]byte(""), []byte(""))
	c.Assert(isIntersect, Equals, true)
	c.Assert(start, DeepEquals, []byte("a"))
	c.Assert(end, DeepEquals, []byte("c"))

	start, end, isIntersect = rg.Intersect([]byte(""), []byte("a"))
	c.Assert(isIntersect, Equals, false)
	c.Assert(start, DeepEquals, []byte(nil))
	c.Assert(end, DeepEquals, []byte(nil))

	start, end, isIntersect = rg.Intersect([]byte(""), []byte("b"))
	c.Assert(isIntersect, Equals, true)
	c.Assert(start, DeepEquals, []byte("a"))
	c.Assert(end, DeepEquals, []byte("b"))

	start, end, isIntersect = rg.Intersect([]byte("a"), []byte("b"))
	c.Assert(isIntersect, Equals, true)
	c.Assert(start, DeepEquals, []byte("a"))
	c.Assert(end, DeepEquals, []byte("b"))

	start, end, isIntersect = rg.Intersect([]byte("aa"), []byte("b"))
	c.Assert(isIntersect, Equals, true)
	c.Assert(start, DeepEquals, []byte("aa"))
	c.Assert(end, DeepEquals, []byte("b"))

	start, end, isIntersect = rg.Intersect([]byte("b"), []byte("c"))
	c.Assert(isIntersect, Equals, true)
	c.Assert(start, DeepEquals, []byte("b"))
	c.Assert(end, DeepEquals, []byte("c"))

	start, end, isIntersect = rg.Intersect([]byte(""), []byte{1})
	c.Assert(isIntersect, Equals, false)
	c.Assert(start, DeepEquals, []byte(nil))
	c.Assert(end, DeepEquals, []byte(nil))

	start, end, isIntersect = rg.Intersect([]byte("c"), []byte(""))
	c.Assert(isIntersect, Equals, false)
	c.Assert(start, DeepEquals, []byte(nil))
	c.Assert(end, DeepEquals, []byte(nil))
}

func BenchmarkRangeTreeUpdate(b *testing.B) {
	rangeTree := NewRangeTree()
	for i := 0; i < b.N; i++ {
		item := Range{
			StartKey: []byte(fmt.Sprintf("%20d", i)),
			EndKey:   []byte(fmt.Sprintf("%20d", i+1))}
		rangeTree.Update(item)
	}
}
