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
