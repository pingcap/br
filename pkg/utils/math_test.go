// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	. "github.com/pingcap/check"
)

type testMathSuite struct{}

var _ = Suite(&testMathSuite{})

func (*testMathSuite) TestMinInt(c *C) {
	c.Assert(MinInt(1, 2), Equals, 1)
	c.Assert(MinInt(2, 1), Equals, 1)
	c.Assert(MinInt(1, 1), Equals, 1)
}
