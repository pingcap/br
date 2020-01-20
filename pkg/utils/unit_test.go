package utils

import (
	. "github.com/pingcap/check"
)

type testUnitSuite struct{}

var _ = Suite(&testUnitSuite{})

func (r *testUnitSuite) TestLoadBackupMeta(c *C) {
	c.Assert(B, Equals, uint64(1))
	c.Assert(KB, Equals, uint64(1024))
	c.Assert(MB, Equals, uint64(1024*1024))
	c.Assert(GB, Equals, uint64(1024*1024*1024))
	c.Assert(TB, Equals, uint64(1024*1024*1024*1024))
}
