package task

import (
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testBackupSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testBackupSuite struct{}

func (s *testBackupSuite) TestParseTSString(c *C) {
	var (
		ts  uint64
		err error
	)

	ts, err = parseTSString("")
	c.Assert(err, IsNil)
	c.Assert(int(ts), Equals, 0)

	ts, err = parseTSString("400036290571534337")
	c.Assert(err, IsNil)
	c.Assert(int(ts), Equals, 400036290571534337)

	ts, err = parseTSString("2018-05-11 01:42:23")
	c.Assert(err, IsNil)
	c.Assert(int(ts), Equals, 400024965742592000)
}
