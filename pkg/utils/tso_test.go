package utils

import (
	"math/rand"
	"time"

	. "github.com/pingcap/check"
)

type testTsoSuite struct{}

var _ = Suite(&testTsoSuite{})

func (r *testTsoSuite) TestTimestampEncodeDecode(c *C) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 10; i++ {
		ts := rand.Uint64()
		tp := DecodeTs(ts)
		ts1 := EncodeTs(tp)
		c.Assert(ts, DeepEquals, ts1)
	}
}
