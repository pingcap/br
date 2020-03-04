package mock

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testClusterSuite{})

type testClusterSuite struct {
	mock *Cluster
}

func (s *testClusterSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = NewCluster()
	c.Assert(err, IsNil)
}

func (s *testClusterSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testClusterSuite) TestSmoke(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	s.mock.Stop()
}
