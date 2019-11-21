package utils

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testMockClusterSuite{})

type testMockClusterSuite struct {
	mock *MockCluster
}

func (s *testMockClusterSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = NewMockCluster()
	c.Assert(err, IsNil)
}

func (s *testMockClusterSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testMockClusterSuite) TestSmoke(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	s.mock.Stop()
}
