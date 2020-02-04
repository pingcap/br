package restore

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"

	"github.com/pingcap/br/pkg/utils"
)

var _ = Suite(&testBackofferSuite{})

type testBackofferSuite struct {
	mock *utils.MockCluster
}

func (s *testBackofferSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = utils.NewMockCluster()
	c.Assert(err, IsNil)
}

func (s *testBackofferSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testBackofferSuite) TestImporterBackoffer(c *C) {
	var counter int
	err := utils.WithRetry(func() error {
		defer func() { counter++ }()
		switch counter {
		case 0:
			return errGrpc
		case 1:
			return errResp
		case 2:
			return errRangeIsEmpty
		}
		return nil
	}, newImportSSTBackoffer())
	c.Assert(counter, Equals, 3)
	c.Assert(err, Equals, errRangeIsEmpty)

	counter = 0
	backoffer := importerBackoffer{
		attempt:      10,
		delayTime:    time.Nanosecond,
		maxDelayTime: time.Nanosecond,
	}
	err = utils.WithRetry(func() error {
		defer func() { counter++ }()
		return errResp
	}, &backoffer)
	c.Assert(counter, Equals, 10)
	c.Assert(err, Equals, errResp)
}
