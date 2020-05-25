// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
	"go.uber.org/multierr"

	"github.com/pingcap/br/pkg/mock"
	"github.com/pingcap/br/pkg/utils"
)

var _ = Suite(&testBackofferSuite{})

type testBackofferSuite struct {
	mock *mock.Cluster
}

func (s *testBackofferSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
}

func (s *testBackofferSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testBackofferSuite) TestBackoffWithSuccess(c *C) {
	var counter int
	backoffer := restore.NewBackoffer(10, time.Nanosecond, time.Nanosecond)
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		switch counter {
		case 0:
			return errGRPC
		case 1:
			return errEpochNotMatch
		case 2:
			return nil
		}
		return nil
	}, backoffer)
	c.Assert(counter, Equals, 3)
	c.Assert(err, IsNil)
}

func (s *testBackofferSuite) TestBackoffWithFatalError(c *C) {
	var counter int
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		switch counter {
		case 0:
			return errGrpc
		case 1:
			return errEpochNotMatch
		case 2:
			return errDownloadFailed
		case 3:
			return errRangeIsEmpty
		}
		return nil
	}, backoffer)
	c.Assert(counter, Equals, 4)
	c.Assert(multierr.Errors(err), DeepEquals, []error{
		errGRPC,
		errEpochNotMatch,
		errDownloadFailed,
		errRangeIsEmpty,
	})
}

func (s *testBackofferSuite) TestBackoffWithRetryableError(c *C) {
	var counter int
	backoffer := restore.NewBackoffer(10, time.Nanosecond, time.Nanosecond)
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		return errEpochNotMatch
	}, &backoffer)
	c.Assert(counter, Equals, 10)
	c.Assert(multierr.Errors(err), DeepEquals, []error{
		errEpochNotMatch,
		errEpochNotMatch,
		errEpochNotMatch,
		errEpochNotMatch,
		errEpochNotMatch,
		errEpochNotMatch,
		errEpochNotMatch,
		errEpochNotMatch,
		errEpochNotMatch,
		errEpochNotMatch,
	})
}
