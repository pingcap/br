// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/br/pkg/mock"
	"github.com/pingcap/br/pkg/restore"
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
			return status.Error(codes.Unavailable, "transport is closing")
		case 1:
			return restore.ErrEpochNotMatch
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
	backoffer := restore.NewBackoffer(10, time.Nanosecond, time.Nanosecond)
	gRPCError := status.Error(codes.Unavailable, "transport is closing")
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		switch counter {
		case 0:
			return gRPCError
		case 1:
			return restore.ErrEpochNotMatch
		case 2:
			return restore.ErrDownloadFailed
		case 3:
			return restore.ErrRangeIsEmpty
		}
		return nil
	}, backoffer)
	c.Assert(counter, Equals, 4)
	c.Assert(multierr.Errors(err), DeepEquals, []error{
		gRPCError,
		restore.ErrEpochNotMatch,
		restore.ErrDownloadFailed,
		restore.ErrRangeIsEmpty,
	})
}

func (s *testBackofferSuite) TestBackoffWithFatalRawGRPCError(c *C) {
	var counter int
	canceledError := status.Error(codes.Canceled, "context canceled")
	backoffer := restore.NewBackoffer(10, time.Nanosecond, time.Nanosecond)
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		return canceledError
	}, backoffer)
	c.Assert(counter, Equals, 1)
	c.Assert(multierr.Errors(err), DeepEquals, []error{
		canceledError,
	})
}

func (s *testBackofferSuite) TestBackoffWithRetryableError(c *C) {
	var counter int
	backoffer := restore.NewBackoffer(10, time.Nanosecond, time.Nanosecond)
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		return restore.ErrEpochNotMatch
	}, backoffer)
	c.Assert(counter, Equals, 10)
	c.Assert(multierr.Errors(err), DeepEquals, []error{
		restore.ErrEpochNotMatch,
		restore.ErrEpochNotMatch,
		restore.ErrEpochNotMatch,
		restore.ErrEpochNotMatch,
		restore.ErrEpochNotMatch,
		restore.ErrEpochNotMatch,
		restore.ErrEpochNotMatch,
		restore.ErrEpochNotMatch,
		restore.ErrEpochNotMatch,
		restore.ErrEpochNotMatch,
	})
}
