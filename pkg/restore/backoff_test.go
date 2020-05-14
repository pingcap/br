// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"

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

func (s *testBackofferSuite) TestBackoffWithFatalError(c *C) {
	var counter int
	backoffer := restore.NewBackoffer(10, time.Nanosecond, time.Nanosecond)
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		switch counter {
		case 0:
			return restore.ErrGRPC
		case 1:
			return restore.ErrEpochNotMatch
		case 2:
			return restore.ErrRangeIsEmpty
		}
		return nil
	}, backoffer)
	c.Assert(counter, Equals, 3)
	c.Assert(err, Equals, restore.ErrRangeIsEmpty)
}

func (s *testBackofferSuite) TestBackoffWithRetryableError(c *C) {
	var counter int
	backoffer := restore.NewBackoffer(10, time.Nanosecond, time.Nanosecond)
	err := utils.WithRetry(context.Background(), func() error {
		defer func() { counter++ }()
		return restore.ErrEpochNotMatch
	}, backoffer)
	c.Assert(counter, Equals, 10)
	c.Assert(err, Equals, restore.ErrEpochNotMatch)
}
