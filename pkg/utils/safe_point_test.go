// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
	pd "github.com/tikv/pd/client"

	"github.com/pingcap/br/pkg/utils"
)

var _ = Suite(&testSafePointSuite{})

type testSafePointSuite struct{}

func (s *testSafePointSuite) SetUpSuite(c *C) {}

func (s *testSafePointSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testSafePointSuite) TestCheckGCSafepoint(c *C) {
	ctx := context.Background()
	pdClient := &mockSafePoint{safepoint: 2333}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 2333+1)
		c.Assert(err, IsNil)
	}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 2333)
		c.Assert(err, NotNil)
	}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 2333-1)
		c.Assert(err, NotNil)
	}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 0)
		c.Assert(err, ErrorMatches, ".*GC safepoint 2333 exceed TS 0.*")
	}
}

type mockSafePoint struct {
	sync.Mutex
	pd.Client
	safepoint uint64
}

func (m *mockSafePoint) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	m.Lock()
	defer m.Unlock()

	if m.safepoint < safePoint {
		m.safepoint = safePoint
	}
	return m.safepoint, nil
}
