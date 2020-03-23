// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"sync"

	. "github.com/pingcap/check"
	pd "github.com/pingcap/pd/v3/client"
	"github.com/pingcap/tidb/util/testleak"

	"github.com/pingcap/br/pkg/mock"
)

var _ = Suite(&testSaftPointSuite{})

type testSaftPointSuite struct {
	mock *mock.Cluster
}

func (s *testSaftPointSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
}

func (s *testSaftPointSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testSaftPointSuite) TestCheckGCSafepoint(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

	ctx := context.Background()
	pdClient := &mockSafepoint{Client: s.mock.PDClient, safepoint: 2333}
	{
		err := CheckGCSafepoint(ctx, pdClient, 2333+1)
		c.Assert(err, IsNil)
	}
	{
		err := CheckGCSafepoint(ctx, pdClient, 2333)
		c.Assert(err, NotNil)
	}
	{
		err := CheckGCSafepoint(ctx, pdClient, 2333-1)
		c.Assert(err, NotNil)
	}
	{
		err := CheckGCSafepoint(ctx, pdClient, 0)
		c.Assert(err, ErrorMatches, "GC safepoint 2333 exceed TS 0")
	}
}

type mockSafepoint struct {
	sync.Mutex
	pd.Client
	safepoint uint64
}

func (m *mockSafepoint) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	m.Lock()
	defer m.Unlock()

	if m.safepoint < safePoint {
		m.safepoint = safePoint
	}
	return m.safepoint, nil
}
