// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"

	filter "github.com/pingcap/tidb-tools/pkg/table-filter"

	"github.com/pingcap/br/pkg/gluetidb"
	"github.com/pingcap/br/pkg/mock"
	"github.com/pingcap/br/pkg/restore"
)

type testLogRestoreSuite struct{
	mock *mock.Cluster

	client *restore.LogClient
}

var _ = Suite(&testLogRestoreSuite{})


func (s *testLogRestoreSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
	restoreClient, err := restore.NewRestoreClient(gluetidb.New(), s.mock.PDClient, s.mock.Storage, nil)
	c.Assert(err, IsNil)

	s.client, err = restore.NewLogRestoreClient(
		context.Background(),
		restoreClient,
		0,
		math.MaxInt64,
		filter.NewSchemasFilter("test"),
		8,
		16,
		5 << 20,
		16,
	)
	c.Assert(err, IsNil)
}

func (s *testLogRestoreSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testLogRestoreSuite) TestTsInRange(c *C) {
	fileName1 := "cdclog.1"
	s.client.ResetTSRange(1, 2)
	collected, err := s.client.NeedRestoreRowChange(fileName1)
	c.Assert(err, IsNil)
	c.Assert(collected, IsTrue)

	// cdclog.3 may have events in [1, 2]
	// so we should collect it.
	fileName2 := "cdclog.3"
	s.client.ResetTSRange(1, 2)
	collected, err = s.client.NeedRestoreRowChange(fileName2)
	c.Assert(err, IsNil)
	c.Assert(collected, IsTrue)

	fileName3 := "cdclog.3"
	s.client.ResetTSRange(4, 5)
	collected, err = s.client.NeedRestoreRowChange(fileName3)
	c.Assert(err, IsNil)
	c.Assert(collected, IsFalse)
}

