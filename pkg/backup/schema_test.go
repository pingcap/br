// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"context"
	"math"
	"sync/atomic"

	. "github.com/pingcap/check"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"

	"github.com/pingcap/br/pkg/backup"
	"github.com/pingcap/br/pkg/mock"
)

var _ = Suite(&testBackupSchemaSuite{})

type testBackupSchemaSuite struct {
	mock *mock.Cluster
}

func (s *testBackupSchemaSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
}

func (s *testBackupSchemaSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

type simpleProgress struct {
	counter int64
}

func (sp *simpleProgress) Inc() {
	atomic.AddInt64(&sp.counter, 1)
}

func (sp *simpleProgress) Close() {}

func (sp *simpleProgress) reset() {
	atomic.StoreInt64(&sp.counter, 0)
}

func (sp *simpleProgress) get() int64 {
	return atomic.LoadInt64(&sp.counter)
}

func (s *testBackupSchemaSuite) TestBuildBackupRangeAndSchema(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

	tk := testkit.NewTestKit(c, s.mock.Storage)

	// Table t1 is not exist.
	testFilter, err := filter.Parse([]string{"test.t1"})
	c.Assert(err, IsNil)
	_, backupSchemas, err := backup.BuildBackupRangeAndSchema(
		s.mock.Domain, s.mock.Storage, testFilter, math.MaxUint64, false)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas, IsNil)

	// Database is not exist.
	fooFilter, err := filter.Parse([]string{"foo.t1"})
	c.Assert(err, IsNil)
	_, backupSchemas, err = backup.BuildBackupRangeAndSchema(
		s.mock.Domain, s.mock.Storage, fooFilter, math.MaxUint64, false)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas, IsNil)

	// Empty database.
	noFilter, err := filter.Parse([]string{"*.*"})
	c.Assert(err, IsNil)
	_, backupSchemas, err = backup.BuildBackupRangeAndSchema(
		s.mock.Domain, s.mock.Storage, noFilter, math.MaxUint64, false)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("insert into t1 values (10);")

	_, backupSchemas, err = backup.BuildBackupRangeAndSchema(
		s.mock.Domain, s.mock.Storage, testFilter, math.MaxUint64, false)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas.Len(), Equals, 1)
	updateCh := new(simpleProgress)
	backupSchemas.Start(context.Background(), s.mock.Storage, math.MaxUint64, 1, variable.DefChecksumTableConcurrency, updateCh)
	schemas, err := backupSchemas.FinishTableChecksum()
	c.Assert(updateCh.get(), Equals, int64(1))
	c.Assert(err, IsNil)
	c.Assert(len(schemas), Equals, 1)
	// Cluster returns a dummy checksum (all fields are 1).
	c.Assert(schemas[0].Crc64Xor, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalKvs, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalBytes, Not(Equals), 0, Commentf("%v", schemas[0]))

	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int);")
	tk.MustExec("insert into t2 values (10);")
	tk.MustExec("insert into t2 values (11);")

	_, backupSchemas, err = backup.BuildBackupRangeAndSchema(
		s.mock.Domain, s.mock.Storage, noFilter, math.MaxUint64, false)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas.Len(), Equals, 2)
	updateCh.reset()
	backupSchemas.Start(context.Background(), s.mock.Storage, math.MaxUint64, 2, variable.DefChecksumTableConcurrency, updateCh)
	schemas, err = backupSchemas.FinishTableChecksum()
	c.Assert(updateCh.get(), Equals, int64(2))
	c.Assert(err, IsNil)
	c.Assert(len(schemas), Equals, 2)
	// Cluster returns a dummy checksum (all fields are 1).
	c.Assert(schemas[0].Crc64Xor, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalKvs, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalBytes, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[1].Crc64Xor, Not(Equals), 0, Commentf("%v", schemas[1]))
	c.Assert(schemas[1].TotalKvs, Not(Equals), 0, Commentf("%v", schemas[1]))
	c.Assert(schemas[1].TotalBytes, Not(Equals), 0, Commentf("%v", schemas[1]))
}
