package raw

import (
	"context"
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"

	"github.com/pingcap/br/pkg/utils"
)

var _ = Suite(&testBackupSchemaSuite{})

type testBackupSchemaSuite struct {
	mock *utils.MockCluster
}

func (s *testBackupSchemaSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = utils.NewMockCluster()
	c.Assert(err, IsNil)
}

func (s *testBackupSchemaSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testBackupSchemaSuite) TestBuildBackupRangeAndSchema(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

	tk := testkit.NewTestKit(c, s.mock.Storage)

	// Table t1 is not exist.
	_, backupSchemas, err := BuildBackupRangeAndSchema(
		s.mock.Domain, s.mock.Storage, math.MaxUint64, "test", "t1")
	c.Assert(err, NotNil)
	c.Assert(backupSchemas, IsNil)

	// Database is not exist.
	_, backupSchemas, err = BuildBackupRangeAndSchema(
		s.mock.Domain, s.mock.Storage, math.MaxUint64, "foo", "t1")
	c.Assert(err, NotNil)
	c.Assert(backupSchemas, IsNil)

	// Empty databse.
	_, backupSchemas, err = BuildAllBackupRangeAndSchema(
		s.mock.Domain, s.mock.Storage, math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas, NotNil)
	c.Assert(backupSchemas.Len(), Equals, 0)
	updateCh := make(chan struct{}, 2)
	backupSchemas.Start(context.Background(), s.mock.Storage, math.MaxUint64, 1, updateCh)
	schemas, err := backupSchemas.finishTableChecksum()
	c.Assert(err, IsNil)
	c.Assert(len(schemas), Equals, 0)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("insert into t1 values (10);")

	_, backupSchemas, err = BuildBackupRangeAndSchema(
		s.mock.Domain, s.mock.Storage, math.MaxUint64, "test", "t1")
	c.Assert(err, IsNil)
	c.Assert(backupSchemas.Len(), Equals, 1)
	backupSchemas.Start(context.Background(), s.mock.Storage, math.MaxUint64, 1, updateCh)
	schemas, err = backupSchemas.finishTableChecksum()
	<-updateCh
	c.Assert(err, IsNil)
	c.Assert(len(schemas), Equals, 1)
	// MockCluster returns a dummy checksum (all fields are 1).
	c.Assert(schemas[0].Crc64Xor, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalKvs, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalBytes, Not(Equals), 0, Commentf("%v", schemas[0]))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t2 (a int);")
	tk.MustExec("insert into t2 values (10);")
	tk.MustExec("insert into t2 values (11);")

	_, backupSchemas, err = BuildAllBackupRangeAndSchema(
		s.mock.Domain, s.mock.Storage, math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas.Len(), Equals, 2)
	backupSchemas.Start(context.Background(), s.mock.Storage, math.MaxUint64, 2, updateCh)
	schemas, err = backupSchemas.finishTableChecksum()
	<-updateCh
	<-updateCh
	c.Assert(err, IsNil)
	c.Assert(len(schemas), Equals, 2)
	// MockCluster returns a dummy checksum (all fields are 1).
	c.Assert(schemas[0].Crc64Xor, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalKvs, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalBytes, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[1].Crc64Xor, Not(Equals), 0, Commentf("%v", schemas[1]))
	c.Assert(schemas[1].TotalKvs, Not(Equals), 0, Commentf("%v", schemas[1]))
	c.Assert(schemas[1].TotalBytes, Not(Equals), 0, Commentf("%v", schemas[1]))
}
