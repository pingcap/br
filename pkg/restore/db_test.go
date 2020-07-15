// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"math"
	"strconv"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"

	"github.com/pingcap/br/pkg/backup"
	"github.com/pingcap/br/pkg/gluetidb"
	"github.com/pingcap/br/pkg/mock"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/utils"
)

var _ = Suite(&testRestoreSchemaSuite{})

type testRestoreSchemaSuite struct {
	mock *mock.Cluster
}

func (s *testRestoreSchemaSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
	c.Assert(s.mock.Start(), IsNil)
}
func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testRestoreSchemaSuite) TearDownSuite(c *C) {
	s.mock.Stop()
	testleak.AfterTest(c)()
}

func (s *testRestoreSchemaSuite) TestRestoreAutoIncID(c *C) {
	tk := testkit.NewTestKit(c, s.mock.Storage)
	tk.MustExec("use test")
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("drop table if exists `\"t\"`;")
	// Test SQL Mode
	tk.MustExec("create table `\"t\"` (" +
		"a int not null," +
		"time timestamp not null default '0000-00-00 00:00:00'," +
		"primary key (a));",
	)
	tk.MustExec("insert into `\"t\"` values (10, '0000-00-00 00:00:00');")
	// Query the current AutoIncID
	autoIncID, err := strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	c.Assert(err, IsNil, Commentf("Error query auto inc id: %s", err))
	// Get schemas of db and table
	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	c.Assert(err, IsNil, Commentf("Error get snapshot info schema: %s", err))
	dbInfo, exists := info.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue, Commentf("Error get db info"))
	tableInfo, err := info.TableByName(model.NewCIStr("test"), model.NewCIStr("\"t\""))
	c.Assert(err, IsNil, Commentf("Error get table info: %s", err))
	table := utils.Table{
		Info: tableInfo.Meta(),
		Db:   dbInfo,
	}
	// Get the next AutoIncID
	idAlloc := autoid.NewAllocator(s.mock.Storage, dbInfo.ID, false, autoid.RowIDAllocType)
	globalAutoID, err := idAlloc.NextGlobalAutoID(table.Info.ID)
	c.Assert(err, IsNil, Commentf("Error allocate next auto id"))
	c.Assert(autoIncID, Equals, uint64(globalAutoID))
	// Alter AutoIncID to the next AutoIncID + 100
	table.Info.AutoIncID = globalAutoID + 100
	db, err := restore.NewDB(gluetidb.New(), s.mock.Storage)
	c.Assert(err, IsNil, Commentf("Error create DB"))
	tk.MustExec("drop database if exists test;")
	// Test empty collate value
	table.Db.Charset = "utf8mb4"
	table.Db.Collate = ""
	err = db.CreateDatabase(context.Background(), table.Db)
	c.Assert(err, IsNil, Commentf("Error create empty collate db: %s %s", err, s.mock.DSN))
	tk.MustExec("drop database if exists test;")
	// Test empty charset value
	table.Db.Charset = ""
	table.Db.Collate = "utf8mb4_bin"
	err = db.CreateDatabase(context.Background(), table.Db)
	c.Assert(err, IsNil, Commentf("Error create empty charset db: %s %s", err, s.mock.DSN))
	err = db.CreateTable(context.Background(), &table)
	c.Assert(err, IsNil, Commentf("Error create table: %s %s", err, s.mock.DSN))
	tk.MustExec("use test")
	// Check if AutoIncID is altered successfully
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	c.Assert(err, IsNil, Commentf("Error query auto inc id: %s", err))
	c.Assert(autoIncID, Equals, uint64(globalAutoID+100))
}

func (s *testRestoreSchemaSuite) TestFilterDDLJobs(c *C) {
	tk := testkit.NewTestKit(c, s.mock.Storage)
	tk.MustExec("CREATE DATABASE IF NOT EXISTS test_db;")
	tk.MustExec("CREATE TABLE IF NOT EXISTS test_db.test_table (c1 INT);")
	lastTs, err := s.mock.GetOracle().GetTimestamp(context.Background())
	c.Assert(err, IsNil, Commentf("Error get last ts: %s", err))
	tk.MustExec("RENAME TABLE test_db.test_table to test_db.test_table1;")
	tk.MustExec("DROP TABLE test_db.test_table1;")
	tk.MustExec("DROP DATABASE test_db;")
	tk.MustExec("CREATE DATABASE test_db;")
	tk.MustExec("USE test_db;")
	tk.MustExec("CREATE TABLE test_table1 (c2 CHAR(255));")
	tk.MustExec("RENAME TABLE test_table1 to test_table;")
	tk.MustExec("TRUNCATE TABLE test_table;")

	ts, err := s.mock.GetOracle().GetTimestamp(context.Background())
	c.Assert(err, IsNil, Commentf("Error get ts: %s", err))
	allDDLJobs, err := backup.GetBackupDDLJobs(s.mock.Domain, lastTs, ts)
	c.Assert(err, IsNil, Commentf("Error get ddl jobs: %s", err))
	infoSchema, err := s.mock.Domain.GetSnapshotInfoSchema(ts)
	c.Assert(err, IsNil, Commentf("Error get snapshot info schema: %s", err))
	dbInfo, ok := infoSchema.SchemaByName(model.NewCIStr("test_db"))
	c.Assert(ok, IsTrue, Commentf("DB info not exist"))
	tableInfo, err := infoSchema.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_table"))
	c.Assert(err, IsNil, Commentf("Error get table info: %s", err))
	tables := []*utils.Table{{
		Db:   dbInfo,
		Info: tableInfo.Meta(),
	}}
	ddlJobs := restore.FilterDDLJobs(allDDLJobs, tables)
	for _, job := range ddlJobs {
		c.Logf("get ddl job: %s", job.Query)
	}
	c.Assert(len(ddlJobs), Equals, 7)
}
