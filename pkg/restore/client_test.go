// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"math"
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/testleak"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/br/pkg/gluetidb"
	"github.com/pingcap/br/pkg/mock"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/utils"
)

var _ = Suite(&testRestoreClientSuite{})

var defaultKeepaliveCfg = keepalive.ClientParameters{
	Time:    3 * time.Second,
	Timeout: 10 * time.Second,
}

type testRestoreClientSuite struct {
	mock *mock.Cluster
}

func (s *testRestoreClientSuite) SetUpTest(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
}

func (s *testRestoreClientSuite) TearDownTest(c *C) {
	testleak.AfterTest(c)()
}

func (s *testRestoreClientSuite) TestCreateTables(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()
	client, err := restore.NewRestoreClient(gluetidb.New(), s.mock.PDClient, s.mock.Storage, nil, defaultKeepaliveCfg)
	c.Assert(err, IsNil)

	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	c.Assert(err, IsNil)
	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
	c.Assert(isExist, IsTrue)

	tables := make([]*utils.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.Charset = "binary"
	for i := len(tables) - 1; i >= 0; i-- {
		tables[i] = &utils.Table{
			DB: dbSchema,
			Info: &model.TableInfo{
				ID:   int64(i),
				Name: model.NewCIStr("test" + strconv.Itoa(i)),
				Columns: []*model.ColumnInfo{{
					ID:        1,
					Name:      model.NewCIStr("id"),
					FieldType: *intField,
					State:     model.StatePublic,
				}},
				Charset: "utf8mb4",
				Collate: "utf8mb4_bin",
			},
		}
	}
	rules, newTables, err := client.CreateTables(s.mock.Domain, tables, 0)
	c.Assert(err, IsNil)
	// make sure tables and newTables have same order
	for i, t := range tables {
		c.Assert(newTables[i].Name, Equals, t.Info.Name)
	}
	for _, nt := range newTables {
		c.Assert(nt.Name.String(), Matches, "test[0-3]")
	}
	oldTableIDExist := make(map[int64]bool)
	newTableIDExist := make(map[int64]bool)
	for _, tr := range rules.Table {
		oldTableID := tablecodec.DecodeTableID(tr.GetOldKeyPrefix())
		c.Assert(oldTableIDExist[oldTableID], IsFalse, Commentf("table rule duplicate old table id"))
		oldTableIDExist[oldTableID] = true

		newTableID := tablecodec.DecodeTableID(tr.GetNewKeyPrefix())
		c.Assert(newTableIDExist[newTableID], IsFalse, Commentf("table rule duplicate new table id"))
		newTableIDExist[newTableID] = true
	}

	for i := 0; i < len(tables); i++ {
		c.Assert(oldTableIDExist[int64(i)], IsTrue, Commentf("table rule does not exist"))
	}
}

func (s *testRestoreClientSuite) TestIsOnline(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

	client, err := restore.NewRestoreClient(gluetidb.New(), s.mock.PDClient, s.mock.Storage, nil, defaultKeepaliveCfg)
	c.Assert(err, IsNil)

	c.Assert(client.IsOnline(), IsFalse)
	client.EnableOnline()
	c.Assert(client.IsOnline(), IsTrue)
}

func (s *testRestoreClientSuite) TestPreCheckTableClusterIndex(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

	client, err := restore.NewRestoreClient(gluetidb.New(), s.mock.PDClient, s.mock.Storage, nil, defaultKeepaliveCfg)
	c.Assert(err, IsNil)

	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	c.Assert(err, IsNil)
	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
	c.Assert(isExist, IsTrue)

	tables := make([]*utils.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.Charset = "binary"
	for i := len(tables) - 1; i >= 0; i-- {
		tables[i] = &utils.Table{
			DB: dbSchema,
			Info: &model.TableInfo{
				ID:   int64(i),
				Name: model.NewCIStr("test" + strconv.Itoa(i)),
				Columns: []*model.ColumnInfo{{
					ID:        1,
					Name:      model.NewCIStr("id"),
					FieldType: *intField,
					State:     model.StatePublic,
				}},
				Charset: "utf8mb4",
				Collate: "utf8mb4_bin",
			},
		}
	}
	_, _, err = client.CreateTables(s.mock.Domain, tables, 0)
	c.Assert(err, IsNil)

	// exist different tables
	tables[1].Info.IsCommonHandle = true
	c.Assert(client.PreCheckTableClusterIndex(tables, nil, s.mock.Domain),
		ErrorMatches, `.*@@tidb_enable_clustered_index should be ON \(backup table = true, created table = false\).*`)

	// exist different DDLs
	jobs := []*model.Job{{
		ID:         5,
		Type:       model.ActionCreateTable,
		SchemaName: "test",
		Query:      "",
		BinlogInfo: &model.HistoryInfo{
			TableInfo: &model.TableInfo{
				Name:           model.NewCIStr("test1"),
				IsCommonHandle: true,
			},
		},
	}}
	c.Assert(client.PreCheckTableClusterIndex(nil, jobs, s.mock.Domain),
		ErrorMatches, `.*@@tidb_enable_clustered_index should be ON \(backup table = true, created table = false\).*`)

	// should pass pre-check cluster index
	tables[1].Info.IsCommonHandle = false
	jobs[0].BinlogInfo.TableInfo.IsCommonHandle = false
	c.Assert(client.PreCheckTableClusterIndex(tables, jobs, s.mock.Domain), IsNil)
}
