package restore

import (
	"context"
	"math"
	"strconv"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"

	"github.com/pingcap/br/pkg/utils"
)

var _ = Suite(&testRestoreSchemaSuite{})

type testRestoreSchemaSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	db        *DB
}

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testRestoreSchemaSuite) SetUpSuite(c *C) {
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)
	c.Assert(err, IsNil)
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	s.db, err = NewDB(store)
	c.Assert(err, IsNil)
}

func (s *testRestoreSchemaSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testRestoreSchemaSuite) startServer(c *C) {
	mvccStore := mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(mockstore.WithMVCCStore(mvccStore))
	c.Assert(err, IsNil)
	s.db, err = NewDB(store)
	c.Assert(err, IsNil)
}

func (s *testRestoreSchemaSuite) stopServer(*C) {
	if s.db != nil {
		s.db.Close()
	}
}

func (s *testRestoreSchemaSuite) TestRestoreAutoIncID(c *C) {
	s.startServer(c)
	defer s.stopServer(c)
	tk := testkit.NewTestKit(c, s.db.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (10);")
	// Query the current AutoIncID
	autoIncID, err := strconv.ParseUint(tk.MustQuery("admin show t next_row_id").Rows()[0][3].(string), 10, 64)
	c.Assert(err, IsNil, Commentf("Error query auto inc id: %s", err))
	// Get schemas of db and table
	info, err := s.db.dom.GetSnapshotInfoSchema(math.MaxUint64)
	c.Assert(err, IsNil, Commentf("Error get snapshot info schema: %s", err))
	dbInfo, exists := info.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue, Commentf("Error get db info"))
	tableInfo, err := info.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil, Commentf("Error get table info: %s", err))
	table := utils.Table{
		Schema: tableInfo.Meta(),
		Db:     dbInfo,
	}
	// Get the next AutoIncID
	idAlloc := autoid.NewAllocator(s.db.store, dbInfo.ID, false)
	globalAutoID, err := idAlloc.NextGlobalAutoID(table.Schema.ID)
	c.Assert(err, IsNil, Commentf("Error allocate next auto id"))
	c.Assert(autoIncID, Equals, uint64(globalAutoID))
	// Alter AutoIncID to the next AutoIncID + 100
	table.Schema.AutoIncID = globalAutoID + 100
	err = s.db.AlterAutoIncID(context.Background(), &table)
	c.Assert(err, IsNil, Commentf("Error alter auto inc id: %s", err))
	// Check if AutoIncID is altered successfully
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show t next_row_id").Rows()[0][3].(string), 10, 64)
	c.Assert(err, IsNil, Commentf("Error query auto inc id: %s", err))
	c.Assert(autoIncID, Equals, uint64(globalAutoID+100))
}
