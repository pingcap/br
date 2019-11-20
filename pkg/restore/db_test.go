package restore

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

	"github.com/pingcap/br/pkg/utils"
)

var _ = Suite(&testRestoreSchemaSuite{})

type testRestoreSchemaSuite struct {
	mock *utils.MockCluster
}

func (s *testRestoreSchemaSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = utils.NewMockCluster()
	c.Assert(err, IsNil)
}
func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testRestoreSchemaSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testRestoreSchemaSuite) TestRestoreAutoIncID(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

	tk := testkit.NewTestKit(c, s.mock.Storage)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (10);")
	// Query the current AutoIncID
	autoIncID, err := strconv.ParseUint(tk.MustQuery("admin show t next_row_id").Rows()[0][3].(string), 10, 64)
	c.Assert(err, IsNil, Commentf("Error query auto inc id: %s", err))
	// Get schemas of db and table
	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
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
	idAlloc := autoid.NewAllocator(s.mock.Storage, dbInfo.ID, false)
	globalAutoID, err := idAlloc.NextGlobalAutoID(table.Schema.ID)
	c.Assert(err, IsNil, Commentf("Error allocate next auto id"))
	c.Assert(autoIncID, Equals, uint64(globalAutoID))
	// Alter AutoIncID to the next AutoIncID + 100
	table.Schema.AutoIncID = globalAutoID + 100
	err = s.mock.DB.AlterAutoIncID(context.Background(), &table)
	c.Assert(err, IsNil, Commentf("Error alter auto inc id: %s %s", err, s.mock.DSN))
	// Check if AutoIncID is altered successfully
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show t next_row_id").Rows()[0][3].(string), 10, 64)
	c.Assert(err, IsNil, Commentf("Error query auto inc id: %s", err))
	c.Assert(autoIncID, Equals, uint64(globalAutoID+100))
}
