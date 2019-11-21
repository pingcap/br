package restore

import (
	"context"
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/testleak"

	"github.com/pingcap/br/pkg/utils"
)

var _ = Suite(&testRestoreSchemaSuite{})

type testRestoreClientSuite struct {
	mock *utils.MockCluster
}

func (s *testRestoreClientSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = utils.NewMockCluster()
	c.Assert(err, IsNil)
}

func (s *testRestoreClientSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testRestoreClientSuite) TestCreateTables(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

	client := Client{}
	db, err := NewDB(s.mock.Storage)
	c.Assert(err, IsNil)
	client.db = db
	client.ctx = context.Background()

	info, err := db.dom.GetSnapshotInfoSchema(math.MaxInt64)
	c.Assert(err, IsNil)
	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
	c.Assert(isExist, IsTrue)

	tables := make([]*utils.Table, 4)
	for i := 0; i < len(tables); i++ {
		tables[i] = &utils.Table{
			Db: dbSchema,
			Schema: &model.TableInfo{
				ID:   int64(i),
				Name: model.NewCIStr("test" + string(i)),
			},
		}
	}
	rules, newTables, err := client.CreateTables(tables)
	c.Assert(err, IsNil)
	for _, nt := range newTables {
		c.Assert(nt.Name.String(), Matches, "test[0-3]")
	}
	oldTableIDExist := make(map[int64]bool)
	newTableIDExist := make(map[int64]bool)
	for _, tr := range rules.Table {
		oldTableID := tablecodec.DecodeTableID(tr.GetOldKeyPrefix())
		c.Assert(oldTableIDExist[oldTableID], IsFalse, "table rule duplicate old table id")
		oldTableIDExist[oldTableID] = true

		newTableID := tablecodec.DecodeTableID(tr.GetNewKeyPrefix())
		c.Assert(newTableIDExist[newTableID], IsFalse, "table rule duplicate new table id")
		newTableIDExist[newTableID] = true
	}

	for i := 0; i < len(tables); i++ {
		c.Assert(oldTableIDExist[int64(i)], IsTrue, "table rule does not exist")
		c.Assert(oldTableIDExist[int64(i+1)], IsTrue, "table rule does not exist")
	}
}
