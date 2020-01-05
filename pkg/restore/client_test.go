package restore

import (
	"context"
	"math"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/testleak"

	"github.com/pingcap/br/pkg/utils"
)

var _ = Suite(&testRestoreClientSuite{})

type testRestoreClientSuite struct {
	mock *utils.MockCluster
}

func (s *testRestoreClientSuite) SetUpTest(c *C) {
	var err error
	s.mock, err = utils.NewMockCluster()
	c.Assert(err, IsNil)
}

func (s *testRestoreClientSuite) TearDownTest(c *C) {
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

	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxInt64)
	c.Assert(err, IsNil)
	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
	c.Assert(isExist, IsTrue)

	tables := make([]*utils.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.Charset = "binary"
	for i := len(tables) - 1; i >= 0; i-- {
		tables[i] = &utils.Table{
			Db: dbSchema,
			Schema: &model.TableInfo{
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

	client := Client{}
	db, err := NewDB(s.mock.Storage)
	c.Assert(err, IsNil)
	client.db = db
	client.ctx = context.Background()

	c.Assert(client.IsOnline(), IsFalse)
	client.EnableOnline()
	c.Assert(client.IsOnline(), IsTrue)
}
