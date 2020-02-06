package checksum

import (
	"context"
	"math"
	"testing"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tipb/go-tipb"

	"github.com/pingcap/br/pkg/utils"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testChecksumSuite{})

type testChecksumSuite struct {
	mock *utils.MockCluster
}

func (s *testChecksumSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = utils.NewMockCluster()
	c.Assert(err, IsNil)
}

func (s *testChecksumSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testChecksumSuite) getTableInfo(c *C, db, table string) *model.TableInfo {
	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	c.Assert(err, IsNil)
	cDBName := model.NewCIStr(db)
	cTableName := model.NewCIStr(table)
	tableInfo, err := info.TableByName(cDBName, cTableName)
	c.Assert(err, IsNil)
	return tableInfo.Meta()
}

func (s *testChecksumSuite) TestChecksum(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

	tk := testkit.NewTestKit(c, s.mock.Storage)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("insert into t1 values (10);")
	tableInfo1 := s.getTableInfo(c, "test", "t1")
	exe1, err := NewExecutorBuilder(tableInfo1, math.MaxUint64).Build()
	c.Assert(err, IsNil)
	c.Assert(len(exe1.reqs), Equals, 1)
	resp, err := exe1.Execute(context.TODO(), s.mock.Storage.GetClient(), func() {})
	c.Assert(err, IsNil)
	// MockCluster returns a dummy checksum (all fields are 1).
	c.Assert(resp.Checksum, Equals, uint64(1), Commentf("%v", resp))
	c.Assert(resp.TotalKvs, Equals, uint64(1), Commentf("%v", resp))
	c.Assert(resp.TotalBytes, Equals, uint64(1), Commentf("%v", resp))

	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int);")
	tk.MustExec("alter table t2 add index i2(a);")
	tk.MustExec("insert into t2 values (10);")
	tableInfo2 := s.getTableInfo(c, "test", "t2")
	exe2, err := NewExecutorBuilder(tableInfo2, math.MaxUint64).Build()
	c.Assert(err, IsNil)
	c.Assert(len(exe2.reqs), Equals, 2, Commentf("%v", tableInfo2))
	resp2, err := exe2.Execute(context.TODO(), s.mock.Storage.GetClient(), func() {})
	c.Assert(err, IsNil)
	c.Assert(resp2.Checksum, Equals, uint64(0), Commentf("%v", resp2))
	c.Assert(resp2.TotalKvs, Equals, uint64(2), Commentf("%v", resp2))
	c.Assert(resp2.TotalBytes, Equals, uint64(2), Commentf("%v", resp2))

	// Test rewrite rules
	tk.MustExec("alter table t1 add index i2(a);")
	tableInfo1 = s.getTableInfo(c, "test", "t1")
	oldTable := utils.Table{Info: tableInfo1}
	exe2, err = NewExecutorBuilder(tableInfo2, math.MaxUint64).
		SetOldTable(&oldTable).Build()
	c.Assert(err, IsNil)
	c.Assert(len(exe2.reqs), Equals, 2)
	req := tipb.ChecksumRequest{}
	err = proto.Unmarshal(exe2.reqs[0].Data, &req)
	c.Assert(err, IsNil)
	c.Assert(req.Rule, NotNil)
	req = tipb.ChecksumRequest{}
	err = proto.Unmarshal(exe2.reqs[1].Data, &req)
	c.Assert(err, IsNil)
	c.Assert(req.Rule, NotNil)
	resp2, err = exe2.Execute(context.TODO(), s.mock.Storage.GetClient(), func() {})
	c.Assert(err, IsNil)
	c.Assert(resp2, NotNil)
}
