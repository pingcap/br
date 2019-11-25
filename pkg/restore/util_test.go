package restore

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/util/testleak"

	"github.com/pingcap/br/pkg/utils"
)

var _ = Suite(&testRestoreUtilSuite{})

type testRestoreUtilSuite struct {
	mock *utils.MockCluster
}

func (s *testRestoreUtilSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = utils.NewMockCluster()
	c.Assert(err, IsNil)
}

func (s *testRestoreUtilSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testRestoreUtilSuite) TestGetSSTMetaFromFile(c *C) {
	file := &backup.File{
		Name:     "file_default.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t1ccc"),
	}
	rule := &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("t1"),
		NewKeyPrefix: []byte("t2"),
	}
	region := &metapb.Region{
		StartKey: []byte("t2abc"),
		EndKey:   []byte("t3a"),
	}
	sstMeta := getSSTMetaFromFile([]byte{}, file, region, rule)
	c.Assert(string(sstMeta.GetRange().GetStart()), Equals, "t2abc")
	c.Assert(string(sstMeta.GetRange().GetEnd()), Equals, "t2\xff")
}
