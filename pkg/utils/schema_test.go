package utils

import (
	"encoding/json"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/tablecodec"
)

type testSchemaSuite struct{}

var _ = Suite(&testSchemaSuite{})

func mockBackupMeta(mockSchemas []*backup.Schema, mockFiles []*backup.File) *backup.BackupMeta {
	return &backup.BackupMeta{
		Files:   mockFiles,
		Schemas: mockSchemas,
	}
}

func (r *testSchemaSuite) TestLoadBackupMeta(c *C) {
	tblName := model.NewCIStr("t1")
	dbName := model.NewCIStr("test")
	tblID := int64(123)
	mockTbl := &model.TableInfo{
		ID:   tblID,
		Name: tblName,
	}

	mockDB := model.DBInfo{
		ID:   1,
		Name: dbName,
		Tables: []*model.TableInfo{
			mockTbl,
		},
	}
	dbBytes, err := json.Marshal(mockDB)
	c.Assert(err, IsNil)
	tblBytes, err := json.Marshal(mockTbl)
	c.Assert(err, IsNil)
	mockSchemas := []*backup.Schema{
		{
			Db:    dbBytes,
			Table: tblBytes,
		},
	}

	mockFiles := []*backup.File{
		// should include 1.sst
		{
			Name:     "1.sst",
			StartKey: tablecodec.EncodeRowKey(tblID, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(tblID+1, []byte("a")),
		},
		// shouldn't include 2.sst
		{
			Name:     "2.sst",
			StartKey: tablecodec.EncodeRowKey(tblID-1, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(tblID, []byte("a")),
		},
	}

	meta := mockBackupMeta(mockSchemas, mockFiles)
	dbs, err := LoadBackupTables(meta)
	tbl := dbs[dbName.String()].GetTable(tblName.String())
	c.Assert(err, IsNil)
	c.Assert(tbl.Files, HasLen, 1)
	c.Assert(tbl.Files[0].Name, Equals, "1.sst")
}
