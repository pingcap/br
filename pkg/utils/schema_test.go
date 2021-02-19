// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"encoding/json"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/statistics/handle"
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
	mockStats := handle.JSONTable{
		DatabaseName: dbName.String(),
		TableName:    tblName.String(),
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
	statsBytes, err := json.Marshal(mockStats)
	c.Assert(err, IsNil)

	mockSchemas := []*backup.Schema{
		{
			Db:    dbBytes,
			Table: tblBytes,
			Stats: statsBytes,
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

func (r *testSchemaSuite) TestLoadBackupMetaPartionTable(c *C) {
	tblName := model.NewCIStr("t1")
	dbName := model.NewCIStr("test")
	tblID := int64(123)
	partID1 := int64(124)
	partID2 := int64(125)
	mockTbl := &model.TableInfo{
		ID:   tblID,
		Name: tblName,
		Partition: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{ID: partID1},
				{ID: partID2},
			},
		},
	}
	mockStats := handle.JSONTable{
		DatabaseName: dbName.String(),
		TableName:    tblName.String(),
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
	statsBytes, err := json.Marshal(mockStats)
	c.Assert(err, IsNil)

	mockSchemas := []*backup.Schema{
		{
			Db:    dbBytes,
			Table: tblBytes,
			Stats: statsBytes,
		},
	}

	mockFiles := []*backup.File{
		// should include 1.sst - 3.sst
		{
			Name:     "1.sst",
			StartKey: tablecodec.EncodeRowKey(partID1, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(partID1, []byte("b")),
		},
		{
			Name:     "2.sst",
			StartKey: tablecodec.EncodeRowKey(partID1, []byte("b")),
			EndKey:   tablecodec.EncodeRowKey(partID2, []byte("a")),
		},
		{
			Name:     "3.sst",
			StartKey: tablecodec.EncodeRowKey(partID2, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(partID2+1, []byte("b")),
		},
		// shouldn't include 4.sst
		{
			Name:     "4.sst",
			StartKey: tablecodec.EncodeRowKey(tblID-1, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(tblID, []byte("a")),
		},
	}

	meta := mockBackupMeta(mockSchemas, mockFiles)
	dbs, err := LoadBackupTables(meta)
	tbl := dbs[dbName.String()].GetTable(tblName.String())
	c.Assert(err, IsNil)
	c.Assert(tbl.Files, HasLen, 3)
	c.Assert(tbl.Files[0].Name, Equals, "1.sst")
	c.Assert(tbl.Files[1].Name, Equals, "2.sst")
	c.Assert(tbl.Files[2].Name, Equals, "3.sst")
}

func buildTableAndFiles(name string, tableID, fileCount int) (*model.TableInfo, []*backup.File) {
	tblName := model.NewCIStr(name)
	tblID := int64(tableID)
	mockTbl := &model.TableInfo{
		ID:   tblID,
		Name: tblName,
	}

	mockFiles := make([]*backup.File, 0, fileCount)
	for i := 0; i < fileCount; i++ {
		mockFiles = append(mockFiles, &backup.File{
			Name:     fmt.Sprintf("%d-%d.sst", tableID, i),
			StartKey: tablecodec.EncodeRowKey(tblID, []byte(fmt.Sprintf("%09d", i))),
			EndKey:   tablecodec.EncodeRowKey(tblID, []byte(fmt.Sprintf("%09d", i+1))),
		})
	}
	return mockTbl, mockFiles
}

func buildBenchmarkBackupmeta(c *C, dbName string, tableCount, fileCountPerTable int) *backup.BackupMeta {
	mockFiles := make([]*backup.File, 0, tableCount*fileCountPerTable)
	mockSchemas := make([]*backup.Schema, 0, tableCount)
	for i := 1; i <= tableCount; i++ {
		mockTbl, files := buildTableAndFiles(fmt.Sprintf("mock%d", i), i, fileCountPerTable)
		mockFiles = append(mockFiles, files...)

		mockDB := model.DBInfo{
			ID:   1,
			Name: model.NewCIStr(dbName),
			Tables: []*model.TableInfo{
				mockTbl,
			},
		}
		dbBytes, err := json.Marshal(mockDB)
		c.Assert(err, IsNil)
		tblBytes, err := json.Marshal(mockTbl)
		c.Assert(err, IsNil)
		mockSchemas = append(mockSchemas, &backup.Schema{
			Db:    dbBytes,
			Table: tblBytes,
		})
	}
	return mockBackupMeta(mockSchemas, mockFiles)
}

// Run `go test github.com/pingcap/br/pkg/utils -check.b -test.v` to get benchmark result.
func (r *testSchemaSuite) BenchmarkLoadBackupMeta64(c *C) {
	meta := buildBenchmarkBackupmeta(c, "bench", 64, 64)
	for i := 0; i < c.N; i++ {
		dbs, err := LoadBackupTables(meta)
		c.Assert(err, IsNil)
		c.Assert(dbs, HasLen, 1)
		c.Assert(dbs, HasKey, "bench")
		c.Assert(dbs["bench"].Tables, HasLen, 64)
	}
}

func (r *testSchemaSuite) BenchmarkLoadBackupMeta1024(c *C) {
	meta := buildBenchmarkBackupmeta(c, "bench", 1024, 64)
	for i := 0; i < c.N; i++ {
		dbs, err := LoadBackupTables(meta)
		c.Assert(err, IsNil)
		c.Assert(dbs, HasLen, 1)
		c.Assert(dbs, HasKey, "bench")
		c.Assert(dbs["bench"].Tables, HasLen, 1024)
	}
}

func (r *testSchemaSuite) BenchmarkLoadBackupMeta10240(c *C) {
	meta := buildBenchmarkBackupmeta(c, "bench", 10240, 64)
	for i := 0; i < c.N; i++ {
		dbs, err := LoadBackupTables(meta)
		c.Assert(err, IsNil)
		c.Assert(dbs, HasLen, 1)
		c.Assert(dbs, HasKey, "bench")
		c.Assert(dbs["bench"].Tables, HasLen, 10240)
	}
}
