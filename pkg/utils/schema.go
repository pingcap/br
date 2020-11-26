// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/tablecodec"
)

const (
	// LockFile represents file name
	LockFile = "backup.lock"
	// MetaFile represents file name
	MetaFile = "backupmeta"
	// MetaJSONFile represents backup meta json file name
	MetaJSONFile = "backupmeta.json"
	// SavedMetaFile represents saved meta file name for recovering later
	SavedMetaFile = "backupmeta.bak"
)

// Table wraps the schema and files of a table.
type Table struct {
	DB              *model.DBInfo
	Info            *model.TableInfo
	Crc64Xor        uint64
	TotalKvs        uint64
	TotalBytes      uint64
	Files           []*backup.File
	TiFlashReplicas int
	Stats           *handle.JSONTable
}

// NoChecksum checks whether the table has a calculated checksum.
func (tbl *Table) NoChecksum() bool {
	return tbl.Crc64Xor == 0 && tbl.TotalKvs == 0 && tbl.TotalBytes == 0
}

// NeedAutoID checks whether the table needs backing up with an autoid.
func NeedAutoID(tblInfo *model.TableInfo) bool {
	hasRowID := !tblInfo.PKIsHandle && !tblInfo.IsCommonHandle
	hasAutoIncID := tblInfo.GetAutoIncrementColInfo() != nil
	return hasRowID || hasAutoIncID
}

// Database wraps the schema and tables of a database.
type Database struct {
	Info   *model.DBInfo
	Tables []*Table
}

// GetTable returns a table of the database by name.
func (db *Database) GetTable(name string) *Table {
	for _, table := range db.Tables {
		if table.Info.Name.String() == name {
			return table
		}
	}
	return nil
}

// LoadBackupTables loads schemas from BackupMeta.
func LoadBackupTables(meta *backup.BackupMeta) (map[string]*Database, error) {
	databases := make(map[string]*Database)
	for _, schema := range meta.Schemas {
		// Parse the database schema.
		dbInfo := &model.DBInfo{}
		err := json.Unmarshal(schema.Db, dbInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// If the database do not ever added into the map, initialize a database object in the map.
		db, ok := databases[dbInfo.Name.String()]
		if !ok {
			db = &Database{
				Info:   dbInfo,
				Tables: make([]*Table, 0),
			}
			databases[dbInfo.Name.String()] = db
		}
		// Parse the table schema.
		tableInfo := &model.TableInfo{}
		err = json.Unmarshal(schema.Table, tableInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// stats maybe nil from old backup file.
		stats := &handle.JSONTable{}
		if schema.Stats != nil {
			// Parse the stats table.
			err = json.Unmarshal(schema.Stats, stats)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		partitions := make(map[int64]bool)
		if tableInfo.Partition != nil {
			for _, p := range tableInfo.Partition.Definitions {
				partitions[p.ID] = true
			}
		}
		// Find the files belong to the table
		tableFiles := make([]*backup.File, 0)
		for _, file := range meta.Files {
			// If the file do not contains any table data, skip it.
			if !bytes.HasPrefix(file.GetStartKey(), tablecodec.TablePrefix()) &&
				!bytes.HasPrefix(file.GetEndKey(), tablecodec.TablePrefix()) {
				continue
			}
			startTableID := tablecodec.DecodeTableID(file.GetStartKey())
			// If the file contains a part of the data of the table, append it to the slice.
			if ok := partitions[startTableID]; ok || startTableID == tableInfo.ID {
				tableFiles = append(tableFiles, file)
			}
		}
		table := &Table{
			DB:              dbInfo,
			Info:            tableInfo,
			Crc64Xor:        schema.Crc64Xor,
			TotalKvs:        schema.TotalKvs,
			TotalBytes:      schema.TotalBytes,
			Files:           tableFiles,
			TiFlashReplicas: int(schema.TiflashReplicas),
			Stats:           stats,
		}
		db.Tables = append(db.Tables, table)
	}

	return databases, nil
}

// ArchiveSize returns the total size of the backup archive.
func ArchiveSize(meta *backup.BackupMeta) uint64 {
	total := uint64(meta.Size())
	for _, file := range meta.Files {
		total += file.Size_
	}
	return total
}

// EncloseName formats name in sql.
func EncloseName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
