package utils

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	// MetaFile represents file name
	MetaFile = "backupmeta"
)

// Table wraps the schema and files of a table.
type Table struct {
	Db     *model.DBInfo
	Schema *model.TableInfo
	Files  []*backup.File
}

// Database wraps the schema and tables of a database.
type Database struct {
	Schema *model.DBInfo
	Tables []*Table
}

// GetTable returns a table of the database by name.
func (db *Database) GetTable(name string) *Table {
	for _, table := range db.Tables {
		if table.Schema.Name.String() == name {
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
				Schema: dbInfo,
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
		// Find the files belong to the table
		tableFiles := make([]*backup.File, 0)
		for _, file := range meta.Files {
			// If the file do not contains any table data, skip it.
			if !bytes.HasPrefix(file.GetStartKey(), tablecodec.TablePrefix()) &&
				!bytes.HasPrefix(file.EndKey, tablecodec.TablePrefix()) {
				continue
			}
			startTableID := tablecodec.DecodeTableID(file.GetStartKey())
			endTableID := tablecodec.DecodeTableID(file.GetEndKey())
			// If the file contains a part of the data of the table, append it to the slice.
			if startTableID == tableInfo.ID || endTableID == tableInfo.ID {
				tableFiles = append(tableFiles, file)
			}
		}
		table := &Table{
			Db:     dbInfo,
			Schema: tableInfo,
			Files:  tableFiles,
		}
		db.Tables = append(db.Tables, table)
	}

	return databases, nil
}

// ResultSetToStringSlice changes the RecordSet to [][]string. port from tidb
func ResultSetToStringSlice(ctx context.Context, s session.Session, rs sqlexec.RecordSet) ([][]string, error) {
	rows, err := session.GetRows4Test(ctx, s, rs)
	if err != nil {
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		return nil, err
	}
	sRows := make([][]string, len(rows))
	for i := range rows {
		row := rows[i]
		iRow := make([]string, row.Len())
		for j := 0; j < row.Len(); j++ {
			if row.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
				iRow[j], err = d.ToString()
				if err != nil {
					return nil, err
				}
			}
		}
		sRows[i] = iRow
	}
	return sRows, nil
}
