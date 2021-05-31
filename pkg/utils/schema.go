// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"

	"github.com/pingcap/br/pkg/metautil"
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

// NeedAutoID checks whether the table needs backing up with an autoid.
func NeedAutoID(tblInfo *model.TableInfo) bool {
	hasRowID := !tblInfo.PKIsHandle && !tblInfo.IsCommonHandle
	hasAutoIncID := tblInfo.GetAutoIncrementColInfo() != nil
	return hasRowID || hasAutoIncID
}

// Database wraps the schema and tables of a database.
type Database struct {
	Info   *model.DBInfo
	Tables []*metautil.Table
}

// GetTable returns a table of the database by name.
func (db *Database) GetTable(name string) *metautil.Table {
	for _, table := range db.Tables {
		if table.Info.Name.String() == name {
			return table
		}
	}
	return nil
}

// LoadBackupTables loads schemas from BackupMeta.
func LoadBackupTables(ctx context.Context, reader *metautil.MetaReader) (map[string]*Database, error) {
	ch := make(chan *metautil.Table)
	errCh := make(chan error)
	go func() {
		if err := reader.ReadSchemasFiles(ctx, ch); err != nil {
			errCh <- errors.Trace(err)
		}
		close(errCh)
		close(ch)
	}()

	databases := make(map[string]*Database)
	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case table, ok := <-ch:
			if !ok {
				return databases, nil
			}
			dbName := table.DB.Name.String()
			db, ok := databases[dbName]
			if !ok {
				db = &Database{
					Info:   table.DB,
					Tables: make([]*metautil.Table, 0),
				}
				databases[dbName] = db
			}
			db.Tables = append(db.Tables, table)
		}
	}
}

// ArchiveSize returns the total size of the backup archive.
func ArchiveSize(meta *backuppb.BackupMeta) uint64 {
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

// EncloseDBAndTable formats the database and table name in sql.
func EncloseDBAndTable(database, table string) string {
	return fmt.Sprintf("%s.%s", EncloseName(database), EncloseName(table))
}

// IsSysDB tests whether the database is system DB.
// Currently, the only system DB is mysql.
func IsSysDB(dbLowerName string) bool {
	return dbLowerName == mysql.SystemDB
}

// TemporaryDBName makes a 'private' database name.
func TemporaryDBName(db string) model.CIStr {
	return model.NewCIStr("__TiDB_BR_Temporary_" + db)
}
