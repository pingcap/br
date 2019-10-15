package restore

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	restore_util "github.com/5kbpers/tidb-tools/pkg/restore-util"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/twinj/uuid"
	"go.uber.org/zap"
)

var (
	dataKeyPrefix   = []byte{'z'}
	recordPrefixSep = []byte("_r")
)

// LoadBackupTables loads schemas from BackupMeta
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
		tableFiles := make([]*File, 0)
		for _, fileMeta := range meta.Files {
			// If the file do not contains any table data, skip it.
			if !bytes.HasPrefix(fileMeta.GetStartKey(), tablecodec.TablePrefix()) &&
				!bytes.HasPrefix(fileMeta.EndKey, tablecodec.TablePrefix()) {
				continue
			}
			startTableID := tablecodec.DecodeTableID(fileMeta.GetStartKey())
			endTableID := tablecodec.DecodeTableID(fileMeta.GetEndKey())
			// If the file contains a part of the data of the table, append it to the slice.
			if startTableID == tableInfo.ID || endTableID == tableInfo.ID {
				tableFiles = append(tableFiles, &File{
					UUID: uuid.NewV4().Bytes(),
					Meta: fileMeta,
				})
			}
		}
		table := &Table{
			Db:     dbInfo,
			Schema: tableInfo,
			Files:  tableFiles,
		}
		db.Tables = append(db.Tables, table)
	}

	dbNames := make([]string, 0, len(databases))
	for name := range databases {
		dbNames = append(dbNames, name)
	}
	log.Info("load databases", zap.Reflect("db", dbNames))

	return databases, nil
}

func GetRewriteRules(srcTable *model.TableInfo, destTable *model.TableInfo) []*import_sstpb.RewriteRule {
	rules := make([]*import_sstpb.RewriteRule, 0, len(srcTable.Indices)+1)

	rules = append(rules, &import_sstpb.RewriteRule{
		OldKeyPrefix: append(tablecodec.EncodeTablePrefix(srcTable.ID), recordPrefixSep...),
		NewKeyPrefix: append(tablecodec.EncodeTablePrefix(destTable.ID), recordPrefixSep...),
	})

	for _, srcIndex := range srcTable.Indices {
		for _, destIndex := range destTable.Indices {
			if srcIndex.Name == destIndex.Name {
				rules = append(rules, &import_sstpb.RewriteRule{
					OldKeyPrefix: tablecodec.EncodeTableIndexPrefix(srcTable.ID, srcIndex.ID),
					NewKeyPrefix: tablecodec.EncodeTableIndexPrefix(destTable.ID, destIndex.ID),
				})
			}
		}
	}

	return rules
}

func GetSSTMetaFromFile(file *File, region *metapb.Region) import_sstpb.SSTMeta {
	var cfName string
	if strings.Contains(file.Meta.Name, "default") {
		cfName = "default"
	} else if strings.Contains(file.Meta.Name, "write") {
		cfName = "write"
	}
	rangeStart := file.Meta.GetStartKey()
	if bytes.Compare(region.GetStartKey(), rangeStart) > 0 {
		rangeStart = region.GetStartKey()
	}
	rangeEnd := file.Meta.GetEndKey()
	if len(region.GetEndKey()) != 0 && bytes.Compare(region.GetEndKey(), rangeEnd) < 0 {
		rangeEnd = region.GetEndKey()
	}
	return import_sstpb.SSTMeta{
		Uuid:   file.UUID,
		Crc32:  file.Meta.GetCrc32(),
		CfName: cfName,
		Range: &import_sstpb.Range{
			Start: rangeStart,
			End:   rangeEnd,
		},
	}
}

type RetryableFunc func() error
type ContinueFunc func(error) bool

func WithRetry(retryableFunc RetryableFunc, continueFunc ContinueFunc, attempts uint, delayTime time.Duration) error {
	var lastErr error
	for i := uint(0); i < attempts; i++ {
		err := retryableFunc()
		if err != nil {
			lastErr = err
			// If this is the last attempt, do not wait
			if !continueFunc(err) || i == attempts-1 {
				break
			}
			time.Sleep(delayTime)
		} else {
			return nil
		}
	}
	return lastErr
}

func GetRanges(files []*File) []restore_util.Range {
	ranges := make([]restore_util.Range, 0, len(files))
	for _, file := range files {
		ranges = append(ranges, restore_util.Range{
			StartKey: file.Meta.GetStartKey(),
			EndKey:   file.Meta.GetEndKey(),
		})
	}
	return ranges
}
