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
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
)

var recordPrefixSep = []byte("_r")

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

	dbNames := make([]string, 0, len(databases))
	for name := range databases {
		dbNames = append(dbNames, name)
	}
	log.Info("load databases", zap.Reflect("db", dbNames))

	return databases, nil
}

// GetRewriteRules returns the rewrite rule of the new table and the old table.
func GetRewriteRules(newTable *model.TableInfo, oldTable *model.TableInfo) *restore_util.RewriteRules {
	tableRule := &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTable.ID),
		NewKeyPrefix: tablecodec.EncodeTablePrefix(newTable.ID),
	}

	dataRules := make([]*import_sstpb.RewriteRule, 0, len(oldTable.Indices)+1)
	dataRules = append(dataRules, &import_sstpb.RewriteRule{
		OldKeyPrefix: append(tablecodec.EncodeTablePrefix(oldTable.ID), recordPrefixSep...),
		NewKeyPrefix: append(tablecodec.EncodeTablePrefix(newTable.ID), recordPrefixSep...),
	})

	for _, srcIndex := range oldTable.Indices {
		for _, destIndex := range newTable.Indices {
			if srcIndex.Name == destIndex.Name {
				dataRules = append(dataRules, &import_sstpb.RewriteRule{
					OldKeyPrefix: tablecodec.EncodeTableIndexPrefix(oldTable.ID, srcIndex.ID),
					NewKeyPrefix: tablecodec.EncodeTableIndexPrefix(newTable.ID, destIndex.ID),
				})
			}
		}
	}

	return &restore_util.RewriteRules{
		Table: []*import_sstpb.RewriteRule{tableRule},
		Data:  dataRules,
	}
}

// getSSTMetaFromFile compares the keys in file, region and rewrite rules, then returns a sst meta.
// It will rewrites the file start key and end key (if there is not any corresponding rewrite rule, set it to ""),
// then returns the range of the sst meta as [max(rewrite file start key, region start key), min(rewrite file end key, region end key)]
func getSSTMetaFromFile(id []byte, file *backup.File, region *metapb.Region, rewriteRules *restore_util.RewriteRules, needRewrite bool) import_sstpb.SSTMeta {
	// Get the column family of the file by the file name.
	var cfName string
	if strings.Contains(file.GetName(), "default") {
		cfName = "default"
	} else if strings.Contains(file.GetName(), "write") {
		cfName = "write"
	}
	// Find the overlapped part between the file and the region.
	// Here we rewrites the keys to compare with the keys of the region.
	rangeStart := rewriteRawKeyWithNewPrefix(file.GetStartKey(), rewriteRules)
	if bytes.Compare(region.GetStartKey(), rangeStart) > 0 {
		rangeStart = region.GetStartKey()
	}
	rangeEnd := rewriteRawKeyWithNewPrefix(file.GetEndKey(), rewriteRules)
	if len(rangeEnd) == 0 || (len(region.GetEndKey()) != 0 && bytes.Compare(region.GetEndKey(), rangeEnd) < 0) {
		rangeEnd = region.GetEndKey()
	}

	if !needRewrite {
		rangeStart = rewriteEncodedKeyWithOldPrefix(rangeStart, rewriteRules)
		rangeEnd = rewriteEncodedKeyWithOldPrefix(rangeEnd, rewriteRules)
	}
	if len(rangeEnd) == 0 {
		rangeEnd = []byte{0xff}
	}
	return import_sstpb.SSTMeta{
		Uuid:   id,
		CfName: cfName,
		Range: &import_sstpb.Range{
			Start: rangeStart,
			End:   rangeEnd,
		},
	}
}

type retryableFunc func() error
type continueFunc func(error) bool

func withRetry(retryableFunc retryableFunc, continueFunc continueFunc, attempts uint, delayTime time.Duration) error {
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

// GetRanges returns the ranges of the files.
func GetRanges(files []*backup.File) []restore_util.Range {
	ranges := make([]restore_util.Range, 0, len(files))
AppendRange:
	for _, file := range files {
		for _, rg := range ranges {
			if bytes.Equal(rg.StartKey, file.GetStartKey()) && bytes.Equal(rg.EndKey, file.GetEndKey()) {
				continue AppendRange
			}
		}
		ranges = append(ranges, restore_util.Range{
			StartKey: file.GetStartKey(),
			EndKey:   file.GetEndKey(),
		})
	}
	return ranges
}

// rules must be encoded
func findRegionRewriteRule(region *metapb.Region, rewriteRules *restore_util.RewriteRules) *import_sstpb.RewriteRule {
	for _, rule := range rewriteRules.Data {
		// regions may have the new prefix
		if bytes.HasPrefix(region.GetStartKey(), rule.GetNewKeyPrefix()) {
			return rule
		}
	}
	return nil
}

func encodeRewriteRules(rewriteRules *restore_util.RewriteRules) *restore_util.RewriteRules {
	encodedTableRules := make([]*import_sstpb.RewriteRule, 0, len(rewriteRules.Table))
	encodedDataRules := make([]*import_sstpb.RewriteRule, 0, len(rewriteRules.Data))
	for _, rule := range rewriteRules.Table {
		encodedTableRules = append(encodedTableRules, &import_sstpb.RewriteRule{
			OldKeyPrefix: encodeKeyPrefix(rule.GetOldKeyPrefix()),
			NewKeyPrefix: encodeKeyPrefix(rule.GetNewKeyPrefix()),
		})
	}
	for _, rule := range rewriteRules.Data {
		encodedDataRules = append(encodedDataRules, &import_sstpb.RewriteRule{
			OldKeyPrefix: encodeKeyPrefix(rule.GetOldKeyPrefix()),
			NewKeyPrefix: encodeKeyPrefix(rule.GetNewKeyPrefix()),
		})
	}
	return &restore_util.RewriteRules{
		Table: encodedTableRules,
		Data:  encodedDataRules,
	}
}

func encodeKeyPrefix(key []byte) []byte {
	encodedPrefix := make([]byte, 0)
	ungroupedLen := len(key) % 8
	encodedPrefix = append(encodedPrefix, codec.EncodeBytes([]byte{}, key[:len(key)-ungroupedLen])...)
	return append(encodedPrefix[:len(encodedPrefix)-9], key[len(key)-ungroupedLen:]...)
}

func rewriteRawKeyWithNewPrefix(key []byte, rewriteRules *restore_util.RewriteRules) []byte {
	if len(key) > 0 {
		ret := make([]byte, len(key))
		copy(ret, key)
		ret = codec.EncodeBytes([]byte{}, ret)
		for _, rule := range rewriteRules.Data {
			// regions may have the new prefix
			if bytes.HasPrefix(ret, rule.GetOldKeyPrefix()) {
				return bytes.Replace(ret, rule.GetOldKeyPrefix(), rule.GetNewKeyPrefix(), 1)
			}
		}
		for _, rule := range rewriteRules.Table {
			// regions may have the new prefix
			if bytes.HasPrefix(ret, rule.GetOldKeyPrefix()) {
				return bytes.Replace(ret, rule.GetOldKeyPrefix(), rule.GetNewKeyPrefix(), 1)
			}
		}
	}
	return []byte("")
}

func rewriteEncodedKeyWithOldPrefix(key []byte, rewriteRules *restore_util.RewriteRules) []byte {
	if len(key) > 0 {
		ret := make([]byte, len(key))
		copy(ret, key)
		for _, rule := range rewriteRules.Data {
			// regions may have the new prefix
			if bytes.HasPrefix(ret, rule.GetNewKeyPrefix()) {
				return bytes.Replace(ret, rule.GetNewKeyPrefix(), rule.GetOldKeyPrefix(), 1)
			}
		}
		for _, rule := range rewriteRules.Table {
			// regions may have the new prefix
			if bytes.HasPrefix(ret, rule.GetNewKeyPrefix()) {
				return bytes.Replace(ret, rule.GetNewKeyPrefix(), rule.GetOldKeyPrefix(), 1)
			}
		}
	}
	return []byte("")
}
