package restore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/twinj/uuid"
	"go.uber.org/zap"
)

// LoadBackupTables loads schemas from BackupMeta
func LoadBackupTables(meta *backup.BackupMeta) (map[string]*Database, error) {
	databases := make(map[string]*Database)
	filePairs := groupFiles(meta.Files)

	for _, schema := range meta.Schemas {
		dbInfo := &model.DBInfo{}
		err := json.Unmarshal(schema.Db, dbInfo)
		if err != nil {
			log.Error("load db info failed", zap.Binary("data", schema.Db), zap.Error(err))
			return nil, errors.Trace(err)
		}

		tableInfo := &model.TableInfo{}
		err = json.Unmarshal(schema.Table, tableInfo)
		if err != nil {
			log.Error("load table info failed", zap.Binary("data", schema.Table), zap.Error(err))
			return nil, errors.Trace(err)
		}

		tableFiles := make([]*FilePair, 0)
		for _, pair := range filePairs {
			f := pair.Write
			if !bytes.HasPrefix(f.StartKey, tablecodec.TablePrefix()) && !bytes.HasPrefix(f.EndKey, tablecodec.TablePrefix()) {
				continue
			}
			startTableID := tablecodec.DecodeTableID(f.StartKey)
			endTableID := tablecodec.DecodeTableID(f.EndKey)

			if startTableID == tableInfo.ID || endTableID == tableInfo.ID {
				tableFiles = append(tableFiles, pair)
			}
		}
		table := &Table{
			Uuid:   uuid.NewV4().Bytes(),
			Db:     dbInfo,
			Schema: tableInfo,
			Files:  tableFiles,
		}

		db, ok := databases[table.Db.Name.O]
		if !ok {
			db = &Database{
				Schema: table.Db,
				Tables: make([]*Table, 0),
			}
			databases[table.Db.Name.O] = db
		}
		db.Tables = append(db.Tables, table)

		log.Info("load table", zap.Reflect("table", table))
	}

	return databases, nil
}

// FetchTableInfo fetches table schema from status address
func FetchTableInfo(addr string, dbName string, tableName string) (*model.TableInfo, error) {
	url := fmt.Sprintf("http://%s/schema/%s/%s", addr, dbName, tableName)
	log.Info("fetch table schema", zap.String("URL", url))
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var schema model.TableInfo
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &schema)
	if err != nil {
		return nil, err
	}
	return &schema, nil
}

// GroupIDPairs returns grouped id pairs
func GroupIDPairs(srcTable *model.TableInfo, destTable *model.TableInfo) (tableIDs []*import_kvpb.IdPair, indexIDs []*import_kvpb.IdPair) {
	tableIDs = make([]*import_kvpb.IdPair, 0)
	tableIDs = append(tableIDs, &import_kvpb.IdPair{
		OldId: srcTable.ID,
		NewId: destTable.ID,
	})

	indexIDs = make([]*import_kvpb.IdPair, 0)
	for _, src := range srcTable.Indices {
		for _, dest := range destTable.Indices {
			if src.Name == dest.Name {
				indexIDs = append(indexIDs, &import_kvpb.IdPair{
					OldId: src.ID,
					NewId: dest.ID,
				})
			}
		}
	}
	log.Info("group id pairs",
		zap.Reflect("src", srcTable),
		zap.Reflect("dest", destTable),
		zap.Reflect("table_id", tableIDs),
		zap.Reflect("index_id", indexIDs),
	)

	return
}

func groupFiles(files []*backup.File) (filePairs []*FilePair) {
	filePairs = make([]*FilePair, 0)
	for _, file := range files {
		if strings.HasSuffix(file.Name, "write") {
			var defaultFile *backup.File
			defaultName := strings.TrimSuffix(file.Name, "write") + "default"
			for _, f := range files {
				if f.Name == defaultName {
					defaultFile = f
				}
			}
			filePairs = append(filePairs, &FilePair{
				Default: defaultFile,
				Write:   file,
			})
		}
	}
	return
}
