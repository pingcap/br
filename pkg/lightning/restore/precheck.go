package restore

import (
	"context"
	"fmt"
	"github.com/pingcap/br/pkg/lightning/backend/kv"
	verify "github.com/pingcap/br/pkg/lightning/verification"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/table/tables"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/lightning/log"
	"github.com/pingcap/br/pkg/lightning/mydump"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"io"
)

const (
	maxSampleDataSize = 10 * 1024 * 1024
	maxSampleRowCount = 10 * 1024
)

func (rc *Controller) SampleDataFromTable(ctx context.Context, dbName string, tableMeta *mydump.MDTableMeta, tableInfo *model.TableInfo) error {
	if len(tableMeta.DataFiles) == 0 {
		return nil
	}
	sampleFile := tableMeta.DataFiles[0].FileMeta
	var reader storage.ReadSeekCloser
	var err error
	if sampleFile.Type == mydump.SourceTypeParquet {
		reader, err = mydump.OpenParquetReader(ctx, rc.store, sampleFile.Path, sampleFile.FileSize)
	} else {
		reader, err = rc.store.Open(ctx, sampleFile.Path)
	}
	if err != nil {
		return errors.Trace(err)
	}
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, tableInfo)

	kvEncoder, err := rc.backend.NewEncoder(tbl, &kv.SessionOptions{
		SQLMode:   rc.cfg.TiDB.SQLMode,
		Timestamp: 0,
		SysVars:   rc.sysVars,
		AutoRandomSeed: 0,
	})
	blockBufSize := int64(rc.cfg.Mydumper.ReadBlockSize)

	var parser mydump.Parser
	switch tableMeta.DataFiles[0].FileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := rc.cfg.Mydumper.CSV.Header
		parser = mydump.NewCSVParser(&rc.cfg.Mydumper.CSV, reader, blockBufSize, rc.ioWorkers, hasHeader)
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(rc.cfg.TiDB.SQLMode, reader, blockBufSize, rc.ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, rc.store, reader, sampleFile.Path)
		if err != nil {
			errors.Trace(err)
		}
	default:
		panic(fmt.Sprintf("file '%s' with unknown source type '%s'", sampleFile.Path, sampleFile.Type.String()))
	}
	logTask := log.With(zap.String("table", tableMeta.Name)).Begin(zap.InfoLevel, "restore file")
	igCols, err := rc.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(dbName, tableMeta.Name, rc.cfg.Mydumper.CaseSensitive)
	if err != nil {
		return errors.Trace(err)
	}

	initializedColumns, reachEOF := false, false
	var columnPermutation []int
	var kvSize uint64 = 0
	var rowSize uint64 = 0
	rowCount := 0
	dataKVs := rc.backend.MakeEmptyRows()
	indexKVs := rc.backend.MakeEmptyRows()
	lastKey := make([]byte, 1)
	for !reachEOF {
		offset, _ := parser.Pos()
		err = parser.ReadRow()
		columnNames := parser.Columns()

		switch errors.Cause(err) {
		case nil:
			if !initializedColumns {
				if len(columnPermutation) == 0 {
					columnPermutation, err = createColumnPermutation(columnNames, igCols.Columns, tableInfo)
				}
				initializedColumns = true
			}
		case io.EOF:
			reachEOF = true
			break
		default:
			err = errors.Annotatef(err, "in file  offset %d", offset)
			continue
		}
		lastRow := parser.LastRow()
		// sql -> kv
		for _, r := range lastRow.Row {
			rowSize += uint64(r.Length())
		}
		rowCount += 1

		var dataChecksum, indexChecksum verify.KVChecksum
		kvs, encodeErr := kvEncoder.Encode(logTask.Logger, lastRow.Row, lastRow.RowID, columnPermutation, offset)
		parser.RecycleRow(lastRow)
		if encodeErr != nil {
			err = errors.Annotatef(encodeErr, "in file at offset %d", offset)
			continue
		}
		kvs.ClassifyAndAppend(&dataKVs, &dataChecksum, &indexKVs, &indexChecksum)
		for _, kv := range kv.KvPairsFromRows(dataKVs) {
			if len(lastKey) == 0 ||  {

			}
		}
		kvSize += kvs.Size()
		dataKVs = dataKVs.Clear()
		indexKVs = indexKVs.Clear()
		failpoint.Inject("mock-kv-size", func(val failpoint.Value) {
			kvSize += uint64(val.(int))
		})
		if rowSize > maxSampleDataSize && rowCount > maxSampleRowCount {
			break
		}
	}

	if kvSize > rowSize {
		tableMeta.IndexRatio = float64(kvSize - rowSize) / float64(rowSize)
	}

	return nil
}
func (rc *Controller) SampleSourceData(ctx context.Context) error {
	for _, db := range rc.dbMetas {
		info, ok := rc.dbInfos[db.Name]
		if !ok {
			continue
		}
		for _, tbl := range db.Tables {
			tableInfo, ok := info.Tables[tbl.Name]
			if ok {
				if err := rc.SampleDataFromTable(ctx, db.Name, tbl, tableInfo.Core); err != nil {
				}
			}
		}
	}
}
