// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cdclog

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var extraHandleColumnInfo = model.NewExtraHandleColInfo()

// Encoder encodes a row of SQL values into some opaque type which can be
// consumed by OpenEngine.WriteEncoded.
type Encoder interface {
	// Close the encoder.
	Close()

	// AddRecord encode encodes a row of SQL values into a backend-friendly format.
	AddRecord(
		row []types.Datum,
		rowID int64,
		columnPermutation []int,
	) (Row, error)

	// RemoveRecord encode encodes a row of SQL values into a backend-friendly format.
	RemoveRecord(
		row []types.Datum,
		rowID int64,
		columnPermutation []int,
	) (Row, error)
}

// Row represents a single encoded row.
type Row interface {
	// ClassifyAndAppend separates the data-like and index-like parts of the
	// encoded row, and appends these parts into the existing buffers and
	// checksums.
	ClassifyAndAppend(
		data *Rows,
		dataChecksum *KVChecksum,
		indices *Rows,
		indexChecksum *KVChecksum,
	)
}

// Rows represents a collection of encoded rows.
type Rows interface {
	// SplitIntoChunks splits the rows into multiple consecutive parts, each
	// part having total byte size less than `splitSize`. The meaning of "byte
	// size" should be consistent with the value used in `Row.ClassifyAndAppend`.
	SplitIntoChunks(splitSize int) []Rows

	// Clear returns a new collection with empty content. It may share the
	// capacity with the current instance. The typical usage is `x = x.Clear()`.
	Clear() Rows
}

type tableKVEncoder struct {
	tbl         table.Table
	se          *session
	recordCache []types.Datum
}

// NewTableKVEncoder creates the Encoder.
func NewTableKVEncoder(tbl table.Table, options *SessionOptions) Encoder {
	se := newSession(options)
	// Set CommonAddRecordCtx to session to reuse the slices and BufStore in AddRecord
	recordCtx := tables.NewCommonAddRecordCtx(len(tbl.Cols()))
	tables.SetAddRecordCtx(se, recordCtx)
	return &tableKVEncoder{
		tbl: tbl,
		se:  se,
	}
}

type rowArrayMarshaler []types.Datum

var kindStr = [...]string{
	types.KindNull:          "null",
	types.KindInt64:         "int64",
	types.KindUint64:        "uint64",
	types.KindFloat32:       "float32",
	types.KindFloat64:       "float64",
	types.KindString:        "string",
	types.KindBytes:         "bytes",
	types.KindBinaryLiteral: "binary",
	types.KindMysqlDecimal:  "decimal",
	types.KindMysqlDuration: "duration",
	types.KindMysqlEnum:     "enum",
	types.KindMysqlBit:      "bit",
	types.KindMysqlSet:      "set",
	types.KindMysqlTime:     "time",
	types.KindInterface:     "interface",
	types.KindMinNotNull:    "min",
	types.KindMaxValue:      "max",
	types.KindRaw:           "raw",
	types.KindMysqlJSON:     "json",
}

// MarshalLogArray implements the zapcore.ArrayMarshaler interface.
func (row rowArrayMarshaler) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, datum := range row {
		kind := datum.Kind()
		var str string
		var err error
		switch kind {
		case types.KindNull:
			str = "NULL"
		case types.KindMinNotNull:
			str = "-inf"
		case types.KindMaxValue:
			str = "+inf"
		default:
			str, err = datum.ToString()
			if err != nil {
				return err
			}
		}
		encoder.AppendObject(zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
			enc.AddString("kind", kindStr[kind])
			enc.AddString("val", str)
			return nil
		}))
	}
	return nil
}

// KvPairs represents the slice of KvPair.
type KvPairs []KvPair

// MakeRowsFromKvPairs converts a KvPair slice into a Rows instance. This is
// mainly used for testing only. The resulting Rows instance should only be used
// for the importer backend.
func MakeRowsFromKvPairs(pairs []KvPair) Rows {
	return KvPairs(pairs)
}

// MakeRowFromKvPairs converts a KvPair slice into a Row instance. This is
// mainly used for testing only. The resulting Row instance should only be used
// for the importer backend.
func MakeRowFromKvPairs(pairs []KvPair) Row {
	return KvPairs(pairs)
}

// Close ...
func (kvcodec *tableKVEncoder) Close() {
}

// AddRecord encode a row of data into KV pairs.
//
// See comments in `(*TableRestore).initializeColumns` for the meaning of the
// `columnPermutation` parameter.
func (kvcodec *tableKVEncoder) AddRecord(
	row []types.Datum,
	rowID int64,
	columnPermutation []int,
) (Row, error) {
	cols := kvcodec.tbl.Cols()

	var value types.Datum
	var err error
	var record []types.Datum

	if kvcodec.recordCache != nil {
		record = kvcodec.recordCache
	} else {
		record = make([]types.Datum, 0, len(cols)+1)
	}

	isAutoRandom := false
	if kvcodec.tbl.Meta().PKIsHandle && kvcodec.tbl.Meta().ContainsAutoRandomBits() {
		isAutoRandom = true
	}

	for i, col := range cols {
		j := columnPermutation[i]
		isAutoIncCol := mysql.HasAutoIncrementFlag(col.Flag)
		isPk := mysql.HasPriKeyFlag(col.Flag)
		if j >= 0 && j < len(row) {
			value, err = table.CastValue(kvcodec.se, row[j], col.ToInfo(), false, false)
			if err == nil {
				err = col.HandleBadNull(&value, kvcodec.se.vars.StmtCtx)
			}
		} else if isAutoIncCol {
			// we still need a conversion, e.g. to catch overflow with a TINYINT column.
			value, err = table.CastValue(kvcodec.se, types.NewIntDatum(rowID), col.ToInfo(), false, false)
		} else {
			value, err = table.GetColDefaultValue(kvcodec.se, col.ToInfo())
		}
		if err != nil {
			return nil, err
		}

		record = append(record, value)

		if isAutoRandom && isPk {
			typeBitsLength := uint64(mysql.DefaultLengthOfMysqlTypes[col.Tp] * 8)
			incrementalBits := typeBitsLength - kvcodec.tbl.Meta().AutoRandomBits
			hasSignBit := !mysql.HasUnsignedFlag(col.Flag)
			if hasSignBit {
				incrementalBits--
			}
			kvcodec.tbl.RebaseAutoID(kvcodec.se, value.GetInt64()&((1<<incrementalBits)-1), false, autoid.AutoRandomType)
		}
		if isAutoIncCol {
			kvcodec.tbl.RebaseAutoID(kvcodec.se, value.GetInt64(), false, autoid.AutoIncrementType)
		}
	}

	if TableHasAutoRowID(kvcodec.tbl.Meta()) {
		j := columnPermutation[len(cols)]
		if j >= 0 && j < len(row) {
			value, err = table.CastValue(kvcodec.se, row[j], extraHandleColumnInfo, false, false)
		} else {
			value, err = types.NewIntDatum(rowID), nil
		}
		if err != nil {
			return nil, err
		}
		record = append(record, value)
		kvcodec.tbl.RebaseAutoID(kvcodec.se, value.GetInt64(), false, autoid.RowIDAllocType)
	}
	_, err = kvcodec.tbl.AddRecord(kvcodec.se, record)
	if err != nil {
		log.Error("kv add Record failed",
			zap.Array("originalRow", rowArrayMarshaler(row)),
			zap.Array("convertedRow", rowArrayMarshaler(record)),
			zap.Error(err),
		)
		return nil, errors.Trace(err)
	}

	pairs := kvcodec.se.takeKvPairs()
	kvcodec.recordCache = record[:0]
	return KvPairs(pairs), nil
}

// RemoveRecord encode a row of data into KV pairs.
func (kvcodec *tableKVEncoder) RemoveRecord(
	row []types.Datum,
	rowID int64,
	columnPermutation []int,
) (Row, error) {
	cols := kvcodec.tbl.Cols()

	var value types.Datum
	var err error
	var record []types.Datum

	if kvcodec.recordCache != nil {
		record = kvcodec.recordCache
	} else {
		record = make([]types.Datum, 0, len(cols)+1)
	}

	for i, col := range cols {
		j := columnPermutation[i]
		isAutoIncCol := mysql.HasAutoIncrementFlag(col.Flag)
		if j >= 0 && j < len(row) {
			value, err = table.CastValue(kvcodec.se, row[j], col.ToInfo(), false, false)
			if err == nil {
				err = col.HandleBadNull(&value, kvcodec.se.vars.StmtCtx)
			}
		} else if isAutoIncCol {
			// we still need a conversion, e.g. to catch overflow with a TINYINT column.
			value, err = table.CastValue(kvcodec.se, types.NewIntDatum(rowID), col.ToInfo(), false, false)
		} else {
			value, err = table.GetColDefaultValue(kvcodec.se, col.ToInfo())
		}
		if err != nil {
			return nil, err
		}
		record = append(record, value)
	}
	err = kvcodec.tbl.RemoveRecord(kvcodec.se, kv.IntHandle(rowID), record)
	if err != nil {
		log.Error("kv remove record failed",
			zap.Array("originalRow", rowArrayMarshaler(row)),
			zap.Array("convertedRow", rowArrayMarshaler(record)),
			zap.Error(err),
		)
		return nil, errors.Trace(err)
	}

	pairs := kvcodec.se.takeKvPairs()
	kvcodec.recordCache = record[:0]
	return KvPairs(pairs), nil
}

// ClassifyAndAppend split KvPairs to data rows and index rows.
func (kvs KvPairs) ClassifyAndAppend(
	data *Rows,
	dataChecksum *KVChecksum,
	indices *Rows,
	indexChecksum *KVChecksum,
) {
	dataKVs := (*data).(KvPairs)
	indexKVs := (*indices).(KvPairs)

	for _, kv := range kvs {
		if kv.Key[tablecodec.TableSplitKeyLen+1] == 'r' {
			dataKVs = append(dataKVs, kv)
			dataChecksum.UpdateOne(kv)
		} else {
			indexKVs = append(indexKVs, kv)
			indexChecksum.UpdateOne(kv)
		}
	}

	*data = dataKVs
	*indices = indexKVs
}

// SplitIntoChunks is used in lightning.
func (kvs KvPairs) SplitIntoChunks(splitSize int) []Rows {
	if len(kvs) == 0 {
		return nil
	}

	res := make([]Rows, 0, 1)
	i := 0
	cumSize := 0

	for j, pair := range kvs {
		size := len(pair.Key) + len(pair.Val)
		if i < j && cumSize+size > splitSize {
			res = append(res, kvs[i:j])
			i = j
			cumSize = 0
		}
		cumSize += size
	}

	return append(res, kvs[i:])
}

// Clear resets the KvPairs.
func (kvs KvPairs) Clear() Rows {
	return kvs[:0]
}
