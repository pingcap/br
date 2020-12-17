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

package kv

import (
	"bytes"
	"sort"
	"sync"

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

// PairIter abstract iterator method for Ingester.
type PairIter interface {
	// IsEmpty checks whether iter has no pairs.
	IsEmpty() bool
	// Error return current error on this iter.
	Error() error
	// First return the first key in this iter.
	First() []byte
	// Last return the last key in this iter.
	Last() []byte
	// Valid check this iter reach the end.
	Valid() bool
	// Next moves this iter forward.
	Next() bool
	// Key represents current position pair's key.
	Key() []byte
	// Value represents current position pair's Value.
	Value() []byte
	// Close close this iter.
	Close() error
}

// SimplePairIterGen generate iter with range.
type SimplePairIterGen struct {
	pairs Pairs
}

// NewSimplePairIterGen creates SimplePairIterGen.
func NewSimplePairIterGen(pairs Pairs) *SimplePairIterGen {
	return &SimplePairIterGen{
		pairs: pairs,
	}
}

// GenerateIter generate SimplePairIter with given range[start, end].
func (g *SimplePairIterGen) GenerateIter(start []byte, end []byte) PairIter {
	startIndex := sort.Search(len(g.pairs), func(i int) bool {
		return bytes.Compare(start, g.pairs[i].Key) < 1
	})
	endIndex := sort.Search(len(g.pairs), func(i int) bool {
		return bytes.Compare(end, g.pairs[i].Key) < 1
	})
	return newSimpleKeyIter(g.pairs[startIndex : endIndex+1])
}

// SimplePairIter represents simple pair iterator.
// which is used for log restore.
type SimplePairIter struct {
	mu    sync.Mutex
	index int
	pairs Pairs
}

// newSimpleKeyIter creates SimplePairIter.
func newSimpleKeyIter(pairs Pairs) PairIter {
	return &SimplePairIter{
		index: -1,
		pairs: pairs,
	}
}

// IsEmpty implements PairIter.IsEmpty
func (s *SimplePairIter) IsEmpty() bool {
	return len(s.pairs) == 0
}

// Error implements PairIter.Error
func (s *SimplePairIter) Error() error {
	return nil
}

// First implements PairIter.First
func (s *SimplePairIter) First() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.IsEmpty() {
		return nil
	}
	s.index = 0
	return s.pairs[s.index].Key
}

// Last implements PairIter.Last
func (s *SimplePairIter) Last() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.IsEmpty() {
		return nil
	}
	s.index = len(s.pairs) - 1
	return s.pairs[s.index].Key
}

// Valid implements PairIter.Valid
func (s *SimplePairIter) Valid() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index == -1 {
		return false
	}
	return s.index < len(s.pairs)
}

// Next implements PairIter.Next
func (s *SimplePairIter) Next() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index++
	return s.index <= len(s.pairs)
}

// Key implements PairIter.Key
func (s *SimplePairIter) Key() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pairs[s.index].Key
}

// Value implements PairIter.Value
func (s *SimplePairIter) Value() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pairs[s.index].Val
}

// Close implements PairIter.Close
func (s *SimplePairIter) Close() error {
	return nil
}

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
	) (Row, int, error)

	// RemoveRecord encode encodes a row of SQL delete values into a backend-friendly format.
	RemoveRecord(
		row []types.Datum,
		rowID int64,
		columnPermutation []int,
	) (Row, int, error)
}

// Row represents a single encoded row.
type Row interface {
	// ClassifyAndAppend separates the data-like and index-like parts of the
	// encoded row, and appends these parts into the existing buffers and
	// checksums.
	ClassifyAndAppend(
		data *Pairs,
		dataChecksum *Checksum,
		indices *Pairs,
		indexChecksum *Checksum,
	)
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
				return errors.Trace(err)
			}
		}
		_ = encoder.AppendObject(zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
			enc.AddString("kind", kindStr[kind])
			enc.AddString("val", str)
			return nil
		}))
	}
	return nil
}

// Pairs represents the slice of Pair.
type Pairs []Pair

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
) (Row, int, error) {
	cols := kvcodec.tbl.Cols()

	var value types.Datum
	var err error

	record := kvcodec.recordCache
	if record == nil {
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
		switch {
		case j >= 0 && j < len(row):
			value, err = table.CastValue(kvcodec.se, row[j], col.ToInfo(), false, false)
			if err == nil {
				err = col.HandleBadNull(&value, kvcodec.se.vars.StmtCtx)
			}
		case isAutoIncCol:
			// we still need a conversion, e.g. to catch overflow with a TINYINT column.
			value, err = table.CastValue(kvcodec.se, types.NewIntDatum(rowID), col.ToInfo(), false, false)
		default:
			value, err = table.GetColDefaultValue(kvcodec.se, col.ToInfo())
		}
		if err != nil {
			return nil, 0, errors.Trace(err)
		}

		record = append(record, value)

		if isAutoRandom && isPk {
			typeBitsLength := uint64(mysql.DefaultLengthOfMysqlTypes[col.Tp] * 8)
			incrementalBits := typeBitsLength - kvcodec.tbl.Meta().AutoRandomBits
			hasSignBit := !mysql.HasUnsignedFlag(col.Flag)
			if hasSignBit {
				incrementalBits--
			}
			_ = kvcodec.tbl.RebaseAutoID(kvcodec.se, value.GetInt64()&((1<<incrementalBits)-1), false, autoid.AutoRandomType)
		}
		if isAutoIncCol {
			// TODO use auto incremental type
			_ = kvcodec.tbl.RebaseAutoID(kvcodec.se, value.GetInt64(), false, autoid.RowIDAllocType)
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
			return nil, 0, errors.Trace(err)
		}
		record = append(record, value)
		_ = kvcodec.tbl.RebaseAutoID(kvcodec.se, value.GetInt64(), false, autoid.RowIDAllocType)
	}
	_, err = kvcodec.tbl.AddRecord(kvcodec.se, record)
	if err != nil {
		log.Error("kv add Record failed",
			zap.Array("originalRow", rowArrayMarshaler(row)),
			zap.Array("convertedRow", rowArrayMarshaler(record)),
			zap.Error(err),
		)
		return nil, 0, errors.Trace(err)
	}

	pairs, size := kvcodec.se.takeKvPairs()
	kvcodec.recordCache = record[:0]
	return Pairs(pairs), size, nil
}

// RemoveRecord encode a row of data into KV pairs.
func (kvcodec *tableKVEncoder) RemoveRecord(
	row []types.Datum,
	rowID int64,
	columnPermutation []int,
) (Row, int, error) {
	cols := kvcodec.tbl.Cols()

	var value types.Datum
	var err error

	record := kvcodec.recordCache
	if record == nil {
		record = make([]types.Datum, 0, len(cols)+1)
	}

	for i, col := range cols {
		j := columnPermutation[i]
		isAutoIncCol := mysql.HasAutoIncrementFlag(col.Flag)
		switch {
		case j >= 0 && j < len(row):
			value, err = table.CastValue(kvcodec.se, row[j], col.ToInfo(), false, false)
			if err == nil {
				err = col.HandleBadNull(&value, kvcodec.se.vars.StmtCtx)
			}
		case isAutoIncCol:
			// we still need a conversion, e.g. to catch overflow with a TINYINT column.
			value, err = table.CastValue(kvcodec.se, types.NewIntDatum(rowID), col.ToInfo(), false, false)
		default:
			value, err = table.GetColDefaultValue(kvcodec.se, col.ToInfo())
		}
		if err != nil {
			return nil, 0, errors.Trace(err)
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
		return nil, 0, errors.Trace(err)
	}

	pairs, size := kvcodec.se.takeKvPairs()
	kvcodec.recordCache = record[:0]
	return Pairs(pairs), size, nil
}

// ClassifyAndAppend split Pairs to data rows and index rows.
func (kvs Pairs) ClassifyAndAppend(
	data *Pairs,
	dataChecksum *Checksum,
	indices *Pairs,
	indexChecksum *Checksum,
) {
	dataKVs := *data
	indexKVs := *indices

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

// Clear resets the Pairs.
func (kvs Pairs) Clear() Pairs {
	return kvs[:0]
}

// NextKey return the smallest []byte that is bigger than current bytes.
// special case when key is empty, empty bytes means infinity in our context, so directly return itself.
func NextKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{}
	}

	// in tikv <= 4.x, tikv will truncate the row key, so we should fetch the next valid row key
	// See: https://github.com/tikv/tikv/blob/f7f22f70e1585d7ca38a59ea30e774949160c3e8/components/raftstore/src/coprocessor/split_observer.rs#L36-L41
	if tablecodec.IsRecordKey(key) {
		tableID, handle, _ := tablecodec.DecodeRecordKey(key)
		return tablecodec.EncodeRowKeyWithHandle(tableID, handle.Next())
	}

	// if key is an index, directly append a 0x00 to the key.
	res := make([]byte, 0, len(key)+1)
	res = append(res, key...)
	res = append(res, 0)
	return res
}
