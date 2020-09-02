// Copyright 2020 PingCAP, Inc.
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
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

// TableBuffer represents the kv buffer of this table.
// we restore one tableBuffer in one goroutine.
// this is the concurrent unit of log restore.
type TableBuffer struct {
	KvPairs []Row
	Size    int64

	KvEncoder Encoder
	tableInfo table.Table

	flushKVPairs int

	colNames []string
	colPerm  []int
}

// NewTableBuffer creates TableBuffer.
func NewTableBuffer(tbl table.Table, flushKVPairs int) *TableBuffer {
	kvEncoder := NewTableKVEncoder(tbl, &SessionOptions{
		Timestamp: time.Now().Unix(),
		// TODO make it config
		RowFormatVersion: "1",
	})

	columns := tbl.Meta().Cols()
	colNames := make([]string, 0, len(columns))
	colPerm := make([]int, 0, len(columns)+1)

	for i, col := range columns {
		colNames = append(colNames, col.Name.String())
		colPerm = append(colPerm, i)
	}
	if TableHasAutoRowID(tbl.Meta()) {
		colPerm = append(colPerm, -1)
	}

	return &TableBuffer{
		KvPairs:   make([]Row, 0, flushKVPairs),
		KvEncoder: kvEncoder,
		tableInfo: tbl,

		flushKVPairs: flushKVPairs,

		colNames: colNames,
		colPerm:  colPerm,
	}
}

func (t *TableBuffer) translateToDatum(row map[string]column) ([]types.Datum, error) {
	cols := make([]types.Datum, 0, len(row))
	for _, col := range t.colNames {
		val, err := row[col].toDatum()
		if err != nil {
			return nil, err
		}
		log.Debug("translate to datum", zap.String("col", col), zap.Stringer("val", val))
		cols = append(cols, val)
	}
	return cols, nil
}

func (t *TableBuffer) appendRow(
	row map[string]column,
	item *SortItem,
	encodeFn func(row []types.Datum,
		rowID int64,
		columnPermutation []int) (Row, error),
) error {
	cols, err := t.translateToDatum(row)
	if err != nil {
		return err
	}
	pair, err := encodeFn(cols, item.RowID, t.colPerm)
	if err != nil {
		return errors.Trace(err)
	}
	t.KvPairs = append(t.KvPairs, pair)
	t.Size++
	return nil
}

// Append appends the item to this buffer.
func (t *TableBuffer) Append(ctx context.Context, item *SortItem) error {
	log.Debug("Append item to buffer",
		zap.Stringer("table", t.tableInfo.Meta().Name),
	)
	row := item.Meta.(*MessageRow)

	select {
	case <-ctx.Done():
		log.Info("stop append to table buffer", zap.Stringer("table", t.tableInfo.Meta().Name))
	default:
		if row.PreColumns != nil {
			// Remove previous columns
			log.Debug("process update event", zap.Any("row", row))
			err := t.appendRow(row.PreColumns, item, t.KvEncoder.RemoveRecord)
			if err != nil {
				return err
			}
		}
		if row.Update != nil {
			// Add new columns
			if row.PreColumns == nil {
				log.Debug("process insert event", zap.Any("row", row))
			}
			err := t.appendRow(row.Update, item, t.KvEncoder.AddRecord)
			if err != nil {
				return err
			}
		}
		if row.Delete != nil {
			// Remove current columns
			log.Debug("process delete event", zap.Any("row", row))
			err := t.appendRow(row.Delete, item, t.KvEncoder.RemoveRecord)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ShouldApply tells whether we should flush memory kv buffer to storage.
func (t *TableBuffer) ShouldApply() bool {
	// flush when reached flush kv len
	return len(t.KvPairs) >= t.flushKVPairs
}

// IsEmpty tells buffer is empty.
func (t *TableBuffer) IsEmpty() bool {
	return t.Size == 0
}

// Clear reset the buffer.
func (t *TableBuffer) Clear() {
	t.KvPairs = t.KvPairs[:0]
	t.Size = 0
}
