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

const (
	defaultKVLen = 1280
)

type TableBuffer struct {
	KvPairs []Row
	Size    int64

	KvEncoder Encoder
	tableInfo table.Table

	colNames []string
	colPerm  []int
}

func NewTableBuffer(tbl table.Table) *TableBuffer {
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
		KvPairs:   make([]Row, 0, defaultKVLen),
		KvEncoder: kvEncoder,
		tableInfo: tbl,

		colNames: colNames,
		colPerm:  colPerm,
	}
}

func (t *TableBuffer) translateToDatum(row map[string]column) []types.Datum {
	cols := make([]types.Datum, 0, len(row))
	for _, col := range t.colNames {
		cols = append(cols, row[col].toDatum())
	}
	return cols
}

func (t *TableBuffer) Append(ctx context.Context, item *SortItem) error {
	log.Debug("Append item to buffer",
		zap.Stringer("table", t.tableInfo.Meta().Name),
	)
	row := item.Meta.(*MessageRow)
	// FIXME given proper row ID after encode key
	rowID := int64(1)

	if row.Update != nil {
		if row.PreColumns != nil {
			log.Debug("process update event", zap.Any("row", row))
			oldCols := t.translateToDatum(row.PreColumns)
			pair, err := t.KvEncoder.RemoveRecord(oldCols, rowID, t.colPerm)
			if err != nil {
				return errors.Trace(err)
			}
			t.KvPairs = append(t.KvPairs, pair)

			newCols := t.translateToDatum(row.Update)
			pair, err = t.KvEncoder.AddRecord(newCols, rowID, t.colPerm)
			if err != nil {
				return errors.Trace(err)
			}
			t.KvPairs = append(t.KvPairs, pair)
		} else {
			log.Debug("process insert event", zap.Any("row", row))
			cols := t.translateToDatum(row.Update)
			pair, err := t.KvEncoder.AddRecord(cols, rowID, t.colPerm)
			if err != nil {
				return errors.Trace(err)
			}
			t.KvPairs = append(t.KvPairs, pair)
		}
	}
	if row.Delete != nil {
		log.Debug("process delete event", zap.Any("row", row))
		cols := t.translateToDatum(row.Delete)
		pair, err := t.KvEncoder.RemoveRecord(cols, rowID, t.colPerm)
		if err != nil {
			return errors.Trace(err)
		}
		t.KvPairs = append(t.KvPairs, pair)
	}
	return nil
}

func (t *TableBuffer) ShouldApply() bool {
	// flush when reached flush kv len
	return len(t.KvPairs) >= defaultKVLen
}

func (t *TableBuffer) Clear() {
	t.KvPairs = t.KvPairs[:0]
	t.Size = 0
}
