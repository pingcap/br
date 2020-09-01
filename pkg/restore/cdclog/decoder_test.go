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
	"encoding/binary"
	"testing"

	"github.com/pingcap/check"
)

func Test(t *testing.T) { check.TestingT(t) }

type batchSuite struct {
	ddlEvents     []*MessageDDL
	ddlDecodeItem []*SortItem

	rowEvents     []*MessageRow
	rowDecodeItem []*SortItem
}

var updateCols = map[string]column{
	// json.Number
	"id":   {Type: 1, Value: "1"},
	"name": {Type: 2, Value: "test"},
}

var _ = check.Suite(&batchSuite{
	ddlEvents: []*MessageDDL{
		{"drop table event", 4},
		{"create table event", 3},
		{"drop table event", 5},
	},
	ddlDecodeItem: []*SortItem{
		{ItemType: DDL, Schema: "test", Table: "event", TS: 1, Meta: MessageDDL{"drop table event", 4}},
		{ItemType: DDL, Schema: "test", Table: "event", TS: 1, Meta: MessageDDL{"create table event", 3}},
		{ItemType: DDL, Schema: "test", Table: "event", TS: 1, Meta: MessageDDL{"drop table event", 5}},
	},

	rowEvents: []*MessageRow{
		{Update: updateCols},
		{PreColumns: updateCols},
		{Delete: updateCols},
	},
	rowDecodeItem: []*SortItem{
		{ItemType: RowChanged, Schema: "test", Table: "event", TS: 1, RowID: 0, Meta: MessageRow{Update: updateCols}},
		{ItemType: RowChanged, Schema: "test", Table: "event", TS: 1, RowID: 1, Meta: MessageRow{PreColumns: updateCols}},
		{ItemType: RowChanged, Schema: "test", Table: "event", TS: 1, RowID: 2, Meta: MessageRow{Delete: updateCols}},
	},
})

func buildEncodeRowData(events []*MessageRow) []byte {
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	data := append(make([]byte, 0), versionByte[:]...)
	key := messageKey{
		Ts:     1,
		Schema: "test",
		Table:  "event",
	}
	var LenByte [8]byte
	for i := 0; i < len(events); i++ {
		key.RowID = int64(i)
		keyBytes, _ := key.Encode()
		binary.BigEndian.PutUint64(LenByte[:], uint64(len(keyBytes)))
		data = append(data, LenByte[:]...)
		data = append(data, keyBytes...)
		eventBytes, _ := events[i].Encode()
		binary.BigEndian.PutUint64(LenByte[:], uint64(len(eventBytes)))
		data = append(data, LenByte[:]...)
		data = append(data, eventBytes...)
	}
	return data
}

func buildEncodeDDLData(events []*MessageDDL) []byte {
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	data := append(make([]byte, 0), versionByte[:]...)
	key := messageKey{
		Ts:     1,
		Schema: "test",
		Table:  "event",
	}
	var LenByte [8]byte
	for i := 0; i < len(events); i++ {
		keyBytes, _ := key.Encode()
		binary.BigEndian.PutUint64(LenByte[:], uint64(len(keyBytes)))
		data = append(data, LenByte[:]...)
		data = append(data, keyBytes...)
		eventBytes, _ := events[i].Encode()
		binary.BigEndian.PutUint64(LenByte[:], uint64(len(eventBytes)))
		data = append(data, LenByte[:]...)
		data = append(data, eventBytes...)
	}
	return data
}

func (s *batchSuite) TestDecoder(c *check.C) {
	var item *SortItem

	data := buildEncodeDDLData(s.ddlEvents)
	decoder, err := NewJSONEventBatchDecoder(data)
	c.Assert(err, check.IsNil)
	index := 0
	for {
		hasNext := decoder.HasNext()
		if !hasNext {
			break
		}
		item, err = decoder.NextDDLEvent()
		c.Assert(err, check.IsNil)
		c.Assert(item.Meta.(*MessageDDL), check.DeepEquals, s.ddlEvents[index])
		index++
	}

	data = buildEncodeRowData(s.rowEvents)
	decoder, err = NewJSONEventBatchDecoder(data)
	c.Assert(err, check.IsNil)
	index = 0
	for {
		hasNext := decoder.HasNext()
		if !hasNext {
			break
		}
		item, err = decoder.NextRowChangedEvent()
		c.Assert(err, check.IsNil)
		c.Assert(item.Meta.(*MessageRow), check.DeepEquals, s.rowEvents[index])
		c.Assert(item.RowID, check.Equals, int64(index))
		index++
	}
}
