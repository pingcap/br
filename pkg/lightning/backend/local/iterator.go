// Copyright 2021 PingCAP, Inc.
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

package local

import (
	"bytes"
	"context"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/multierr"

	"github.com/pingcap/br/pkg/lightning/log"
	"github.com/pingcap/br/pkg/logutil"
)

// Iterator is the abstract interface for iterating an engine.
type Iterator interface {
	First() bool
	Last() bool
	Next() bool
	Key() []byte
	Value() []byte
	Valid() bool
	Error() error
	Close() error
}

const maxDuplicateBatchSize = 4 << 20

// duplicateNotifyFunc is called when duplicate is detected. A duplicate pair will trigger a notify.
type duplicateNotifyFunc func()

// lazyOpenBatchFunc is used for duplicateIterator to lazily open a *pebble.Batch for recording duplicates.
type lazyOpenBatchFunc func() (*pebble.Batch, error)

type duplicateIterator struct {
	ctx       context.Context
	iter      *pebble.Iterator
	curKey    []byte
	curRawKey []byte
	curVal    []byte
	nextKey   []byte
	err       error

	lazyOpenBatch   lazyOpenBatchFunc
	duplicateNotify duplicateNotifyFunc

	writeBatch     *pebble.Batch
	writeBatchSize int64
}

func (d *duplicateIterator) First() bool {
	if d.err != nil || !d.iter.First() {
		return false
	}
	d.fillCurKV()
	return d.err == nil
}

func (d *duplicateIterator) Last() bool {
	if d.err != nil || !d.iter.Last() {
		return false
	}
	d.fillCurKV()
	return d.err == nil
}

func (d *duplicateIterator) fillCurKV() {
	d.curKey, d.err = decodeKeyWithSuffix(d.curKey[:0], d.iter.Key())
	d.curRawKey = append(d.curRawKey[:0], d.iter.Key()...)
	d.curVal = append(d.curVal[:0], d.iter.Value()...)
}

func (d *duplicateIterator) flush() {
	d.err = d.writeBatch.Commit(pebble.Sync)
	d.writeBatch.Reset()
	d.writeBatchSize = 0
}

func (d *duplicateIterator) record(key []byte, val []byte) {
	if d.duplicateNotify != nil {
		d.duplicateNotify()
	}
	if d.writeBatch == nil {
		d.writeBatch, d.err = d.lazyOpenBatch()
		if d.err != nil {
			return
		}
	}
	d.err = d.writeBatch.Set(key, val, nil)
	if d.err != nil {
		return
	}
	d.writeBatchSize += int64(len(key) + len(val))
	if d.writeBatchSize >= maxDuplicateBatchSize {
		d.flush()
	}
}

func (d *duplicateIterator) Next() bool {
	recordFirst := false
	for d.err == nil && d.ctx.Err() == nil && d.iter.Next() {
		d.nextKey, d.err = decodeKeyWithSuffix(d.nextKey[:0], d.iter.Key())
		if d.err != nil {
			return false
		}
		if bytes.Compare(d.nextKey, d.curKey) != 0 {
			d.curKey, d.nextKey = d.nextKey, d.curKey[:0]
			d.curRawKey = append(d.curRawKey[:0], d.iter.Key()...)
			d.curVal = append(d.curVal[:0], d.iter.Value()...)
			return true
		}
		log.L().Debug("duplicate key detected", logutil.Key("key", d.curKey))
		if !recordFirst {
			d.record(d.curRawKey, d.curVal)
			recordFirst = true
		}
		d.record(d.iter.Key(), d.iter.Value())
	}
	if d.err == nil {
		d.err = d.ctx.Err()
	}
	return false
}

func (d *duplicateIterator) Key() []byte {
	return d.curKey
}

func (d *duplicateIterator) Value() []byte {
	return d.curVal
}

func (d *duplicateIterator) Valid() bool {
	return d.err == nil && d.iter.Valid()
}

func (d *duplicateIterator) Error() error {
	return multierr.Combine(d.iter.Error(), d.err)
}

func (d *duplicateIterator) Close() error {
	if d.writeBatch != nil {
		if d.err == nil {
			d.flush()
		}
		d.writeBatch.Close()
	}
	return d.iter.Close()
}

var _ Iterator = &duplicateIterator{}

func newDuplicateIterator(ctx context.Context, db *pebble.DB, opts *pebble.IterOptions,
	lazyOpenBatch lazyOpenBatchFunc, duplicateNotify duplicateNotifyFunc) Iterator {
	newOpts := &pebble.IterOptions{TableFilter: opts.TableFilter}
	if len(opts.LowerBound) > 0 {
		newOpts.LowerBound = codec.EncodeBytes(nil, opts.LowerBound)
	}
	if len(opts.UpperBound) > 0 {
		newOpts.UpperBound = codec.EncodeBytes(nil, opts.UpperBound)
	}
	return &duplicateIterator{
		ctx:             ctx,
		iter:            db.NewIter(newOpts),
		duplicateNotify: duplicateNotify,
		lazyOpenBatch:   lazyOpenBatch,
	}
}
