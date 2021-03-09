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

package backend

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"

	"github.com/pingcap/br/pkg/lightning/common"
)

// invalidIterator is a trimmed down Iterator type which is invalid.
type invalidIterator struct {
	kv.Iterator
}

// Valid implements the kv.Iterator interface
func (*invalidIterator) Valid() bool {
	return false
}

// Close implements the kv.Iterator interface
func (*invalidIterator) Close() {
}

type kvMemBuf struct {
	kv.MemBuffer
	kvPairs []common.KvPair
	size    int
}

var _ kv.MemBuffer = &kvMemBuf{}

func (mb *kvMemBuf) Set(k kv.Key, v []byte) error {
	mb.kvPairs = append(mb.kvPairs, common.KvPair{
		Key: k.Clone(),
		Val: append([]byte{}, v...),
	})
	mb.size += len(k) + len(v)
	return nil
}

func (mb *kvMemBuf) Delete(k kv.Key) error {
	return errors.New("unsupported operation")
}

// Size returns sum of keys and values length.
func (mb *kvMemBuf) Size() int {
	return mb.size
}

func (mb *kvMemBuf) Discard() {
	// do nothing
}

func (mb *kvMemBuf) Flush() (int, error) {
	// do nothing
	return 0, nil
}

// Len returns the number of entries in the DB.
func (t *transaction) Len() int {
	return t.GetMemBuffer().Len()
}

// transaction is a trimmed down Transaction type which only supports adding a
// new KV pair.
type transaction struct {
	kv.Transaction
	kvMemBuf
}

var _ kv.Transaction = &transaction{}

func (t *transaction) NewStagingBuffer() kv.MemBuffer {
	return &t.kvMemBuf
}

func (t *transaction) GetMemBuffer() kv.MemBuffer {
	return &t.kvMemBuf
}

func (t *transaction) Discard() {
	t.kvMemBuf.Discard()
}

func (t *transaction) Flush() (int, error) {
	return t.kvMemBuf.Flush()
}

// Reset implements the kv.MemBuffer interface
func (t *transaction) Reset() {}

// Get implements the kv.Retriever interface
func (t *transaction) Get(ctx context.Context, key kv.Key) ([]byte, error) {
	return nil, kv.ErrNotExist
}

// Iter implements the kv.Retriever interface
func (t *transaction) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	return &invalidIterator{}, nil
}

// Set implements the kv.Mutator interface
func (t *transaction) Set(k kv.Key, v []byte) error {
	return t.kvMemBuf.Set(k, v)
}

func (t *transaction) Delete(k kv.Key) error {
	return t.kvMemBuf.Delete(k)
}

func (t *transaction) Size() int {
	return t.kvMemBuf.Size()
}

// SetOption implements the kv.Transaction interface
func (t *transaction) SetOption(opt kv.Option, val interface{}) {}

// DelOption implements the kv.Transaction interface
func (t *transaction) DelOption(kv.Option) {}

// SetAssertion implements the kv.Transaction interface
func (t *transaction) SetAssertion(kv.Key, kv.AssertionType) {}

// session is a trimmed down Session type which only wraps our own trimmed-down
// transaction type and provides the session variables to the TiDB library
// optimized for Lightning.
type session struct {
	sessionctx.Context
	txn  transaction
	vars *variable.SessionVars
	// currently, we only set `CommonAddRecordCtx`
	values map[fmt.Stringer]interface{}
}

// SessionOptions is the initial configuration of the session.
type SessionOptions struct {
	SQLMode   mysql.SQLMode
	Timestamp int64
	SysVars   map[string]string
	// a seed used for tableKvEncoder's auto random bits value
	AutoRandomSeed int64
}

func newSession(options *SessionOptions) *session {
	sqlMode := options.SQLMode
	vars := variable.NewSessionVars()
	vars.SkipUTF8Check = true
	vars.StmtCtx.InInsertStmt = true
	vars.StmtCtx.BatchCheck = true
	vars.StmtCtx.BadNullAsWarning = !sqlMode.HasStrictMode()
	vars.StmtCtx.TruncateAsWarning = !sqlMode.HasStrictMode()
	vars.StmtCtx.OverflowAsWarning = !sqlMode.HasStrictMode()
	vars.StmtCtx.AllowInvalidDate = sqlMode.HasAllowInvalidDatesMode()
	vars.StmtCtx.IgnoreZeroInDate = !sqlMode.HasStrictMode() || sqlMode.HasAllowInvalidDatesMode()
	vars.SQLMode = sqlMode
	if options.SysVars != nil {
		for k, v := range options.SysVars {
			vars.SetSystemVar(k, v)
		}
	}
	vars.StmtCtx.TimeZone = vars.Location()
	vars.SetSystemVar("timestamp", strconv.FormatInt(options.Timestamp, 10))
	vars.TxnCtx = nil

	s := &session{
		vars:   vars,
		values: make(map[fmt.Stringer]interface{}, 1),
	}

	return s
}

func (se *session) takeKvPairs() []common.KvPair {
	pairs := se.txn.kvMemBuf.kvPairs
	se.txn.kvMemBuf.kvPairs = make([]common.KvPair, 0, len(pairs))
	se.txn.kvMemBuf.size = 0
	return pairs
}

// Txn implements the sessionctx.Context interface
func (se *session) Txn(active bool) (kv.Transaction, error) {
	return &se.txn, nil
}

// GetSessionVars implements the sessionctx.Context interface
func (se *session) GetSessionVars() *variable.SessionVars {
	return se.vars
}

// SetValue saves a value associated with this context for key.
func (se *session) SetValue(key fmt.Stringer, value interface{}) {
	se.values[key] = value
}

// Value returns the value associated with this context for key.
func (se *session) Value(key fmt.Stringer) interface{} {
	return se.values[key]
}

// StmtAddDirtyTableOP implements the sessionctx.Context interface
func (se *session) StmtAddDirtyTableOP(op int, physicalID int64, handle int64) {}
