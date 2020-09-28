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

package utils

import "github.com/pingcap/br/pkg/manual"

const (
	bigValueSize = 1 << 16 // 64K
)

type bytesRecycleChan struct {
	ch chan []byte
}

// recycleChan is used for reusing allocated []byte so we can use memory more efficiently
//
// NOTE: we don't used a `sync.Pool` because when will sync.Pool release is depending on the
// garbage collector which always release the memory so late. Use a fixed size chan to reuse
// can decrease the memory usage to 1/3 compare with sync.Pool.
var recycleChan = &bytesRecycleChan{
	ch: make(chan []byte, 1024),
}

// Acquire try reuse the buffer from pool or alloc 1M buffer
// if there is nothing in pool.
func (c *bytesRecycleChan) Acquire() []byte {
	select {
	case b := <-c.ch:
		return b
	default:
		return manual.New(1 << 20) // 1M
	}
}

// Release free the buffer or put it into pool.
func (c *bytesRecycleChan) Release(w []byte) {
	select {
	case c.ch <- w:
		return
	default:
		manual.Free(w)
	}
}

// BytesBuffer represents the reuse buffer.
type BytesBuffer struct {
	bufs      [][]byte
	curBuf    []byte
	curIdx    int
	curBufIdx int
	curBufLen int
}

// NewBytesBuffer creates the BytesBuffer.
func NewBytesBuffer() *BytesBuffer {
	return &BytesBuffer{bufs: make([][]byte, 0, 128), curBufIdx: -1}
}

// AddBuf adds buffer to BytesBuffer.
func (b *BytesBuffer) AddBuf() {
	if b.curBufIdx < len(b.bufs)-1 {
		b.curBufIdx++
		b.curBuf = b.bufs[b.curBufIdx]
	} else {
		buf := recycleChan.Acquire()
		b.bufs = append(b.bufs, buf)
		b.curBuf = buf
		b.curBufIdx = len(b.bufs) - 1
	}

	b.curBufLen = len(b.curBuf)
	b.curIdx = 0
}

// Reset reset the buffer.
func (b *BytesBuffer) Reset() {
	if len(b.bufs) > 0 {
		b.curBuf = b.bufs[0]
		b.curBufLen = len(b.bufs[0])
		b.curBufIdx = 0
		b.curIdx = 0
	}
}

// Destroy free all buffer.
func (b *BytesBuffer) Destroy() {
	for _, buf := range b.bufs {
		recycleChan.Release(buf)
	}
	b.bufs = b.bufs[:0]
}

// TotalSize represents the total memory size of this BytesBuffer.
func (b *BytesBuffer) TotalSize() int64 {
	return int64(len(b.bufs)) * int64(1<<20)
}

// AddBytes add the bytes into this BytesBuffer.
func (b *BytesBuffer) AddBytes(bytes []byte) []byte {
	if len(bytes) > bigValueSize {
		return append([]byte{}, bytes...)
	}

	if b.curIdx+len(bytes) > b.curBufLen {
		b.AddBuf()
	}
	idx := b.curIdx
	copy(b.curBuf[idx:], bytes)
	b.curIdx += len(bytes)
	return b.curBuf[idx:b.curIdx]
}
