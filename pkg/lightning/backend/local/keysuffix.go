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
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/codec"
)

var minEncodedKeyLen = codec.EncodedBytesLength(0) + 4 + 8

func reallocBytes(b []byte, n int) []byte {
	newSize := len(b) + n
	if cap(b) < newSize {
		bs := make([]byte, len(b), newSize)
		copy(bs, b)
		return bs
	}
	return b
}

func EncodedKeyBytesLength(key []byte) int {
	return codec.EncodedBytesLength(len(key)) + 12
}

// EncodeKeySuffix appends a suffix to the key with key's position.
// To reserved the original order, we must encode the original key first, and then append the suffix.
// `buf` is used to buffer data to avoid the cost of make slice.
func EncodeKeySuffix(buf []byte, key []byte, chunkIndex int32, offset int64) []byte {
	buf = codec.EncodeBytes(buf[:0], key)
	buf = reallocBytes(buf, 12)
	n := len(buf)
	buf = buf[:n+12]
	binary.BigEndian.PutUint32(buf[n:n+4], uint32(chunkIndex))
	binary.BigEndian.PutUint64(buf[n+4:n+12], uint64(offset))
	return buf
}

// DecodeKeySuffix decode the original key.
// `buf` is used to buffer data to avoid the cost of make slice.
func DecodeKeySuffix(buf []byte, data []byte) (key []byte, chunkIndex int32, offset int64, err error) {
	if len(data) < minEncodedKeyLen {
		return nil, 0, 0, errors.New("failed to decode key suffix, encoded key is too short")
	}
	_, key, err = codec.DecodeBytes(data[:len(data)-12], buf)
	if err != nil {
		return
	}
	chunkIndex = int32(binary.BigEndian.Uint32(data[len(data)-12 : len(data)-8]))
	offset = int64(binary.BigEndian.Uint64(data[len(data)-8:]))
	return
}
