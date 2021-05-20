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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/codec"
)

const keySuffixSeparator = '@'

// EncodeKeySuffix appends a suffix to the key with key's position.
// To reserved the original order, we must encode the original key first, and then append the suffix.
// `buf` is used to buffer data to avoid the cost of make slice.
func EncodeKeySuffix(buf []byte, key []byte, suffixBase []byte, offset int64) []byte {
	if bytes.IndexByte(suffixBase, keySuffixSeparator) != -1 {
		panic("key suffix contains separator")
	}
	buf = buf[:0]
	buf = codec.EncodeBytes(buf, key)
	buf = append(buf, keySuffixSeparator)
	buf = append(buf, suffixBase...)
	buf = append(buf, ':')
	buf = strconv.AppendInt(buf, offset, 10)
	return buf
}

// DecodeKeySuffix decode the original key. To simplify the implementation and speed
// decoding, we don't verify the suffix format. We just trim the suffix by keySuffixSeparator.
// `buf` is used to buffer data to avoid the cost of make slice.
func DecodeKeySuffix(buf []byte, data []byte) ([]byte, error) {
	sep := bytes.LastIndexByte(data, keySuffixSeparator)
	if sep == -1 {
		return nil, errors.Errorf("failed to decode key, separator %s is missing", string(keySuffixSeparator))
	}
	_, key, err := codec.DecodeBytes(data[:sep], buf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return key, nil
}
