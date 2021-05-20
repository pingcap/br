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
	"crypto/rand"
	"math"
	"sort"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/codec"
)

type keySuffixSuite struct{}

var _ = Suite(&keySuffixSuite{})

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func (s *keySuffixSuite) TestKeySuffix(c *C) {
	inputs := []struct {
		key        []byte
		suffixBase []byte
		offset     int64
	}{
		{
			[]byte{0x0},
			[]byte("db-table1.sql"),
			0,
		},
		{
			randBytes(32),
			[]byte("db-table2.sql"),
			-2034,
		},
		{
			randBytes(32),
			[]byte("db-table2.sql"),
			math.MaxInt64,
		},
		{
			[]byte{0x31, 0x2e, 0x73, 0x71, keySuffixSeparator, 0x3a, 0x30, 0x2e, ':', 0x71, 0x6c},
			[]byte("db2-table3.sql"),
			1234567,
		},
		{
			[]byte{0, 0x0, 0x0, 0x0, 0x0, ':', 0x0, 0xf8, 0x40, 0x64, 0x62, keySuffixSeparator},
			[]byte("db3:table1.sql"),
			2345678,
		},
	}

	for _, input := range inputs {
		result := EncodeKeySuffix(nil, input.key, input.suffixBase, input.offset)

		// Verify the key.
		sep := bytes.LastIndexByte(result, keySuffixSeparator)
		c.Assert(sep, GreaterEqual, 0)
		_, key, err := codec.DecodeBytes(result[:sep], nil)
		c.Assert(err, IsNil)
		c.Assert(key, BytesEquals, input.key)

		// Verify the suffix format.
		colon := bytes.LastIndexByte(result, ':')
		c.Assert(colon, GreaterEqual, 0)
		suffixBase := result[sep+1 : colon]
		c.Assert(suffixBase, BytesEquals, input.suffixBase)
		offset, err := strconv.ParseInt(string(result[colon+1:]), 10, 64)
		c.Assert(err, IsNil)
		c.Assert(offset, Equals, input.offset)

		// Decode the result.
		key, err = DecodeKeySuffix(nil, result)
		c.Assert(err, IsNil)
		c.Assert(key, BytesEquals, input.key)
	}

	// Test panic encode.
	c.Assert(
		func() {
			EncodeKeySuffix(nil, []byte{0x00}, []byte("db"+string(keySuffixSeparator)+"table.sql"), 24325)
		},
		PanicMatches,
		"key suffix contains separator",
	)

	// Test error decode.
	errOutputs := [][]byte{
		{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf7},
		{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0xff, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8},
		{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0xff, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, keySuffixSeparator, 't', ':', '1', '2', '3', '4'},
	}
	for _, output := range errOutputs {
		_, err := DecodeKeySuffix(nil, output)
		c.Assert(err, NotNil)
	}
}

func (s *keySuffixSuite) TestKeySuffixOrder(c *C) {
	keys := [][]byte{
		{0x0, 0x1, 0x2},
		{0x0, 0x1, 0x3},
		{0x0, 0x1, 0x3, 0x4},
		{0x0, 0x1, 0x3, 0x4, 0x0},
		{0x0, 0x1, 0x3, 0x4, 0x0, 0x0, 0x0},
	}
	var encodedKeys [][]byte
	for i, key := range keys {
		encodedKeys = append(encodedKeys, EncodeKeySuffix(nil, key, []byte("table.sql"), int64(i*1234)))
	}
	sorted := sort.SliceIsSorted(encodedKeys, func(i, j int) bool {
		return bytes.Compare(encodedKeys[i], encodedKeys[j]) < 0
	})
	c.Assert(sorted, IsTrue)
}

func (s *keySuffixSuite) TestEncodeKeySuffixWithBuf(c *C) {
	key := randBytes(32)
	bufLen := 256
	buf := make([]byte, 0, bufLen)
	buf = append(buf, 0x01, 0x02, 0x03, 0x04)
	buf2 := EncodeKeySuffix(buf, key, []byte("table.sql"), 1234)
	// There should be no new buf allocated.
	// If we change a byte in `buf`, `buf2` can read the new byte.
	c.Assert(buf2[:4], BytesEquals, buf)
	buf[0] = 0xff
	c.Assert(buf2[0], Equals, byte(0xff))
	// Also verify the encode result.
	key2, err := DecodeKeySuffix(nil, buf2[4:])
	c.Assert(err, IsNil)
	c.Assert(key2, BytesEquals, key)
}

func (s *keySuffixSuite) TestDecodeKeySuffixWithBuf(c *C) {
	data := []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf7, keySuffixSeparator, 't', ':', '1', '2', '3', '4'}
	buf := make([]byte, 0, len(data))
	buf = append(buf, 0x01, 0x02, 0x03, 0x4)
	key, err := DecodeKeySuffix(buf, data)
	c.Assert(err, IsNil)
	// len(key) should be at least 4 bytes less than len(data). So there is no new buf allocated.
	c.Assert(key[:4], BytesEquals, buf)
	buf[0] = 0xff
	c.Assert(key[0], Equals, byte(0xff))
}
