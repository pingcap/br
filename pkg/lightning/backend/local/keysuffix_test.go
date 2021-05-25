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

	. "github.com/pingcap/check"
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
		chunkIndex int32
		offset     int64
	}{
		{
			[]byte{0x0},
			0,
			0,
		},
		{
			randBytes(32),
			1,
			-2034,
		},
		{
			randBytes(32),
			1,
			math.MaxInt64,
		},
		{
			randBytes(32),
			math.MaxInt32,
			math.MinInt64,
		},
		{
			randBytes(32),
			math.MinInt32,
			2345678,
		},
	}

	for _, input := range inputs {
		result := EncodeKeySuffix(nil, input.key, input.chunkIndex, input.offset)

		// Decode the result.
		key, chunkIndex, offset, err := DecodeKeySuffix(nil, result)
		c.Assert(err, IsNil)
		c.Assert(key, BytesEquals, input.key)
		c.Assert(chunkIndex, Equals, input.chunkIndex)
		c.Assert(offset, Equals, input.offset)
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
		encodedKeys = append(encodedKeys, EncodeKeySuffix(nil, key, 1, int64(i*1234)))
	}
	sorted := sort.SliceIsSorted(encodedKeys, func(i, j int) bool {
		return bytes.Compare(encodedKeys[i], encodedKeys[j]) < 0
	})
	c.Assert(sorted, IsTrue)
}

func (s *keySuffixSuite) TestEncodeKeySuffixWithBuf(c *C) {
	key := randBytes(32)
	buf := make([]byte, 256)
	buf2 := EncodeKeySuffix(buf, key, 1, 1234)
	// Verify the encode result first.
	key2, _, _, err := DecodeKeySuffix(nil, buf2)
	c.Assert(err, IsNil)
	c.Assert(key2, BytesEquals, key)
	// There should be no new slice allocated.
	// If we change a byte in `buf`, `buf2` can read the new byte.
	c.Assert(buf[:len(buf2)], BytesEquals, buf2)
	buf[0]++
	c.Assert(buf[0], Equals, buf2[0])
}

func (s *keySuffixSuite) TestDecodeKeySuffixWithBuf(c *C) {
	data := []byte{
		0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf7,
		0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc,
	}
	buf := make([]byte, len(data))
	key, _, _, err := DecodeKeySuffix(buf, data)
	c.Assert(err, IsNil)
	// There should be no new slice allocated.
	// If we change a byte in `buf`, `buf2` can read the new byte.
	c.Assert(buf, BytesEquals, key[:len(buf)])
	buf[0]++
	c.Assert(buf[0], Equals, key[0])
}
