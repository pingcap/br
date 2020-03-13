// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"encoding/hex"

	. "github.com/pingcap/check"
)

type testKeySuite struct{}

var _ = Suite(&testKeySuite{})

func (r *testKeySuite) TestParseKey(c *C) {
	rawKey := "1234"
	parsedKey, err := ParseKey("raw", rawKey)
	c.Assert(err, IsNil)
	c.Assert(parsedKey, BytesEquals, []byte(rawKey))

	escapedKey := "\\a\\x1"
	parsedKey, err = ParseKey("escaped", escapedKey)
	c.Assert(err, IsNil)
	c.Assert(parsedKey, BytesEquals, []byte("\a\x01"))

	hexKey := hex.EncodeToString([]byte("1234"))
	parsedKey, err = ParseKey("hex", hexKey)
	c.Assert(err, IsNil)
	c.Assert(parsedKey, BytesEquals, []byte("1234"))

	_, err = ParseKey("notSupport", rawKey)
	c.Assert(err, ErrorMatches, "*unknown format*")

}
