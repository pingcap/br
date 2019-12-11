package utils

import (
	"encoding/hex"

	. "github.com/pingcap/check"
	"github.com/spf13/pflag"
)

type testKeySuite struct{}

var _ = Suite(&testKeySuite{})

func (r *testKeySuite) TestParseKey(c *C) {
	flagSet := &pflag.FlagSet{}
	flagSet.String("format", "raw", "")
	rawKey := "1234"
	parsedKey, err := ParseKey(flagSet, rawKey)
	c.Assert(err, IsNil)
	c.Assert(parsedKey, BytesEquals, []byte(rawKey))

	flagSet = &pflag.FlagSet{}
	flagSet.String("format", "escaped", "")
	escapedKey := "\\a\\x1"
	parsedKey, err = ParseKey(flagSet, escapedKey)
	c.Assert(err, IsNil)
	c.Assert(parsedKey, BytesEquals, []byte("\a\x01"))

	flagSet = &pflag.FlagSet{}
	flagSet.String("format", "hex", "")
	hexKey := hex.EncodeToString([]byte("1234"))
	parsedKey, err = ParseKey(flagSet, hexKey)
	c.Assert(err, IsNil)
	c.Assert(parsedKey, BytesEquals, []byte("1234"))

	flagSet = &pflag.FlagSet{}
	flagSet.String("format", "notSupport", "")
	_, err = ParseKey(flagSet, rawKey)
	c.Assert(err, ErrorMatches, "*unknown format*")

}
