package utils

import (
	"fmt"

	. "github.com/pingcap/check"
)

type testStorageSuite struct{}

var _ = Suite(&testStorageSuite{})

func (r *testStorageSuite) TestCreateStorage(c *C) {
	rawURL := "1invalid:"
	_, err := CreateStorage(rawURL)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "parse 1invalid:: first path segment in URL cannot contain colon")

	rawURL = "net:storage"
	_, err = CreateStorage(rawURL)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "storage net not support yet")

	rawURL = fmt.Sprintf("local://%s/storage", c.MkDir())
	_, err = CreateStorage(rawURL)
	c.Assert(err, IsNil)
}
