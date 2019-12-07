package storage_test

import (
	"testing"

	. "github.com/pingcap/check"

	"github.com/pingcap/br/pkg/storage"
)

func Test(t *testing.T) {
	TestingT(t)
}

type testStorageSuite struct{}

var _ = Suite(&testStorageSuite{})

func (r *testStorageSuite) TestCreateStorage(c *C) {
	_, err := storage.ParseBackend("1invalid:")
	c.Assert(err, ErrorMatches, "parse 1invalid:: first path segment in URL cannot contain colon")

	_, err = storage.ParseBackend("net:storage")
	c.Assert(err, ErrorMatches, "storage net not support yet")

	s, err := storage.ParseBackend("local:///tmp/storage")
	c.Assert(err, IsNil)
	c.Assert(s.GetLocal().GetPath(), Equals, "/tmp/storage")

	s, err = storage.ParseBackend("file:///tmp/storage")
	c.Assert(err, IsNil)
	c.Assert(s.GetLocal().GetPath(), Equals, "/tmp/storage")

	s, err = storage.ParseBackend("noop://")
	c.Assert(err, IsNil)
	c.Assert(s.GetNoop(), NotNil)

	s, err = storage.ParseBackend("s3://bucket/more/prefix/")
	c.Assert(err, IsNil)
	s3 := s.GetS3()
	c.Assert(s3, NotNil)
	c.Assert(s3.Bucket, Equals, "bucket")
	c.Assert(s3.Prefix, Equals, "/more/prefix/")
}
