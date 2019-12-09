package storage_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/backup"

	"github.com/pingcap/br/pkg/storage"
)

func Test(t *testing.T) {
	TestingT(t)
}

type testStorageSuite struct{}

var _ = Suite(&testStorageSuite{})

func (r *testStorageSuite) TestCreateStorage(c *C) {
	_, err := storage.ParseBackend("1invalid:", nil)
	c.Assert(err, ErrorMatches, "parse 1invalid:: first path segment in URL cannot contain colon")

	_, err = storage.ParseBackend("net:storage", nil)
	c.Assert(err, ErrorMatches, "storage net not support yet")

	s, err := storage.ParseBackend("local:///tmp/storage", nil)
	c.Assert(err, IsNil)
	c.Assert(s.GetLocal().GetPath(), Equals, "/tmp/storage")

	s, err = storage.ParseBackend("file:///tmp/storage", nil)
	c.Assert(err, IsNil)
	c.Assert(s.GetLocal().GetPath(), Equals, "/tmp/storage")

	s, err = storage.ParseBackend("noop://", nil)
	c.Assert(err, IsNil)
	c.Assert(s.GetNoop(), NotNil)

	s, err = storage.ParseBackend("s3://bucket/more/prefix/", nil)
	c.Assert(err, IsNil)
	s3 := s.GetS3()
	c.Assert(s3, NotNil)
	c.Assert(s3.Bucket, Equals, "bucket")
	c.Assert(s3.Prefix, Equals, "/more/prefix/")

	s3opt := &storage.BackendOptions{
		S3: storage.S3BackendOptions{
			Endpoint: "https://s3.example.com/",
		},
	}
	s, err = storage.ParseBackend("s3://bucket2/prefix/", s3opt)
	c.Assert(err, IsNil)
	s3 = s.GetS3()
	c.Assert(s3, NotNil)
	c.Assert(s3.Bucket, Equals, "bucket2")
	c.Assert(s3.Prefix, Equals, "/prefix/")
	c.Assert(s3.Endpoint, Equals, "https://s3.example.com/")

	_, err = storage.ParseBackend("noop://foo", s3opt)
	c.Assert(err, ErrorMatches, "options 's3.*' are not applicable to noop storage")
}

func (r *testStorageSuite) TestFormatBackendURL(c *C) {
	url := storage.FormatBackendURL(&backup.StorageBackend{
		Backend: &backup.StorageBackend_Local{
			Local: &backup.Local{Path: "/tmp/file"},
		},
	})
	c.Assert(url.String(), Equals, "local:///tmp/file")

	url = storage.FormatBackendURL(&backup.StorageBackend{
		Backend: &backup.StorageBackend_Noop{
			Noop: &backup.Noop{},
		},
	})
	c.Assert(url.String(), Equals, "noop:///")

	url = storage.FormatBackendURL(&backup.StorageBackend{
		Backend: &backup.StorageBackend_S3{
			S3: &backup.S3{
				Bucket:   "bucket",
				Prefix:   "/some prefix/",
				Endpoint: "https://s3.example.com/",
			},
		},
	})
	c.Assert(url.String(), Equals, "s3://bucket/some%20prefix/")

}
