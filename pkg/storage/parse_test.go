package storage_test

import (
	"io/ioutil"
	"os"
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

	_, err = storage.ParseBackend("s3://bucket/more/prefix/", &storage.BackendOptions{})
	c.Assert(err, ErrorMatches, `must provide either 's3\.region' or 's3\.endpoint'`)

	s3opt := &storage.BackendOptions{
		S3: storage.S3BackendOptions{
			Endpoint: "https://s3.example.com/",
		},
	}
	s, err = storage.ParseBackend("s3://bucket2/prefix/", s3opt)
	c.Assert(err, IsNil)
	s3 := s.GetS3()
	c.Assert(s3, NotNil)
	c.Assert(s3.Bucket, Equals, "bucket2")
	c.Assert(s3.Prefix, Equals, "/prefix/")
	c.Assert(s3.Endpoint, Equals, "https://s3.example.com/")

	fakeCredentialsFile, err := ioutil.TempFile("", "fakeCredentialsFile")
	c.Assert(err, IsNil)
	defer func() {
		fakeCredentialsFile.Close()
		os.Remove(fakeCredentialsFile.Name())
	}()
	gcsOpt := &storage.BackendOptions{
		GCS: storage.GCSBackendOptions{
			Endpoint:        "https://gcs.example.com/",
			CredentialsFile: fakeCredentialsFile.Name(),
		},
	}
	s, err = storage.ParseBackend("gcs://bucket2/prefix/", gcsOpt)
	c.Assert(err, IsNil)
	gcs := s.GetGcs()
	c.Assert(gcs, NotNil)
	c.Assert(gcs.Bucket, Equals, "bucket2")
	c.Assert(gcs.Prefix, Equals, "prefix/")
	c.Assert(gcs.Endpoint, Equals, "https://gcs.example.com/")
	s, err = storage.ParseBackend("gcs://bucket/more/prefix/", gcsOpt)
	c.Assert(err, IsNil)
	gcs = s.GetGcs()
	c.Assert(gcs, NotNil)
	c.Assert(gcs.Bucket, Equals, "bucket")
	c.Assert(gcs.Prefix, Equals, "more/prefix/")
	c.Assert(gcs.Endpoint, Equals, "https://gcs.example.com/")
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

	url = storage.FormatBackendURL(&backup.StorageBackend{
		Backend: &backup.StorageBackend_Gcs{
			Gcs: &backup.GCS{
				Bucket:   "bucket",
				Prefix:   "/some prefix/",
				Endpoint: "https://gcs.example.com/",
			},
		},
	})
	c.Assert(url.String(), Equals, "gcs://bucket/some%20prefix/")

}
