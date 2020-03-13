// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"io/ioutil"
	"os"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/backup"
)

func Test(t *testing.T) {
	TestingT(t)
}

type testStorageSuite struct{}

var _ = Suite(&testStorageSuite{})

func (r *testStorageSuite) TestCreateStorage(c *C) {
	_, err := ParseBackend("1invalid:", nil)
	c.Assert(err, ErrorMatches, "parse (.*)1invalid:(.*): first path segment in URL cannot contain colon")

	_, err = ParseBackend("net:storage", nil)
	c.Assert(err, ErrorMatches, "storage net not support yet")

	s, err := ParseBackend("local:///tmp/storage", nil)
	c.Assert(err, IsNil)
	c.Assert(s.GetLocal().GetPath(), Equals, "/tmp/storage")

	s, err = ParseBackend("file:///tmp/storage", nil)
	c.Assert(err, IsNil)
	c.Assert(s.GetLocal().GetPath(), Equals, "/tmp/storage")

	s, err = ParseBackend("noop://", nil)
	c.Assert(err, IsNil)
	c.Assert(s.GetNoop(), NotNil)

	_, err = ParseBackend("s3:///bucket/more/prefix/", &BackendOptions{})
	c.Assert(err, ErrorMatches, `please specify the bucket for s3 in s3:///bucket/more/prefix/`)

	s3opt := &BackendOptions{
		S3: S3BackendOptions{
			Endpoint: "https://s3.example.com/",
		},
	}
	s, err = ParseBackend("s3://bucket2/prefix/", s3opt)
	c.Assert(err, IsNil)
	s3 := s.GetS3()
	c.Assert(s3, NotNil)
	c.Assert(s3.Bucket, Equals, "bucket2")
	c.Assert(s3.Prefix, Equals, "prefix")
	c.Assert(s3.Endpoint, Equals, "https://s3.example.com/")

	gcsOpt := &BackendOptions{
		GCS: GCSBackendOptions{
			Endpoint: "https://gcs.example.com/",
		},
	}
	s, err = ParseBackend("gcs://bucket2/prefix/", gcsOpt)
	c.Assert(err, IsNil)
	gcs := s.GetGcs()
	c.Assert(gcs, NotNil)
	c.Assert(gcs.Bucket, Equals, "bucket2")
	c.Assert(gcs.Prefix, Equals, "prefix/")
	c.Assert(gcs.Endpoint, Equals, "https://gcs.example.com/")
	c.Assert(gcs.CredentialsBlob, Equals, "")

	fakeCredentialsFile, err := ioutil.TempFile("", "fakeCredentialsFile")
	c.Assert(err, IsNil)
	_, err = fakeCredentialsFile.Write([]byte("fakeCredentials"))
	c.Assert(err, IsNil)
	defer func() {
		fakeCredentialsFile.Close()
		os.Remove(fakeCredentialsFile.Name())
	}()
	gcsOpt.GCS.CredentialsFile = fakeCredentialsFile.Name()

	s, err = ParseBackend("gcs://bucket/more/prefix/", gcsOpt)
	c.Assert(err, IsNil)
	gcs = s.GetGcs()
	c.Assert(gcs, NotNil)
	c.Assert(gcs.Bucket, Equals, "bucket")
	c.Assert(gcs.Prefix, Equals, "more/prefix/")
	c.Assert(gcs.Endpoint, Equals, "https://gcs.example.com/")
	c.Assert(gcs.CredentialsBlob, Equals, "fakeCredentials")
}

func (r *testStorageSuite) TestFormatBackendURL(c *C) {
	url := FormatBackendURL(&backup.StorageBackend{
		Backend: &backup.StorageBackend_Local{
			Local: &backup.Local{Path: "/tmp/file"},
		},
	})
	c.Assert(url.String(), Equals, "local:///tmp/file")

	url = FormatBackendURL(&backup.StorageBackend{
		Backend: &backup.StorageBackend_Noop{
			Noop: &backup.Noop{},
		},
	})
	c.Assert(url.String(), Equals, "noop:///")

	url = FormatBackendURL(&backup.StorageBackend{
		Backend: &backup.StorageBackend_S3{
			S3: &backup.S3{
				Bucket:   "bucket",
				Prefix:   "/some prefix/",
				Endpoint: "https://s3.example.com/",
			},
		},
	})
	c.Assert(url.String(), Equals, "s3://bucket/some%20prefix/")

	url = FormatBackendURL(&backup.StorageBackend{
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
