// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
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
	c.Assert(err, ErrorMatches, "storage net not support yet.*")

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
	c.Assert(err, ErrorMatches, `please specify the bucket for s3 in s3:///bucket/more/prefix/.*`)

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

	// nolint:lll
	s, err = ParseBackend(`s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=1&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc`, nil)
	c.Assert(err, IsNil)
	s3 = s.GetS3()
	c.Assert(s3, NotNil)
	c.Assert(s3.Bucket, Equals, "bucket3")
	c.Assert(s3.Prefix, Equals, "prefix/path")
	c.Assert(s3.Endpoint, Equals, "https://127.0.0.1:9000")
	c.Assert(s3.ForcePathStyle, IsTrue)
	c.Assert(s3.Sse, Equals, "aws:kms")
	c.Assert(s3.SseKmsKeyId, Equals, "TestKey")

	// special character in access keys
	s, err = ParseBackend(`s3://bucket4/prefix/path?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw`, nil)
	c.Assert(err, IsNil)
	s3 = s.GetS3()
	c.Assert(s3, NotNil)
	c.Assert(s3.Bucket, Equals, "bucket4")
	c.Assert(s3.Prefix, Equals, "prefix/path")
	c.Assert(s3.AccessKey, Equals, "NXN7IPIOSAAKDEEOLMAF")
	c.Assert(s3.SecretAccessKey, Equals, "nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw")

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

	var credFeilPerm os.FileMode = 0o600
	fakeCredentialsFile := filepath.Join(c.MkDir(), "fakeCredentialsFile")
	err = ioutil.WriteFile(fakeCredentialsFile, []byte("fakeCredentials"), credFeilPerm)
	c.Assert(err, IsNil)

	gcsOpt.GCS.CredentialsFile = fakeCredentialsFile

	s, err = ParseBackend("gcs://bucket/more/prefix/", gcsOpt)
	c.Assert(err, IsNil)
	gcs = s.GetGcs()
	c.Assert(gcs, NotNil)
	c.Assert(gcs.Bucket, Equals, "bucket")
	c.Assert(gcs.Prefix, Equals, "more/prefix/")
	c.Assert(gcs.Endpoint, Equals, "https://gcs.example.com/")
	c.Assert(gcs.CredentialsBlob, Equals, "fakeCredentials")

	err = ioutil.WriteFile(fakeCredentialsFile, []byte("fakeCreds2"), credFeilPerm)
	c.Assert(err, IsNil)
	s, err = ParseBackend("gs://bucket4/backup/?credentials-file="+url.QueryEscape(fakeCredentialsFile), nil)
	c.Assert(err, IsNil)
	gcs = s.GetGcs()
	c.Assert(gcs, NotNil)
	c.Assert(gcs.Bucket, Equals, "bucket4")
	c.Assert(gcs.Prefix, Equals, "backup/")
	c.Assert(gcs.CredentialsBlob, Equals, "fakeCreds2")

	s, err = ParseBackend("/test", nil)
	c.Assert(err, IsNil)
	local := s.GetLocal()
	c.Assert(local, NotNil)
	c.Assert(local.GetPath(), Equals, "/test")
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
