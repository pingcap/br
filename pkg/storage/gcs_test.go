// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io/ioutil"
	"os"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	. "github.com/pingcap/check"
	backuppb "github.com/pingcap/kvproto/pkg/backup"
)

func (r *testStorageSuite) TestGCS(c *C) {
	ctx := context.Background()

	opts := fakestorage.Options{
		NoListener: true,
	}
	server, err := fakestorage.NewServerWithOptions(opts)
	c.Assert(err, IsNil)
	bucketName := "testbucket"
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})

	gcs := &backuppb.GCS{
		Bucket:          bucketName,
		Prefix:          "a/b/",
		StorageClass:    "NEARLINE",
		PredefinedAcl:   "private",
		CredentialsBlob: "Fake Credentials",
	}
	stg, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
		SendCredentials: false,
		SkipCheckPath:   false,
		HTTPClient:      server.HTTPClient(),
	})
	c.Assert(err, IsNil)

	err = stg.WriteFile(ctx, "key", []byte("data"))
	c.Assert(err, IsNil)

	rc, err := server.Client().Bucket(bucketName).Object("a/b/key").NewReader(ctx)
	c.Assert(err, IsNil)
	d, err := ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, IsNil)
	c.Assert(d, DeepEquals, []byte("data"))

	d, err = stg.ReadFile(ctx, "key")
	c.Assert(err, IsNil)
	c.Assert(d, DeepEquals, []byte("data"))

	exist, err := stg.FileExists(ctx, "key")
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	exist, err = stg.FileExists(ctx, "key_not_exist")
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)

	c.Assert(stg.URI(), Equals, "gcs://testbucket/a/b/")
}

func (r *testStorageSuite) TestNewGCSStorage(c *C) {
	ctx := context.Background()

	opts := fakestorage.Options{
		NoListener: true,
	}
	server, err1 := fakestorage.NewServerWithOptions(opts)
	c.Assert(err1, IsNil)
	bucketName := "testbucket"
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})

	{
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b/",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "FakeCredentials",
		}
		_, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials: true,
			SkipCheckPath:   false,
			HTTPClient:      server.HTTPClient(),
		})
		c.Assert(err, IsNil)
		c.Assert(gcs.CredentialsBlob, Equals, "FakeCredentials")
	}

	{
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b/",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "FakeCredentials",
		}
		_, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials: false,
			SkipCheckPath:   false,
			HTTPClient:      server.HTTPClient(),
		})
		c.Assert(err, IsNil)
		c.Assert(gcs.CredentialsBlob, Equals, "")
	}

	{
		fakeCredentialsFile, err := ioutil.TempFile("", "fakeCredentialsFile")
		c.Assert(err, IsNil)
		defer func() {
			fakeCredentialsFile.Close()
			os.Remove(fakeCredentialsFile.Name())
		}()
		_, err = fakeCredentialsFile.Write([]byte(`{"type": "service_account"}`))
		c.Assert(err, IsNil)
		err = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", fakeCredentialsFile.Name())
		defer os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
		c.Assert(err, IsNil)

		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b/",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "",
		}
		_, err = newGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials: true,
			SkipCheckPath:   false,
			HTTPClient:      server.HTTPClient(),
		})
		c.Assert(err, IsNil)
		c.Assert(gcs.CredentialsBlob, Equals, `{"type": "service_account"}`)
	}

	{
		fakeCredentialsFile, err := ioutil.TempFile("", "fakeCredentialsFile")
		c.Assert(err, IsNil)
		defer func() {
			fakeCredentialsFile.Close()
			os.Remove(fakeCredentialsFile.Name())
		}()
		_, err = fakeCredentialsFile.Write([]byte(`{"type": "service_account"}`))
		c.Assert(err, IsNil)
		err = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", fakeCredentialsFile.Name())
		defer os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
		c.Assert(err, IsNil)

		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b/",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "",
		}
		s, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials: false,
			SkipCheckPath:   false,
			HTTPClient:      server.HTTPClient(),
		})
		c.Assert(err, IsNil)
		c.Assert(gcs.CredentialsBlob, Equals, "")
		c.Assert(s.objectName("x"), Equals, "a/b/x")
	}

	{
		os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b/",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "",
		}
		_, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials: true,
			SkipCheckPath:   false,
			HTTPClient:      server.HTTPClient(),
		})
		c.Assert(err, NotNil)
	}

	{
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "FakeCredentials",
		}
		s, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials: false,
			SkipCheckPath:   false,
			HTTPClient:      server.HTTPClient(),
		})
		c.Assert(err, IsNil)
		c.Assert(gcs.CredentialsBlob, Equals, "")
		c.Assert(s.objectName("x"), Equals, "a/b/x")
	}
}
