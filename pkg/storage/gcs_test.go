package storage

import (
	"context"
	"io/ioutil"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/backup"
)

type testSuite struct{}

var _ = Suite(&testSuite{})

func (r *testSuite) TestGCS(c *C) {
	opts := fakestorage.Options{
		NoListener: true,
	}
	server, err := fakestorage.NewServerWithOptions(opts)
	c.Assert(err, IsNil)
	bucketName := "testbucket"
	server.CreateBucket(bucketName)

	stg := &gcsStorage{
		gcs: &backup.GCS{
			Prefix:        "a/b/",
			StorageClass:  "NEARLINE",
			PredefinedAcl: "private",
		},
		bucket: server.Client().Bucket(bucketName),
	}
	ctx := context.Background()
	err = stg.Write(ctx, "key", []byte("data"))
	c.Assert(err, IsNil)

	rc, err := server.Client().Bucket(bucketName).Object("a/b/key").NewReader(ctx)
	c.Assert(err, IsNil)
	d, err := ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, IsNil)
	c.Assert(d, DeepEquals, []byte("data"))

	d, err = stg.Read(ctx, "key")
	c.Assert(err, IsNil)
	c.Assert(d, DeepEquals, []byte("data"))

	exist, err := stg.FileExists(ctx, "key")
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	exist, err = stg.FileExists(ctx, "key_not_exist")
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)
}
