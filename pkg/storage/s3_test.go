package storage

import (
	"io/ioutil"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/pflag"
)

func (r *testStorageSuite) TestApply(c *C) {
	type testcase struct {
		name      string
		options   S3BackendOptions
		errMsg    string
		errReturn bool
	}
	testFn := func(test *testcase, c *C) {
		c.Log(test.name)
		_, err := ParseBackend("s3://bucket2/prefix/", &BackendOptions{S3: test.options})
		if test.errReturn {
			c.Assert(err, ErrorMatches, test.errMsg)
		} else {
			c.Assert(err, IsNil)
		}
	}
	tests := []testcase{
		{
			name: "access_key not found",
			options: S3BackendOptions{
				Region:          "us-west-2",
				SecretAccessKey: "cd",
			},
			errMsg:    "access_key not found",
			errReturn: true,
		},
		{
			name: "secret_access_key not found",
			options: S3BackendOptions{
				Region:    "us-west-2",
				AccessKey: "ab",
			},
			errMsg:    "secret_access_key not found",
			errReturn: true,
		},
		{
			name: "scheme not found",
			options: S3BackendOptions{
				Endpoint: "12345",
			},
			errMsg:    "scheme not found in endpoint",
			errReturn: true,
		},
		{
			name: "host not found",
			options: S3BackendOptions{
				Endpoint: "http:12345",
			},
			errMsg:    "host not found in endpoint",
			errReturn: true,
		},
		{
			name: "invalid endpoint",
			options: S3BackendOptions{
				Endpoint: "!http:12345",
			},
			errMsg:    "parse !http:12345: first path segment in URL cannot contain colon",
			errReturn: true,
		},
	}
	for i := range tests {
		testFn(&tests[i], c)
	}
}
func (r *testStorageSuite) TestApplyUpdate(c *C) {
	type testcase struct {
		name    string
		options S3BackendOptions
		setEnv  bool
		s3      *backup.S3
	}
	testFn := func(test *testcase, c *C) {
		c.Log(test.name)
		if test.setEnv {
			os.Setenv("AWS_ACCESS_KEY_ID", "ab")
			os.Setenv("AWS_SECRET_ACCESS_KEY", "cd")
		}
		u, err := ParseBackend("s3://bucket/prefix/", &BackendOptions{S3: test.options})
		s3 := u.GetS3()
		c.Assert(err, IsNil)
		c.Assert(true, Equals, equalS3(s3, test.s3))
	}
	tests := []testcase{
		{
			name: "no region and no endpoint",
			options: S3BackendOptions{
				Region:   "",
				Endpoint: "",
			},
			s3: &backup.S3{
				Region: "us-east-1",
				Bucket: "bucket",
				Prefix: "prefix",
			},
		},
		{
			name: "no endpoint",
			options: S3BackendOptions{
				Region: "us-west-2",
			},
			s3: &backup.S3{
				Region: "us-west-2",
				Bucket: "bucket",
				Prefix: "prefix",
			},
		},
		{
			name: "https endpoint",
			options: S3BackendOptions{
				Endpoint: "https://s3.us-west-2",
			},
			s3: &backup.S3{
				Region:   "us-east-1",
				Endpoint: "https://s3.us-west-2",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
		},
		{
			name: "http endpoint",
			options: S3BackendOptions{
				Endpoint: "http://s3.us-west-2",
			},
			s3: &backup.S3{
				Region:   "us-east-1",
				Endpoint: "http://s3.us-west-2",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
		},
		{
			name: "ceph provider",
			options: S3BackendOptions{
				Region:         "us-west-2",
				ForcePathStyle: true,
				Provider:       "ceph",
			},
			s3: &backup.S3{
				Region:         "us-west-2",
				ForcePathStyle: true,
				Bucket:         "bucket",
				Prefix:         "prefix",
			},
		},
		{
			name: "ali provider",
			options: S3BackendOptions{
				Region:         "us-west-2",
				ForcePathStyle: true,
				Provider:       "alibaba",
			},
			s3: &backup.S3{
				Region:         "us-west-2",
				ForcePathStyle: false,
				Bucket:         "bucket",
				Prefix:         "prefix",
			},
		},
		{
			name: "netease provider",
			options: S3BackendOptions{
				Region:         "us-west-2",
				ForcePathStyle: true,
				Provider:       "netease",
			},
			s3: &backup.S3{
				Region:         "us-west-2",
				ForcePathStyle: false,
				Bucket:         "bucket",
				Prefix:         "prefix",
			},
		},
		{
			name: "useAccelerateEndpoint",
			options: S3BackendOptions{
				Region:                "us-west-2",
				ForcePathStyle:        true,
				UseAccelerateEndpoint: true,
			},
			s3: &backup.S3{
				Region:         "us-west-2",
				ForcePathStyle: false,
				Bucket:         "bucket",
				Prefix:         "prefix",
			},
		},
		{
			name: "keys",
			options: S3BackendOptions{
				Region:          "us-west-2",
				AccessKey:       "ab",
				SecretAccessKey: "cd",
			},
			s3: &backup.S3{
				Region:          "us-west-2",
				AccessKey:       "ab",
				SecretAccessKey: "cd",
				Bucket:          "bucket",
				Prefix:          "prefix",
			},
			setEnv: true,
		},
	}
	for i := range tests {
		testFn(&tests[i], c)
	}
}

func equalS3(s1, s2 *backup.S3) bool {
	return s1.Endpoint == s2.Endpoint &&
		s1.Region == s2.Region &&
		s1.Bucket == s2.Bucket &&
		s1.Prefix == s2.Prefix &&
		s1.StorageClass == s2.StorageClass &&
		s1.Sse == s2.Sse &&
		s1.Acl == s2.Acl &&
		s1.AccessKey == s2.AccessKey &&
		s1.SecretAccessKey == s2.SecretAccessKey &&
		s1.ForcePathStyle == s2.ForcePathStyle
}

func (r *testStorageSuite) TestS3Storage(c *C) {
	type testcase struct {
		name      string
		s3        *backup.S3
		errReturn bool
		hackCheck bool
	}
	testFn := func(test *testcase, c *C) {
		c.Log(test.name)
		if test.hackCheck {
			checkS3Bucket = func(svc *s3.S3, bucket string) error { return nil }
		}
		s3 := &backup.StorageBackend{
			Backend: &backup.StorageBackend_S3{
				S3: test.s3,
			},
		}
		_, err := Create(s3)
		if test.errReturn {
			c.Assert(err, NotNil)
			return
		}
		c.Assert(err, IsNil)
	}
	tests := []testcase{
		{
			name: "no region and endpoint",
			s3: &backup.S3{
				Region:   "",
				Endpoint: "",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
			errReturn: true,
		},
		{
			name: "no region",
			s3: &backup.S3{
				Region:   "",
				Endpoint: "http://10.1.2.3",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
			errReturn: true,
		},
		{
			name: "no endpoint",
			s3: &backup.S3{
				Region:   "us-west-2",
				Endpoint: "",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
			errReturn: true,
		},
		{
			name: "no region",
			s3: &backup.S3{
				Region:   "",
				Endpoint: "http://10.1.2.3",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
			errReturn: false,
			hackCheck: true,
		},
		{
			name: "normal region",
			s3: &backup.S3{
				Region:   "us-west-2",
				Endpoint: "",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
			errReturn: false,
			hackCheck: true,
		},
	}
	for i := range tests {
		testFn(&tests[i], c)
	}
}
func (r *testStorageSuite) TestS3Handlers(c *C) {
	type testcase struct {
		name    string
		mh      *mockS3Handler
		options *backup.S3
	}

	testFn := func(test *testcase, c *C) {
		c.Log(test.name)
		ms3 := S3Storage{
			svc:     test.mh,
			options: test.options,
		}
		err := ms3.Write("file", []byte("test"))
		c.Assert(err, Equals, test.mh.err)
		_, err = ms3.Read("file")
		c.Assert(err, Equals, test.mh.err)
		_, err = ms3.FileExists("file")
		if err != nil {
			c.Assert(err, Equals, test.mh.err)
		}
	}
	tests := []testcase{
		{
			name: "no error",
			mh: &mockS3Handler{
				err: nil,
			},
			options: &backup.S3{
				Region:       "us-west-2",
				Bucket:       "bucket",
				Prefix:       "prefix",
				Acl:          "acl",
				Sse:          "sse",
				StorageClass: "sc",
			},
		},
		{
			name: "error",
			mh: &mockS3Handler{
				err: errors.New("write error"),
			},
			options: &backup.S3{
				Region: "us-west-2",
				Bucket: "bucket",
				Prefix: "prefix",
			},
		},
		{
			name: "aws not found error",
			mh: &mockS3Handler{
				err: awserr.New(notFound, notFound, errors.New("not found")),
			},
			options: &backup.S3{
				Region: "us-west-2",
				Bucket: "bucket",
				Prefix: "prefix",
			},
		},
		{
			name: "aws other error",
			mh: &mockS3Handler{
				err: awserr.New("other", "other", errors.New("other")),
			},
			options: &backup.S3{
				Region: "us-west-2",
				Bucket: "bucket",
				Prefix: "prefix",
			},
		},
	}
	for i := range tests {
		testFn(&tests[i], c)
	}
}

func (r *testStorageSuite) TestS3Others(c *C) {
	defineS3Flags(&pflag.FlagSet{})
}

type mockS3Handler struct {
	err error
}

func (c *mockS3Handler) HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return nil, c.err
}
func (c *mockS3Handler) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if c.err != nil {
		return nil, c.err
	}
	return &s3.GetObjectOutput{
		Body: ioutil.NopCloser(strings.NewReader("HappyFace.jpg")),
	}, nil
}
func (c *mockS3Handler) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return nil, c.err
}
func (c *mockS3Handler) HeadBucket(input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	return nil, c.err
}
func (c *mockS3Handler) WaitUntilObjectExists(input *s3.HeadObjectInput) error {
	return c.err
}
