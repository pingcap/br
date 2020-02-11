package storage

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/pflag"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
)

const (
	s3EndpointOption     = "s3.endpoint"
	s3RegionOption       = "s3.region"
	s3StorageClassOption = "s3.storage-class"
	s3SSEOption          = "s3.sse"
	s3ACLOption          = "s3.acl"
	s3ProviderOption     = "s3.provider"
	notFound             = "NotFound"
	// number of retries to make of operations
	maxRetries = 3
)

// s3Handlers make it easy to inject test functions
type s3Handlers interface {
	HeadObjectWithContext(context.Context, *s3.HeadObjectInput, ...request.Option) (*s3.HeadObjectOutput, error)
	GetObjectWithContext(context.Context, *s3.GetObjectInput, ...request.Option) (*s3.GetObjectOutput, error)
	PutObjectWithContext(context.Context, *s3.PutObjectInput, ...request.Option) (*s3.PutObjectOutput, error)
	HeadBucketWithContext(context.Context, *s3.HeadBucketInput, ...request.Option) (*s3.HeadBucketOutput, error)
	WaitUntilObjectExistsWithContext(context.Context, *s3.HeadObjectInput, ...request.WaiterOption) error
}

// S3Storage info for s3 storage
type S3Storage struct {
	session *session.Session
	svc     s3Handlers
	options *backup.S3
}

// S3BackendOptions contains options for s3 storage
type S3BackendOptions struct {
	Endpoint              string `json:"endpoint" toml:"endpoint"`
	Region                string `json:"region" toml:"region"`
	StorageClass          string `json:"storage-class" toml:"storage-class"`
	SSE                   string `json:"sse" toml:"sse"`
	ACL                   string `json:"acl" toml:"acl"`
	AccessKey             string `json:"access-key" toml:"access-key"`
	SecretAccessKey       string `json:"secret-access-key" toml:"secret-access-key"`
	Provider              string `json:"provider" toml:"provider"`
	ForcePathStyle        bool   `json:"force-path-style" toml:"force-path-style"`
	UseAccelerateEndpoint bool   `json:"use-accelerate-endpoint" toml:"use-accelerate-endpoint"`
}

func (options *S3BackendOptions) apply(s3 *backup.S3) error {
	if options.Region == "" {
		options.Region = "us-east-1"
	}
	if options.Endpoint != "" {
		u, err := url.Parse(options.Endpoint)
		if err != nil {
			return err
		}
		if u.Scheme == "" {
			return errors.New("scheme not found in endpoint")
		}
		if u.Host == "" {
			return errors.New("host not found in endpoint")
		}
	}
	// In some cases, we need to set ForcePathStyle to false.
	// Refer to: https://rclone.org/s3/#s3-force-path-style
	if options.Provider == "alibaba" || options.Provider == "netease" ||
		options.UseAccelerateEndpoint {
		options.ForcePathStyle = false
	}
	if options.AccessKey == "" && options.SecretAccessKey != "" {
		return errors.New("access_key not found")
	}
	if options.AccessKey != "" && options.SecretAccessKey == "" {
		return errors.New("secret_access_key not found")
	}

	s3.Endpoint = options.Endpoint
	s3.Region = options.Region
	// StorageClass, SSE and ACL are acceptable to be empty
	s3.StorageClass = options.StorageClass
	s3.Sse = options.SSE
	s3.Acl = options.ACL
	s3.AccessKey = options.AccessKey
	s3.SecretAccessKey = options.SecretAccessKey
	s3.ForcePathStyle = options.ForcePathStyle
	return nil
}

func defineS3Flags(flags *pflag.FlagSet) {
	flags.String(s3EndpointOption, "", "Set the S3 endpoint URL, please specify the http or https scheme explicitly")
	flags.String(s3RegionOption, "", "Set the S3 region, e.g. us-east-1")
	flags.String(s3StorageClassOption, "", "Set the S3 storage class, e.g. STANDARD")
	flags.String(s3SSEOption, "", "Set the S3 server-side encryption algorithm, e.g. AES256")
	flags.String(s3ACLOption, "", "Set the S3 canned ACLs, e.g. authenticated-read")
	flags.String(s3ProviderOption, "", "Set the S3 provider, e.g. aws, alibaba, ceph")

	_ = flags.MarkHidden(s3EndpointOption)
	_ = flags.MarkHidden(s3RegionOption)
	_ = flags.MarkHidden(s3StorageClassOption)
	_ = flags.MarkHidden(s3SSEOption)
	_ = flags.MarkHidden(s3ACLOption)
	_ = flags.MarkHidden(s3ProviderOption)
}

func (options *S3BackendOptions) parseFromFlags(flags *pflag.FlagSet) error {
	var err error
	options.Endpoint, err = flags.GetString(s3EndpointOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.Region, err = flags.GetString(s3RegionOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.SSE, err = flags.GetString(s3SSEOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.ACL, err = flags.GetString(s3ACLOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.StorageClass, err = flags.GetString(s3StorageClassOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.ForcePathStyle = true
	options.Provider, err = flags.GetString(s3ProviderOption)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// newS3Storage initialize a new s3 storage for metadata
func newS3Storage( // revive:disable-line:flag-parameter
	backend *backup.S3,
	sendCredential bool,
) (*S3Storage, error) {
	qs := *backend
	awsConfig := aws.NewConfig().
		WithMaxRetries(maxRetries).
		WithS3ForcePathStyle(qs.ForcePathStyle).
		WithRegion(qs.Region)
	if qs.Endpoint != "" {
		awsConfig.WithEndpoint(qs.Endpoint)
	}
	var cred *credentials.Credentials
	if qs.AccessKey != "" && qs.SecretAccessKey != "" {
		cred = credentials.NewStaticCredentials(qs.AccessKey, qs.SecretAccessKey, "")
	}
	if cred != nil {
		awsConfig.WithCredentials(cred)
	}
	// awsConfig.WithLogLevel(aws.LogDebugWithSigning)
	awsSessionOpts := session.Options{
		Config: *awsConfig,
	}
	ses, err := session.NewSessionWithOptions(awsSessionOpts)
	if err != nil {
		return nil, err
	}

	if !sendCredential {
		// Clear the credentials if exists so that they will not be sent to TiKV
		backend.AccessKey = ""
		backend.SecretAccessKey = ""
	} else if ses.Config.Credentials != nil {
		if qs.AccessKey == "" || qs.SecretAccessKey == "" {
			v, cerr := ses.Config.Credentials.Get()
			if cerr != nil {
				return nil, cerr
			}
			backend.AccessKey = v.AccessKeyID
			backend.SecretAccessKey = v.SecretAccessKey
		}
	}

	c := s3.New(ses)
	err = checkS3Bucket(c, qs.Bucket)
	if err != nil {
		return nil, errors.Errorf("Bucket %s is not accessible: %v", qs.Bucket, err)
	}

	qs.Prefix += "/"
	return &S3Storage{
		session: ses,
		svc:     c,
		options: &qs,
	}, nil
}

// checkBucket checks if a bucket exists
var checkS3Bucket = func(svc *s3.S3, bucket string) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := svc.HeadBucket(input)
	return err
}

// Write write to s3 storage
func (rs *S3Storage) Write(ctx context.Context, file string, data []byte) error {
	input := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(bytes.NewReader(data)),
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}
	if rs.options.Acl != "" {
		input = input.SetACL(rs.options.Acl)
	}
	if rs.options.Sse != "" {
		input = input.SetServerSideEncryption(rs.options.Sse)
	}
	if rs.options.StorageClass != "" {
		input = input.SetStorageClass(rs.options.StorageClass)
	}

	_, err := rs.svc.PutObjectWithContext(ctx, input)
	if err != nil {
		return err
	}
	hinput := &s3.HeadObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}
	err = rs.svc.WaitUntilObjectExistsWithContext(ctx, hinput)
	return err
}

// Read read file from s3
func (rs *S3Storage) Read(ctx context.Context, file string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}

	result, err := rs.svc.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()
	data, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// FileExists check if file exists on s3 storage
func (rs *S3Storage) FileExists(ctx context.Context, file string) (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}

	_, err := rs.svc.HeadObjectWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket, s3.ErrCodeNoSuchKey, notFound:
				return false, nil
			default:
				return true, err
			}
		}
	}

	return true, err
}
