package storage

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
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
	// accessKeyEnv         = "AWS_ACCESS_KEY_ID"
	// secretAccessKeyEnv   = "AWS_SECRET_ACCESS_KEY"
	notFound = "NotFound"
	// number of retries to make of operations
	maxRetries = 3
)

// s3Handlers make it easy to inject test functions
type s3Handlers interface {
	HeadObject(*s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	HeadBucket(*s3.HeadBucketInput) (*s3.HeadBucketOutput, error)
	WaitUntilObjectExists(*s3.HeadObjectInput) error
}

// S3Storage info for s3 storage
type S3Storage struct {
	session *session.Session
	// svc     *s3.S3
	svc     s3Handlers
	options *backup.S3
}

// S3BackendOptions contains options for s3 storage
type S3BackendOptions struct {
	Endpoint              string `json:"endpoint" toml:"endpoint"`
	Region                string `json:"region" toml:"region"`
	StorageClass          string `json:"storage_class" toml:"storage_class"`
	SSE                   string `json:"sse" toml:"sse"`
	ACL                   string `json:"acl" toml:"acl"`
	AccessKey             string `json:"access_key" toml:"access_key"`
	SecretAccessKey       string `json:"secret_access_key" toml:"secret_access_key"`
	Provider              string `json:"provider" toml:"provider"`
	ForcePathStyle        bool   `json:"force_path_style" toml:"force_path_style"`
	UseAccelerateEndpoint bool   `json:"use_accelerate_endpoint" toml:"use_accelerate_endpoint"`
}

func (options *S3BackendOptions) apply(s3 *backup.S3) error {
	if options.Endpoint == "" && options.Region == "" {
		return errors.New("must provide either 's3.region' or 's3.endpoint'")
	}
	if options.Endpoint != "" {
		if !strings.HasPrefix(options.Endpoint, "https://") &&
			!strings.HasPrefix(options.Endpoint, "http://") {
			options.Endpoint = "http://" + options.Endpoint
		}
	}
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
	flags.String(s3EndpointOption, "", "Set the S3 endpoint URL")
	flags.String(s3RegionOption, "", "Set the S3 region")
	flags.String(s3StorageClassOption, "", "Set the S3 storage class")
	flags.String(s3SSEOption, "", "Set the S3 server-side encryption algorithm")
	flags.String(s3ACLOption, "", "Set the S3 canned ACLs")
	flags.String(s3ProviderOption, "", "Set the S3 provider")
}

func getBackendOptionsFromS3Flags(flags *pflag.FlagSet) (options S3BackendOptions, err error) {
	send, err := flags.GetBool(flagSendCredentialOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if send {
		c := credentials.NewEnvCredentials()
		v, cerr := c.Get()
		if cerr != nil {
			return options, errors.Trace(cerr)
		}
		options.AccessKey = v.AccessKeyID
		options.SecretAccessKey = v.SecretAccessKey
	}
	options.Endpoint, err = flags.GetString(s3EndpointOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	options.Region, err = flags.GetString(s3RegionOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	options.SSE, err = flags.GetString(s3SSEOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	options.ACL, err = flags.GetString(s3ACLOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	options.StorageClass, err = flags.GetString(s3StorageClassOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	options.ForcePathStyle = true
	options.Provider, err = flags.GetString(s3ProviderOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	return options, err
}

// newS3Storage initialize a new s3 storage for metadata
func newS3Storage(s3Back *backup.S3) (*S3Storage, error) {
	qs := *s3Back
	v := credentials.Value{
		AccessKeyID:     qs.AccessKey,
		SecretAccessKey: qs.SecretAccessKey,
	}

	// low timeout to ec2 metadata service
	lowTimeoutClient := &http.Client{Timeout: 1 * time.Second}
	def := defaults.Get()
	def.Config.HTTPClient = lowTimeoutClient
	ec2Session, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	// first provider to supply a credential set "wins"
	providers := []credentials.Provider{
		// use static credentials if they're present (checked by provider)
		&credentials.StaticProvider{Value: v},

		// * Access Key ID:     AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY
		// * Secret Access Key: AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY
		&credentials.EnvProvider{},

		// A SharedCredentialsProvider retrieves credentials
		// from the current user's home directory.  It checks
		// AWS_SHARED_CREDENTIALS_FILE and AWS_PROFILE too.
		&credentials.SharedCredentialsProvider{},

		// Pick up IAM role if we're in an ECS task
		defaults.RemoteCredProvider(*def.Config, def.Handlers),

		// Pick up IAM role in case we're on EC2
		&ec2rolecreds.EC2RoleProvider{
			Client: ec2metadata.New(ec2Session, &aws.Config{
				HTTPClient: lowTimeoutClient,
			}),
			ExpiryWindow: 3 * time.Minute,
		},
	}
	cred := credentials.NewChainCredentials(providers)
	if qs.Region == "" && qs.Endpoint == "" {
		qs.Endpoint = "https://s3.amazonaws.com/"
	}
	if qs.Region == "" {
		qs.Region = "us-east-1"
	}
	awsConfig := aws.NewConfig().
		WithMaxRetries(maxRetries).
		WithCredentials(cred).
		WithS3ForcePathStyle(qs.ForcePathStyle).
		WithRegion(qs.Region)
	if qs.Endpoint != "" {
		awsConfig.WithEndpoint(qs.Endpoint)
	}
	// awsConfig.WithLogLevel(aws.LogDebugWithSigning)
	awsSessionOpts := session.Options{
		Config: *awsConfig,
	}
	ses, err := session.NewSessionWithOptions(awsSessionOpts)
	if err != nil {
		return nil, err
	}
	c := s3.New(ses)
	err = checkS3Bucket(c, qs.Bucket)
	if err != nil {
		return nil, errors.Errorf("checkS3Bucket error: %v", err)
	}

	qs.Prefix = strings.Trim(qs.Prefix, "/")
	qs.Prefix += "/"
	return &S3Storage{
		session: ses,
		svc:     c,
		options: &qs,
	}, nil
}

// checkBucket checks if a bucket exists and creates it if not
var checkS3Bucket = func(svc *s3.S3, bucket string) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := svc.HeadBucket(input)
	return err
}

// Write write to s3 storage
func (rs *S3Storage) Write(file string, data []byte) error {
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

	// TODO: PutObjectWithContext
	_, err := rs.svc.PutObject(input)
	if err != nil {
		return err
	}
	hinput := &s3.HeadObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}
	// TODO: WaitUntilObjectExistsWithContext
	err = rs.svc.WaitUntilObjectExists(hinput)
	return err
}

// Read read file from s3
func (rs *S3Storage) Read(file string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}

	// TODO: GetObjectWithContext
	result, err := rs.svc.GetObject(input)
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
func (rs *S3Storage) FileExists(file string) (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}

	// TODO: HeadObjectWithContext
	_, err := rs.svc.HeadObject(input)
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
