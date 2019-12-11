package storage

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/pflag"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"

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
	sendCredentialOption = "send-credentials-to-tikv"
	accessKeyEnv         = "AWS_ACCESS_KEY_ID"
	secretAccessKeyEnv   = "AWS_SECRET_ACCESS_KEY"
	// number of retries to make of operations
	maxRetries = 3
)

// S3BackendOptions contains options for s3 storage
type S3BackendOptions struct {
	Endpoint        string `json:"endpoint" toml:"endpoint"`
	Region          string `json:"region" toml:"region"`
	StorageClass    string `json:"storage_class" toml:"storage_class"`
	SSE             string `json:"sse" toml:"sse"`
	ACL             string `json:"acl" toml:"acl"`
	ForcePathStyle  bool   `json:"force_path_style" toml:"force_path_style"`
	AccessKey       string `json:"access_key" toml:"access_key"`
	SecretAccessKey string `json:"secret_access_key" toml:"secret_access_key"`
}

func (options *S3BackendOptions) apply(s3 *backup.S3) error {
	if options.Endpoint == "" && options.Region == "" {
		return errors.New("must provide either 's3.region' or 's3.endpoint'")
	}
	if options.AccessKey == "" && options.SecretAccessKey != "" {
		return errors.New("secret_access_key not found")
	}
	if options.AccessKey != "" && options.SecretAccessKey == "" {
		return errors.New("access_key not found")
	}

	// StorageClass, SSE and ACL are acceptable to be empty
	s3.Endpoint = options.Endpoint
	s3.Region = options.Region
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
		options.AccessKey = os.Getenv(accessKeyEnv)
		options.SecretAccessKey = os.Getenv(secretAccessKeyEnv)
	}
	options.Endpoint, err = flags.GetString(s3EndpointOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if options.Endpoint != "" {
		if !strings.HasPrefix(options.Endpoint, "https://") &&
			!strings.HasPrefix(options.Endpoint, "http://") {
			options.Endpoint = "http://" + options.Endpoint
		}
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
	provider, err := flags.GetString(s3ProviderOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	// TODO: ForcePathStyle may need to be false
	// if UseAccelerateEndpoint enabled for aws s3
	if provider == "alibaba" || provider == "netease" {
		options.ForcePathStyle = false
	}

	return
}

// newS3Storage initialize a new s3 storage for metadata
func newS3Storage(s3Back *backup.S3) (*RemoteStorage, error) {
	qs := *s3Back
	v := credentials.Value{
		AccessKeyID:     qs.AccessKey,
		SecretAccessKey: qs.SecretAccessKey,
	}

	// low timeout to ec2 metadata service
	lowTimeoutClient := &http.Client{Timeout: 1 * time.Second}
	def := defaults.Get()
	def.Config.HTTPClient = lowTimeoutClient

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
			Client: ec2metadata.New(session.New(), &aws.Config{
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
		WithS3ForcePathStyle(qs.ForcePathStyle)
	if qs.Region != "" {
		awsConfig.WithRegion(qs.Region)
	}
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
	// Create a *blob.Bucket.
	bkt, err := s3blob.OpenBucket(context.Background(), ses, qs.Bucket, nil)
	if err != nil {
		return nil, err
	}

	qs.Prefix = strings.Trim(qs.Prefix, "/")
	qs.Prefix += "/"
	return &RemoteStorage{
		bucket: blob.PrefixedBucket(bkt, qs.Prefix),
	}, nil
}

// checkBucket checks if a bucket exists and creates it if not
func checkS3Bucket(svc *s3.S3, bucket string) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := svc.HeadBucket(input)
	return err
}
