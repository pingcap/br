// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	s3SseOption          = "s3.sse"
	s3SseKmsKeyIDOption  = "s3.sse-kms-key-id"
	s3ACLOption          = "s3.acl"
	s3ProviderOption     = "s3.provider"
	notFound             = "NotFound"
	// number of retries to make of operations
	maxRetries = 3

	// the maximum number of byte to read for seek
	maxSkipOffsetByRead = 1 << 16 //64KB
)

// s3Handlers make it easy to inject test functions.
type s3Handlers interface {
	HeadObjectWithContext(context.Context, *s3.HeadObjectInput, ...request.Option) (*s3.HeadObjectOutput, error)
	GetObjectWithContext(context.Context, *s3.GetObjectInput, ...request.Option) (*s3.GetObjectOutput, error)
	PutObjectWithContext(context.Context, *s3.PutObjectInput, ...request.Option) (*s3.PutObjectOutput, error)
	ListObjectsWithContext(context.Context, *s3.ListObjectsInput, ...request.Option) (*s3.ListObjectsOutput, error)
	HeadBucketWithContext(context.Context, *s3.HeadBucketInput, ...request.Option) (*s3.HeadBucketOutput, error)
	WaitUntilObjectExistsWithContext(context.Context, *s3.HeadObjectInput, ...request.WaiterOption) error

	ListObjectsV2WithContext(context.Context, *s3.ListObjectsV2Input, ...request.Option) (*s3.ListObjectsV2Output, error)
	CreateMultipartUploadWithContext(
		context.Context,
		*s3.CreateMultipartUploadInput,
		...request.Option) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUploadWithContext(
		context.Context,
		*s3.CompleteMultipartUploadInput,
		...request.Option) (*s3.CompleteMultipartUploadOutput, error)
	UploadPartWithContext(context.Context, *s3.UploadPartInput, ...request.Option) (*s3.UploadPartOutput, error)
}

// S3Storage info for s3 storage.
type S3Storage struct {
	session *session.Session
	svc     s3Handlers
	options *backup.S3
}

// S3BackendOptions contains options for s3 storage.
type S3BackendOptions struct {
	Endpoint              string `json:"endpoint" toml:"endpoint"`
	Region                string `json:"region" toml:"region"`
	StorageClass          string `json:"storage-class" toml:"storage-class"`
	Sse                   string `json:"sse" toml:"sse"`
	SseKmsKeyID           string `json:"sse-kms-key-id" toml:"sse-kms-key-id"`
	ACL                   string `json:"acl" toml:"acl"`
	AccessKey             string `json:"access-key" toml:"access-key"`
	SecretAccessKey       string `json:"secret-access-key" toml:"secret-access-key"`
	Provider              string `json:"provider" toml:"provider"`
	ForcePathStyle        bool   `json:"force-path-style" toml:"force-path-style"`
	UseAccelerateEndpoint bool   `json:"use-accelerate-endpoint" toml:"use-accelerate-endpoint"`
}

func (options *S3BackendOptions) Apply(s3 *backup.S3) error {
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
	s3.Sse = options.Sse
	s3.SseKmsKeyId = options.SseKmsKeyID
	s3.Acl = options.ACL
	s3.AccessKey = options.AccessKey
	s3.SecretAccessKey = options.SecretAccessKey
	s3.ForcePathStyle = options.ForcePathStyle
	return nil
}

// defineS3Flags defines the command line flags for S3BackendOptions.
func defineS3Flags(flags *pflag.FlagSet) {
	// TODO: remove experimental tag if it's stable
	flags.String(s3EndpointOption, "",
		"(experimental) Set the S3 endpoint URL, please specify the http or https scheme explicitly")
	flags.String(s3RegionOption, "", "(experimental) Set the S3 region, e.g. us-east-1")
	flags.String(s3StorageClassOption, "", "(experimental) Set the S3 storage class, e.g. STANDARD")
	flags.String(s3SseOption, "", "Set S3 server-side encryption, e.g. aws:kms")
	flags.String(s3SseKmsKeyIDOption, "", "KMS CMK key id to use with S3 server-side encryption."+
		"Leave empty to use S3 owned key.")
	flags.String(s3ACLOption, "", "(experimental) Set the S3 canned ACLs, e.g. authenticated-read")
	flags.String(s3ProviderOption, "", "(experimental) Set the S3 provider, e.g. aws, alibaba, ceph")
}

// parseFromFlags parse S3BackendOptions from command line flags.
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
	options.Sse, err = flags.GetString(s3SseOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.SseKmsKeyID, err = flags.GetString(s3SseKmsKeyIDOption)
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

// NewS3Storage initialize a new s3 storage for metadata.
func NewS3Storage( // revive:disable-line:flag-parameter
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

// checkBucket checks if a bucket exists.
var checkS3Bucket = func(svc *s3.S3, bucket string) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := svc.HeadBucket(input)
	return err
}

// Write write to s3 storage.
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
	if rs.options.SseKmsKeyId != "" {
		input = input.SetSSEKMSKeyId(rs.options.SseKmsKeyId)
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

// Read read file from s3.
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

// FileExists check if file exists on s3 storage.
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

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The first argument is the file path that can be used in `Open`
// function; the second argument is the size in byte of the file determined
// by path.
func (rs *S3Storage) WalkDir(ctx context.Context, fn func(string, int64) error) error {
	var marker *string
	maxKeys := int64(1000)
	req := &s3.ListObjectsInput{
		Bucket:  &rs.options.Bucket,
		Prefix:  &rs.options.Prefix,
		MaxKeys: &maxKeys,
	}
	for {
		req.Marker = marker
		res, err := rs.svc.ListObjectsWithContext(ctx, req)
		if err != nil {
			return err
		}
		for _, r := range res.Contents {
			if err = fn(*r.Key, *r.Size); err != nil {
				return err
			}
		}
		if res.IsTruncated != nil && *res.IsTruncated {
			marker = res.Marker
		} else {
			break
		}
	}

	return nil
}

// Open a Reader by file name.
func (rs *S3Storage) Open(ctx context.Context, name string) (ReadSeekCloser, error) {
	reader, err := rs.open(ctx, name, 0, 0)
	if err != nil {
		return nil, err
	}
	return &s3ObjectReader{
		storage: rs,
		name:    name,
		reader:  reader,
	}, nil
}

func (rs *S3Storage) open(ctx context.Context, name string, startOffset int64, endOffset int64) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + name),
	}

	var rangeOffset *string
	if startOffset > 0 {
		if endOffset > startOffset {
			rangeOffset = aws.String(fmt.Sprintf("bytes=%d-%d", startOffset, endOffset))
		} else {
			rangeOffset = aws.String(fmt.Sprintf("bytes=%d-", startOffset))
		}
		input.Range = rangeOffset
	}

	result, err := rs.svc.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, err
	}

	// FIXME: we test in minio, when request with Range, the result.AcceptRanges is a bare string 'range',
	//  not sure whether this is a feature or bug
	//if rangeOffset != nil && (result.AcceptRanges == nil || *result.AcceptRanges != *rangeOffset) {
	//	return nil, errors.Errorf("open file '%s' failed, expected range: %s, got: %v",
	//		name, *rangeOffset, result.AcceptRanges)
	//}

	return result.Body, nil
}

// s3ObjectReader wrap GetObjectOutput.Body and add the `Seek` method.
type s3ObjectReader struct {
	storage *S3Storage
	name    string
	reader  io.ReadCloser
	pos     int64
}

// Read implement the io.Reader interface.
func (r *s3ObjectReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.pos += int64(n)
	return
}

// Close implement the io.Closer interface.
func (r *s3ObjectReader) Close() error {
	return r.reader.Close()
}

// Seek implement the io.Seeker interface.
func (r *s3ObjectReader) Seek(offset int64, whence int) (int64, error) {
	var realOffset int64
	switch whence {
	case io.SeekStart:
		realOffset = offset
	case io.SeekCurrent:
		realOffset = r.pos + offset
	default:
		// TODO: maybe we can fetch the object stat and calculate the absolute offset
		return 0, errors.New("seek by SeekEnd is not supported yet")
	}

	if realOffset == r.pos {
		return realOffset, nil
	}

	// if seek ahead no more than 64k, we discard these data
	if realOffset > r.pos && offset-r.pos <= maxSkipOffsetByRead {
		_, err := io.CopyN(ioutil.Discard, r, offset-r.pos)
		if err != nil {
			return r.pos, err
		}
		return realOffset, nil
	}

	// close current read and open a new one which target offset
	err := r.reader.Close()
	if err != nil {
		return 0, err
	}

	newReader, err := r.storage.open(context.TODO(), r.name, realOffset, 0)
	if err != nil {
		return 0, err
	}
	r.reader = newReader
	r.pos = realOffset
	return realOffset, nil
}

// ListObject list `maxkeys` objects with specify path.
func (rs *S3Storage) ListObject(ctx context.Context, name string, maxKeys int64) (*s3.ListObjectsV2Output, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(rs.options.Bucket),
		Prefix:  aws.String(rs.options.Prefix + name),
		MaxKeys: aws.Int64(maxKeys),
	}
	return rs.svc.ListObjectsV2WithContext(ctx, input)
}

// CreateMultipartUpload create multi upload request.
func (rs *S3Storage) CreateMultipartUpload(ctx context.Context, name string) (*s3.CreateMultipartUploadOutput, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + name),
	}
	return rs.svc.CreateMultipartUploadWithContext(ctx, input)
}

// CompleteMultipartUpload complete multi upload request.
func (rs *S3Storage) CompleteMultipartUpload(
	ctx context.Context,
	resp *s3.CreateMultipartUploadOutput,
	completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return rs.svc.CompleteMultipartUploadWithContext(ctx, completeInput)
}

// UploadPart update partial data to s3, we should call CreateMultipartUpload to start it,
// and call CompleteMultipartUpload to finish it.
func (rs *S3Storage) UploadPart(
	ctx context.Context,
	resp *s3.CreateMultipartUploadOutput,
	fileBytes []byte,
	partNumber int) (*s3.CompletedPart, error) {
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	uploadResult, err := rs.svc.UploadPartWithContext(ctx, partInput)
	if err != nil {
		return nil, err
	}
	return &s3.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: aws.Int64(int64(partNumber)),
	}, nil
}
