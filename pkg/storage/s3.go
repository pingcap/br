// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	berrors "github.com/pingcap/br/pkg/errors"
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
	// number of retries to make of operations.
	maxRetries = 6
	// max number of retries when meets error
	maxErrorRetries = 3

	// the maximum number of byte to read for seek.
	maxSkipOffsetByRead = 1 << 16 // 64KB

	// TODO make this configurable, 5 mb is a good minimum size but on low latency/high bandwidth network you can go a lot bigger
	hardcodedS3ChunkSize = 5 * 1024 * 1024
)

// S3Storage info for s3 storage.
type S3Storage struct {
	session *session.Session
	svc     s3iface.S3API
	options *backup.S3
}

// S3Uploader does multi-part upload to s3.
type S3Uploader struct {
	svc           s3iface.S3API
	createOutput  *s3.CreateMultipartUploadOutput
	completeParts []*s3.CompletedPart
}

// UploadPart update partial data to s3, we should call CreateMultipartUpload to start it,
// and call CompleteMultipartUpload to finish it.
func (u *S3Uploader) Write(ctx context.Context, data []byte) (int, error) {
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(data),
		Bucket:        u.createOutput.Bucket,
		Key:           u.createOutput.Key,
		PartNumber:    aws.Int64(int64(len(u.completeParts) + 1)),
		UploadId:      u.createOutput.UploadId,
		ContentLength: aws.Int64(int64(len(data))),
	}

	uploadResult, err := u.svc.UploadPartWithContext(ctx, partInput)
	if err != nil {
		return 0, errors.Trace(err)
	}
	u.completeParts = append(u.completeParts, &s3.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: partInput.PartNumber,
	})
	return len(data), nil
}

// Close complete multi upload request.
func (u *S3Uploader) Close(ctx context.Context) error {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   u.createOutput.Bucket,
		Key:      u.createOutput.Key,
		UploadId: u.createOutput.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: u.completeParts,
		},
	}
	_, err := u.svc.CompleteMultipartUploadWithContext(ctx, completeInput)
	return errors.Trace(err)
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

// Apply apply s3 options on backup.S3.
func (options *S3BackendOptions) Apply(s3 *backup.S3) error {
	if options.Region == "" {
		options.Region = "us-east-1"
	}
	if options.Endpoint != "" {
		u, err := url.Parse(options.Endpoint)
		if err != nil {
			return errors.Trace(err)
		}
		if u.Scheme == "" {
			return errors.Annotate(berrors.ErrStorageInvalidConfig, "scheme not found in endpoint")
		}
		if u.Host == "" {
			return errors.Annotate(berrors.ErrStorageInvalidConfig, "host not found in endpoint")
		}
	}
	// In some cases, we need to set ForcePathStyle to false.
	// Refer to: https://rclone.org/s3/#s3-force-path-style
	if options.Provider == "alibaba" || options.Provider == "netease" ||
		options.UseAccelerateEndpoint {
		options.ForcePathStyle = false
	}
	if options.AccessKey == "" && options.SecretAccessKey != "" {
		return errors.Annotate(berrors.ErrStorageInvalidConfig, "access_key not found")
	}
	if options.AccessKey != "" && options.SecretAccessKey == "" {
		return errors.Annotate(berrors.ErrStorageInvalidConfig, "secret_access_key not found")
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

// NewS3StorageForTest creates a new S3Storage for testing only.
func NewS3StorageForTest(svc s3iface.S3API, options *backup.S3) *S3Storage {
	return &S3Storage{
		session: nil,
		svc:     svc,
		options: options,
	}
}

// NewS3Storage initialize a new s3 storage for metadata.
//
// Deprecated: Create the storage via `New()` instead of using this.
func NewS3Storage( // revive:disable-line:flag-parameter
	backend *backup.S3,
	sendCredential bool,
) (*S3Storage, error) {
	return newS3Storage(backend, &ExternalStorageOptions{
		SendCredentials: sendCredential,
		SkipCheckPath:   false,
	})
}

func newS3Storage(backend *backup.S3, opts *ExternalStorageOptions) (*S3Storage, error) {
	qs := *backend
	awsConfig := aws.NewConfig().
		WithMaxRetries(maxRetries).
		WithS3ForcePathStyle(qs.ForcePathStyle).
		WithRegion(qs.Region)
	if qs.Endpoint != "" {
		awsConfig.WithEndpoint(qs.Endpoint)
	}
	if opts.HTTPClient != nil {
		awsConfig.WithHTTPClient(opts.HTTPClient)
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
		return nil, errors.Trace(err)
	}

	if !opts.SendCredentials {
		// Clear the credentials if exists so that they will not be sent to TiKV
		backend.AccessKey = ""
		backend.SecretAccessKey = ""
	} else if ses.Config.Credentials != nil {
		if qs.AccessKey == "" || qs.SecretAccessKey == "" {
			v, cerr := ses.Config.Credentials.Get()
			if cerr != nil {
				return nil, errors.Trace(cerr)
			}
			backend.AccessKey = v.AccessKeyID
			backend.SecretAccessKey = v.SecretAccessKey
		}
	}

	c := s3.New(ses)
	if !opts.SkipCheckPath {
		err = checkS3Bucket(c, qs.Bucket)
		if err != nil {
			return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "Bucket %s is not accessible: %v", qs.Bucket, err)
		}
	}

	qs.Prefix += "/"
	return &S3Storage{
		session: ses,
		svc:     c,
		options: &qs,
	}, nil
}

// checkBucket checks if a bucket exists.
func checkS3Bucket(svc *s3.S3, bucket string) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := svc.HeadBucket(input)
	return errors.Trace(err)
}

// WriteFile writes data to a file to storage.
func (rs *S3Storage) WriteFile(ctx context.Context, file string, data []byte) error {
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
		return errors.Trace(err)
	}
	hinput := &s3.HeadObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}
	err = rs.svc.WaitUntilObjectExistsWithContext(ctx, hinput)
	return errors.Trace(err)
}

// ReadFile reads the file from the storage and returns the contents.
func (rs *S3Storage) ReadFile(ctx context.Context, file string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}

	result, err := rs.svc.GetObjectWithContext(ctx, input)
	if err != nil {
		er := errors.Trace(err)
		return nil, errors.Annotatef(er,
			"failed to read s3 file, file info: input.bucket='%s', input.key='%s', input.key ='%s'",
			input.Bucket, input.Key)
	}
	defer result.Body.Close()
	data, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return nil, errors.Trace(err)
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
		if aerr, ok := errors.Cause(err).(awserr.Error); ok { // nolint:errorlint
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket, s3.ErrCodeNoSuchKey, notFound:
				return false, nil
			}
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The first argument is the file path that can be used in `Open`
// function; the second argument is the size in byte of the file determined
// by path.
func (rs *S3Storage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
	if opt == nil {
		opt = &WalkOption{}
	}
	prefix := rs.options.Prefix + opt.SubDir
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	maxKeys := int64(1000)
	if opt.ListCount > 0 {
		maxKeys = opt.ListCount
	}

	req := &s3.ListObjectsInput{
		Bucket:  aws.String(rs.options.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(maxKeys),
	}
	for {
		// FIXME: We can't use ListObjectsV2, it is not universally supported.
		// (Ceph RGW supported ListObjectsV2 since v15.1.0, released 2020 Jan 30th)
		// (as of 2020, DigitalOcean Spaces still does not support V2 - https://developers.digitalocean.com/documentation/spaces/#list-bucket-contents)
		res, err := rs.svc.ListObjectsWithContext(ctx, req)
		if err != nil {
			return errors.Trace(err)
		}
		for _, r := range res.Contents {
			// when walk on specify directory, the result include storage.Prefix,
			// which can not be reuse in other API(Open/Read) directly.
			// so we use TrimPrefix to filter Prefix for next Open/Read.
			path := strings.TrimPrefix(*r.Key, rs.options.Prefix)
			if err = fn(path, *r.Size); err != nil {
				return errors.Trace(err)
			}

			// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html#AmazonS3-ListObjects-response-NextMarker -
			//
			// `res.NextMarker` is populated only if we specify req.Delimiter.
			// Aliyun OSS and minio will populate NextMarker no matter what,
			// but this documented behavior does apply to AWS S3:
			//
			// "If response does not include the NextMarker and it is truncated,
			// you can use the value of the last Key in the response as the marker
			// in the subsequent request to get the next set of object keys."
			req.Marker = r.Key
		}
		if !aws.BoolValue(res.IsTruncated) {
			break
		}
	}

	return nil
}

// URI returns s3://<base>/<prefix>.
func (rs *S3Storage) URI() string {
	return "s3://" + rs.options.Bucket + "/" + rs.options.Prefix
}

// Open a Reader by file path.
func (rs *S3Storage) Open(ctx context.Context, path string) (ExternalFileReader, error) {
	reader, r, err := rs.open(ctx, path, 0, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &s3ObjectReader{
		storage:   rs,
		name:      path,
		reader:    reader,
		ctx:       ctx,
		rangeInfo: r,
	}, nil
}

// RangeInfo represents the an HTTP Content-Range header value
// of the form `bytes [Start]-[End]/[Size]`.
type RangeInfo struct {
	// Start is the absolute position of the first byte of the byte range,
	// starting from 0.
	Start int64
	// End is the absolute position of the last byte of the byte range. This end
	// offset is inclusive, e.g. if the Size is 1000, the maximum value of End
	// would be 999.
	End int64
	// Size is the total size of the original file.
	Size int64
}

// if endOffset > startOffset, should return reader for bytes in [startOffset, endOffset).
func (rs *S3Storage) open(
	ctx context.Context,
	path string,
	startOffset, endOffset int64,
) (io.ReadCloser, RangeInfo, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + path),
	}

	// always set rangeOffset to fetch file size info
	// s3 endOffset is inclusive
	var rangeOffset *string
	if endOffset > startOffset {
		rangeOffset = aws.String(fmt.Sprintf("bytes=%d-%d", startOffset, endOffset-1))
	} else {
		rangeOffset = aws.String(fmt.Sprintf("bytes=%d-", startOffset))
	}
	input.Range = rangeOffset
	result, err := rs.svc.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, RangeInfo{}, errors.Trace(err)
	}

	r, err := ParseRangeInfo(result.ContentRange)
	if err != nil {
		return nil, RangeInfo{}, errors.Trace(err)
	}

	if startOffset != r.Start || (endOffset != 0 && endOffset != r.End+1) {
		return nil, r, errors.Annotatef(berrors.ErrStorageUnknown, "open file '%s' failed, expected range: %s, got: %v",
			path, *rangeOffset, result.ContentRange)
	}

	return result.Body, r, nil
}

var contentRangeRegex = regexp.MustCompile(`bytes (\d+)-(\d+)/(\d+)$`)

// ParseRangeInfo parses the Content-Range header and returns the offsets.
func ParseRangeInfo(info *string) (ri RangeInfo, err error) {
	if info == nil || len(*info) == 0 {
		err = errors.Annotate(berrors.ErrStorageUnknown, "ContentRange is empty")
		return
	}
	subMatches := contentRangeRegex.FindStringSubmatch(*info)
	if len(subMatches) != 4 {
		err = errors.Annotatef(berrors.ErrStorageUnknown, "invalid content range: '%s'", *info)
		return
	}

	ri.Start, err = strconv.ParseInt(subMatches[1], 10, 64)
	if err != nil {
		err = errors.Annotatef(err, "invalid start offset value '%s' in ContentRange '%s'", subMatches[1], *info)
		return
	}
	ri.End, err = strconv.ParseInt(subMatches[2], 10, 64)
	if err != nil {
		err = errors.Annotatef(err, "invalid end offset value '%s' in ContentRange '%s'", subMatches[2], *info)
		return
	}
	ri.Size, err = strconv.ParseInt(subMatches[3], 10, 64)
	if err != nil {
		err = errors.Annotatef(err, "invalid size size value '%s' in ContentRange '%s'", subMatches[3], *info)
		return
	}
	return
}

// s3ObjectReader wrap GetObjectOutput.Body and add the `Seek` method.
type s3ObjectReader struct {
	storage   *S3Storage
	name      string
	reader    io.ReadCloser
	pos       int64
	rangeInfo RangeInfo
	// reader context used for implement `io.Seek`
	// currently, lightning depends on package `xitongsys/parquet-go` to read parquet file and it needs `io.Seeker`
	// See: https://github.com/xitongsys/parquet-go/blob/207a3cee75900b2b95213627409b7bac0f190bb3/source/source.go#L9-L10
	ctx      context.Context
	retryCnt int
}

// Read implement the io.Reader interface.
func (r *s3ObjectReader) Read(p []byte) (n int, err error) {
	maxCnt := r.rangeInfo.End + 1 - r.pos
	if maxCnt > int64(len(p)) {
		maxCnt = int64(len(p))
	}
	n, err = r.reader.Read(p[:maxCnt])
	// TODO: maybe we should use !errors.Is(err, io.EOF) here to avoid error lint, but currently, pingcap/errors
	// doesn't implement this method yet.
	if err != nil && errors.Cause(err) != io.EOF && r.retryCnt < maxErrorRetries { //nolint:errorlint
		// if can retry, reopen a new reader and try read again
		end := r.rangeInfo.End + 1
		if end == r.rangeInfo.Size {
			end = 0
		}
		_ = r.reader.Close()

		newReader, _, err1 := r.storage.open(r.ctx, r.name, r.pos, end)
		if err1 != nil {
			log.Warn("open new s3 reader failed", zap.String("file", r.name), zap.Error(err1))
			return
		}
		r.reader = newReader
		r.retryCnt++
		n, err = r.reader.Read(p[:maxCnt])
	}

	r.pos += int64(n)
	return
}

// Close implement the io.Closer interface.
func (r *s3ObjectReader) Close() error {
	return r.reader.Close()
}

// Seek implement the io.Seeker interface.
//
// Currently, tidb-lightning depends on this method to read parquet file for s3 storage.
func (r *s3ObjectReader) Seek(offset int64, whence int) (int64, error) {
	var realOffset int64
	switch whence {
	case io.SeekStart:
		realOffset = offset
	case io.SeekCurrent:
		realOffset = r.pos + offset
	case io.SeekEnd:
		realOffset = r.rangeInfo.Size + offset
	default:
		return 0, errors.Annotatef(berrors.ErrStorageUnknown, "Seek: invalid whence '%d'", whence)
	}

	if realOffset == r.pos {
		return realOffset, nil
	}

	// if seek ahead no more than 64k, we discard these data
	if realOffset > r.pos && realOffset-r.pos <= maxSkipOffsetByRead {
		_, err := io.CopyN(ioutil.Discard, r, realOffset-r.pos)
		if err != nil {
			return r.pos, errors.Trace(err)
		}
		return realOffset, nil
	}

	// close current read and open a new one which target offset
	err := r.reader.Close()
	if err != nil {
		return 0, errors.Trace(err)
	}

	newReader, info, err := r.storage.open(r.ctx, r.name, realOffset, 0)
	if err != nil {
		return 0, errors.Trace(err)
	}
	r.reader = newReader
	r.rangeInfo = info
	r.pos = realOffset
	return realOffset, nil
}

// CreateUploader create multi upload request.
func (rs *S3Storage) CreateUploader(ctx context.Context, name string) (ExternalFileWriter, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + name),
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

	resp, err := rs.svc.CreateMultipartUploadWithContext(ctx, input)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &S3Uploader{
		svc:           rs.svc,
		createOutput:  resp,
		completeParts: make([]*s3.CompletedPart, 0, 128),
	}, nil
}

// Create creates multi upload request.
func (rs *S3Storage) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
	uploader, err := rs.CreateUploader(ctx, name)
	if err != nil {
		return nil, err
	}
	uploaderWriter := newBufferedWriter(uploader, hardcodedS3ChunkSize, NoCompression)
	return uploaderWriter, nil
}
