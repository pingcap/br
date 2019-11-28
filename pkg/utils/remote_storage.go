// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"

	"github.com/pingcap/log"
)

const (
	accessKey       = "access_key"
	secretAccessKey = "secret_access_key"
	regionKey       = "region"
	insecureKey     = "insecure"
	providerKey     = "provider"
	prefixKey       = "prefix"
	endpointKey     = "endpoint"
	awsKey          = "aws"
	aliKey          = "aliyun"

	notFound   = "NotFound"
	maxRetries = 3 // number of retries to make of operations
)

// RemoteStorage info for remote storage
type RemoteStorage struct {
	bucket *blob.Bucket
}

type s3Query struct {
	accessKey       string
	secretAccessKey string
	region          string
	endpoint        string
	bucket          string
	prefix          string
	provider        string
}

// newRemoteStorage creates new remote storage for metadata
// rawURL will be in format of:
// s3:///bucket_name?access_key=xxx&secret_access_key=yyy&
// provider=aws&region=zzz&prefix=aaa
// s3:///bucket_name?access_key=xxx&secret_access_key=yyy&
// insecure=true&endpoint=172.6.4.3:3000&prefix=aaa
func newRemoteStorage(u *url.URL) (*RemoteStorage, error) {
	switch u.Scheme {
	case "s3":
		qs, err := checkS3Config(u)
		if err != nil {
			return nil, err
		}
		bucket, err := newS3Storage(qs)
		if err != nil {
			return nil, err
		}
		return &RemoteStorage{
			bucket: bucket,
		}, nil
	default:
		return nil, fmt.Errorf("storage %s not support yet", u.Scheme)
	}
}

// Write write to remote storage
func (rs *RemoteStorage) Write(file string, data []byte) error {
	ctx := context.Background()

	// Open the key for writing with the default options.
	err := rs.bucket.WriteAll(ctx, file, data, nil)
	if err != nil {
		return err
	}

	log.Info("Write to remote storage successfully", zap.String("key", file))
	return nil
}

// Read read file from remote storage
func (rs *RemoteStorage) Read(file string) ([]byte, error) {
	ctx := context.Background()
	// Read from the key.
	return rs.bucket.ReadAll(ctx, file)
}

// FileExists check if file exists on s3 storage
func (rs *RemoteStorage) FileExists(file string) (bool, error) {
	ctx := context.Background()
	// Check the key.
	return rs.bucket.Exists(ctx, file)
}

// newS3Storage initialize a new s3 storage for metadata
func newS3Storage(qs *s3Query) (*blob.Bucket, error) {
	v := credentials.Value{
		AccessKeyID:     qs.accessKey,
		SecretAccessKey: qs.secretAccessKey,
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
	}
	cred := credentials.NewChainCredentials(providers)

	awsConfig := aws.NewConfig().
		WithMaxRetries(maxRetries).
		WithCredentials(cred)
	if qs.region != "" {
		awsConfig.WithRegion(qs.region)
	}
	if qs.endpoint != "" {
		awsConfig.WithEndpoint(qs.endpoint).
			WithS3ForcePathStyle(true)
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
	err = checkS3Bucket(c, qs.bucket)
	if err != nil {
		return nil, err
	}
	// Create a *blob.Bucket.
	bkt, err := s3blob.OpenBucket(context.Background(), ses, qs.bucket, nil)
	if err != nil {
		return nil, err
	}
	return blob.PrefixedBucket(bkt, qs.prefix), nil

}

func checkS3Config(u *url.URL) (*s3Query, error) {
	sqs := s3Query{}

	if u.Host == "" {
		return nil, fmt.Errorf("no host from storage arg %#v", u)
	}
	sqs.bucket = u.Host

	if u.Path != "" {
		log.Info("path in storage arg is ignored", zap.String("path", u.Path))
	}

	if u.RawQuery == "" {
		return nil, fmt.Errorf("no query parameters from storage arg %#v", u)
	}
	qs, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}
	ak, ok := qs[accessKey]
	if !ok || len(ak) != 1 {
		return nil, fmt.Errorf("invalid %s query parameters from storage arg %#v", accessKey, u)
	}
	sqs.accessKey = ak[0]
	sak, ok := qs[secretAccessKey]
	if !ok || len(ak) != 1 {
		return nil, fmt.Errorf("invalid %s query parameters from storage arg %#v", secretAccessKey, u)
	}
	sqs.secretAccessKey = sak[0]
	rg, ok := qs[regionKey]
	if ok && len(rg) >= 1 {
		sqs.region = rg[0]
	}
	prs, ok := qs[providerKey]
	if ok && len(prs) >= 1 {
		sqs.provider = prs[0]
	}
	pfs, ok := qs[prefixKey]
	if ok && len(pfs) >= 1 {
		sqs.prefix = pfs[0]
	}
	sqs.prefix = strings.Trim(sqs.prefix, "/")
	sqs.prefix += "/"

	eps, ok := qs[endpointKey]
	if ok && len(eps) >= 1 {
		sqs.endpoint = eps[0]
	}

	if sqs.provider != awsKey && sqs.provider != aliKey && sqs.endpoint == "" {
		return nil, fmt.Errorf("endpoint should be set for provider %s in storage arg %#v", sqs.provider, u)
	}
	if sqs.provider == awsKey && sqs.region == "" {
		return nil, fmt.Errorf("region should be set for provider %s in storage arg %#v", sqs.provider, u)
	}
	if sqs.region == "" {
		sqs.region = "us-east-1"
	}
	var insecure string
	ins, ok := qs[insecureKey]
	if ok && len(ins) == 1 {
		insecure = ins[0]
	}
	if sqs.endpoint != "" {
		if insecure == "true" {
			sqs.endpoint = "http://" + sqs.endpoint
		} else {
			sqs.endpoint = "https://" + sqs.endpoint
		}
	}

	return &sqs, nil
}

// createBucket creates a bucket
func createS3Bucket(svc *s3.S3, bucket string) error {
	// Create the S3 Bucket
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return err
	}

	// Wait until bucket is created before finishing
	err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	return err
}

// checkBucket checks if a bucket exists and creates it if not
func checkS3Bucket(svc *s3.S3, bucket string) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}

	_, err := svc.HeadBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket, notFound:
				return createS3Bucket(svc, bucket)
			default:
				return err
			}
		}
	}
	return err
}
