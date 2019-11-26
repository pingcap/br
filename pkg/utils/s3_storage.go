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
	"bytes"
	"fmt"
	"io/ioutil"
	"net/url"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"

	"github.com/pingcap/log"
)

const (
	notFound        = "NotFound"
	accessKey       = "ACCESS_KEY"
	secretAccessKey = "SECRET_ACCESS_KEY"
	regionKey       = "REGION"
	maxRetries      = 3 // number of retries to make of operations
)

// S3Storage info for s3 storage
type S3Storage struct {
	session *session.Session
	svc     *s3.S3
	bucket  string
	path    string
}

// newS3Storage creates new s3 storage for metadata
// rawURL will be in format of "s3://bucket/prefix"
func newS3Storage(u *url.URL) (*S3Storage, error) {
	if u.Scheme == "" {
		return nil, fmt.Errorf("no scheme from storage arg %#v", u)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("no host from storage arg %#v", u)
	}
	if u.Path == "" {
		return nil, fmt.Errorf("no path from storage arg %#v", u)
	}
	if u.RawQuery == "" {
		return nil, fmt.Errorf("no query parameters from storage arg %#v", u)
	}
	qs, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}
	ak, ok := qs[accessKey]
	if !ok || len(ak) != 1 || len(ak[0]) == 0 {
		return nil, fmt.Errorf("invalid %s query parameters from storage arg %#v", accessKey, u)
	}
	sak, ok := qs[secretAccessKey]
	if !ok || len(ak) != 1 || len(sak[0]) == 0 {
		return nil, fmt.Errorf("invalid %s query parameters from storage arg %#v", secretAccessKey, u)
	}
	rg, ok := qs[regionKey]
	if !ok || len(rg) != 1 || len(rg[0]) == 0 {
		return nil, fmt.Errorf("invalid %s query parameters from storage arg %#v", regionKey, u)
	}

	v := credentials.Value{
		AccessKeyID:     ak[0],
		SecretAccessKey: sak[0],
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
		WithCredentials(cred).
		WithRegion(rg[0])

	// if endpoint != "" {
	// 	awsConfig.WithEndpoint(opt.Endpoint)
	// }
	// awsConfig.WithLogLevel(aws.LogDebugWithSigning)
	awsSessionOpts := session.Options{
		Config: *awsConfig,
	}
	ses, err := session.NewSessionWithOptions(awsSessionOpts)
	if err != nil {
		return nil, err
	}
	c := s3.New(ses)

	rs := S3Storage{
		session: ses,
		svc:     c,
		bucket:  u.Host,
		path:    u.Path,
	}
	err = rs.checkBucket()
	if err != nil {
		return nil, err
	}
	return &rs, nil
}

// createBucket creates a bucket
func (rs *S3Storage) createBucket() error {
	// Create the S3 Bucket
	_, err := rs.svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(rs.bucket),
	})
	if err != nil {
		return err
	}

	// Wait until bucket is created before finishing
	err = rs.svc.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(rs.bucket),
	})

	return err
}

// checkBucket checks if a bucket exists and creates it if not
func (rs *S3Storage) checkBucket() error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(rs.bucket),
	}

	_, err := rs.svc.HeadBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket, notFound:
				return rs.createBucket()
			default:
				return err
			}
		}
	}
	return err
}

// Write write to s3 storage
func (rs *S3Storage) Write(file string, data []byte) error {
	input := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(bytes.NewReader(data)),
		Bucket: aws.String(rs.bucket),
		Key:    aws.String(path.Join(rs.path, file)),
	}

	_, err := rs.svc.PutObject(input)
	if err != nil {
		return err
	}
	log.Info("Write to s3 successfully", zap.String("bucket", rs.bucket), zap.String("key", path.Join(rs.path, file)))
	return nil
}

// Read read file from s3
func (rs *S3Storage) Read(name string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(rs.bucket),
		Key:    aws.String(path.Join(rs.path, name)),
	}

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
func (rs *S3Storage) FileExists(rfile string) (bool, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(rs.bucket),
		Key:    aws.String(path.Join(rs.path, rfile)),
	}

	_, err := rs.svc.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket, s3.ErrCodeNoSuchKey:
				return false, nil
			default:
				return true, err
			}
		}
	}

	return true, err
}
