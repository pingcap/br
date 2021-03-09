// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"strings"
	"time"

	"go.uber.org/multierr"
)

// RetryableFunc presents a retryable operation.
type RetryableFunc func() error

// Backoffer implements a backoff policy for retrying operations.
type Backoffer interface {
	// NextBackoff returns a duration to wait before retrying again
	NextBackoff(err error) time.Duration
	// Attempt returns the remain attempt times
	Attempt() int
}

// WithRetry retries a given operation with a backoff policy.
//
// Returns nil if `retryableFunc` succeeded at least once. Otherwise, returns a
// multierr containing all errors encountered.
func WithRetry(
	ctx context.Context,
	retryableFunc RetryableFunc,
	backoffer Backoffer,
) error {
	var allErrors error
	for backoffer.Attempt() > 0 {
		err := retryableFunc()
		if err != nil {
			allErrors = multierr.Append(allErrors, err)
			select {
			case <-ctx.Done():
				return allErrors // nolint:wrapcheck
			case <-time.After(backoffer.NextBackoff(err)):
			}
		} else {
			return nil
		}
	}
	return allErrors // nolint:wrapcheck
}

// MessageIsRetryableS3Error checks whether the message returning from TiKV is retryable ExternalStorageError.
//
func MessageIsRetryableS3Error(msg string) bool {
	msgLower := strings.ToLower(msg)
	return (strings.Contains(msgLower, "failed to put object") || strings.Contains(msgLower, "failed to get object")) /* If failed to read/write to S3... */ &&
		// ...Because of s3 stop or not start...
		(strings.Contains(msgLower, "server closed") || strings.Contains(msgLower, "connection refused"))
	// ...those conditions would be retryable.
}
