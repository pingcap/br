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

// MessageIsRetryableStorageError checks whether the message returning from TiKV is retryable ExternalStorageError.
func MessageIsRetryableStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	// If failed to read/write to S3/GCS.
	failed := strings.Contains(msgLower, "failed to put object") ||
		strings.Contains(msgLower, "failed to get object") || strings.Contains(msgLower, "invalid http request")
	// If S3/GCS stop or not start.
	closedOrRefused := strings.Contains(msgLower, "server closed") || strings.Contains(msgLower, "writing a body to connection") ||
		strings.Contains(msgLower, "connection refused") ||  strings.Contains(msgLower, "connection reset by peer")
	// Those conditions are retryable.
	return failed && closedOrRefused
}
