// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"time"
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

// WithRetry retrys a given operation with a backoff policy
func WithRetry(
	ctx context.Context,
	retryableFunc RetryableFunc,
	backoffer Backoffer,
) error {
	var lastErr error
	for backoffer.Attempt() > 0 {
		err := retryableFunc()
		if err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				return lastErr
			case <-time.After(backoffer.NextBackoff(err)):
			}
		} else {
			return nil
		}
	}
	return lastErr
}
