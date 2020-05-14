// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/utils"
)

var (
	// ErrEpochNotMatch is the error raised when ingestion failed with "epoch
	// not match". This error is retryable.
	ErrEpochNotMatch = errors.NewNoStackError("epoch not match")
	// ErrKeyNotInRegion is the error raised when ingestion failed with "key not
	// in region". This error cannot be retried.
	ErrKeyNotInRegion = errors.NewNoStackError("key not in region")
	// ErrRewriteRuleNotFound is the error raised when download failed with
	// "rewrite rule not found". This error cannot be retried
	ErrRewriteRuleNotFound = errors.NewNoStackError("rewrite rule not found")
	// ErrRangeIsEmpty is the error raised when download failed with "range is
	// empty". This error cannot be retried.
	ErrRangeIsEmpty = errors.NewNoStackError("range is empty")
	// ErrGRPC indicates any gRPC communication error. This error can be retried.
	ErrGRPC = errors.NewNoStackError("gRPC error")
	// ErrDownloadFailed indicates a generic, non-retryable download error.
	ErrDownloadFailed = errors.NewNoStackError("download sst failed")
	// ErrIngestFailed indicates a generic, retryable ingest error.
	ErrIngestFailed = errors.NewNoStackError("ingest sst failed")
)

const (
	importSSTRetryTimes      = 16
	importSSTWaitInterval    = 10 * time.Millisecond
	importSSTMaxWaitInterval = 1 * time.Second

	downloadSSTRetryTimes      = 8
	downloadSSTWaitInterval    = 10 * time.Millisecond
	downloadSSTMaxWaitInterval = 1 * time.Second

	resetTsRetryTime       = 16
	resetTSWaitInterval    = 50 * time.Millisecond
	resetTSMaxWaitInterval = 500 * time.Millisecond
)

type importerBackoffer struct {
	attempt      int
	delayTime    time.Duration
	maxDelayTime time.Duration
}

// NewBackoffer creates a new controller regulating a truncated exponential backoff.
func NewBackoffer(attempt int, delayTime, maxDelayTime time.Duration) utils.Backoffer {
	return &importerBackoffer{
		attempt:      attempt,
		delayTime:    delayTime,
		maxDelayTime: maxDelayTime,
	}
}

func newImportSSTBackoffer() utils.Backoffer {
	return NewBackoffer(importSSTRetryTimes, importSSTWaitInterval, importSSTMaxWaitInterval)
}

func newDownloadSSTBackoffer() utils.Backoffer {
	return NewBackoffer(downloadSSTRetryTimes, downloadSSTWaitInterval, downloadSSTMaxWaitInterval)
}

func (bo *importerBackoffer) NextBackoff(err error) time.Duration {
	switch errors.Cause(err) {
	case ErrGRPC, ErrEpochNotMatch, ErrIngestFailed:
		bo.delayTime = 2 * bo.delayTime
		bo.attempt--
	case ErrRangeIsEmpty, ErrRewriteRuleNotFound:
		// Excepted error, finish the operation
		bo.delayTime = 0
		bo.attempt = 0
	default:
		// Unexcepted error
		bo.delayTime = 0
		bo.attempt = 0
		log.Warn("unexcepted error, stop to retry", zap.Error(err))
	}
	if bo.delayTime > bo.maxDelayTime {
		return bo.maxDelayTime
	}
	return bo.delayTime
}

func (bo *importerBackoffer) Attempt() int {
	return bo.attempt
}

type pdReqBackoffer struct {
	attempt      int
	delayTime    time.Duration
	maxDelayTime time.Duration
}

func newPDReqBackoffer() utils.Backoffer {
	return &pdReqBackoffer{
		attempt:      resetTsRetryTime,
		delayTime:    resetTSWaitInterval,
		maxDelayTime: resetTSMaxWaitInterval,
	}
}

func (bo *pdReqBackoffer) NextBackoff(err error) time.Duration {
	bo.delayTime = 2 * bo.delayTime
	bo.attempt--
	if bo.delayTime > bo.maxDelayTime {
		return bo.maxDelayTime
	}
	return bo.delayTime
}

func (bo *pdReqBackoffer) Attempt() int {
	return bo.attempt
}
