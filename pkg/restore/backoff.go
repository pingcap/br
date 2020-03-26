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
	errEpochNotMatch       = errors.NewNoStackError("epoch not match")
	errKeyNotInRegion      = errors.NewNoStackError("key not in region")
	errRewriteRuleNotFound = errors.NewNoStackError("rewrite rule not found")
	errRangeIsEmpty        = errors.NewNoStackError("range is empty")
	errGrpc                = errors.NewNoStackError("gRPC error")
	errDownloadFailed      = errors.NewNoStackError("download sst failed")
	errIngestFailed        = errors.NewNoStackError("ingest sst failed")
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

func newImportSSTBackoffer() utils.Backoffer {
	return &importerBackoffer{
		attempt:      importSSTRetryTimes,
		delayTime:    importSSTWaitInterval,
		maxDelayTime: importSSTMaxWaitInterval,
	}
}

func newDownloadSSTBackoffer() utils.Backoffer {
	return &importerBackoffer{
		attempt:      downloadSSTRetryTimes,
		delayTime:    downloadSSTWaitInterval,
		maxDelayTime: downloadSSTMaxWaitInterval,
	}
}

func (bo *importerBackoffer) NextBackoff(err error) time.Duration {
	switch errors.Cause(err) {
	case errGrpc, errEpochNotMatch, errIngestFailed:
		bo.delayTime = 2 * bo.delayTime
		bo.attempt--
	case errRangeIsEmpty, errRewriteRuleNotFound:
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
