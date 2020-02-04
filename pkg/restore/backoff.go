package restore

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/utils"
)

var (
	errNotLeader           = errors.New("not leader")
	errKeyNotInRegion      = errors.New("key not in region")
	errResp                = errors.New("response error")
	errRewriteRuleNotFound = errors.New("rewrite rule not found")
	errRangeIsEmpty        = errors.New("range is empty")
	errGrpc                = errors.New("gRPC error")

	// TODO: add `error` field to `DownloadResponse` for distinguish the errors of gRPC
	// and the errors of request
	errBadFormat      = errors.New("bad format")
	errWrongKeyPrefix = errors.New("wrong key prefix")
	errFileCorrupted  = errors.New("file corrupted")
	errCannotRead     = errors.New("cannot read externel storage")
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
	switch err {
	case errResp, errGrpc:
		bo.delayTime = 2 * bo.delayTime
		bo.attempt--
	default:
		// Don't continue to retry
		bo.delayTime = 0
		bo.attempt = 0
		log.Warn("undetemined error, stop to retry", zap.Error(err))
	}
	if bo.delayTime > bo.maxDelayTime {
		return bo.maxDelayTime
	}
	return bo.delayTime
}

func (bo *importerBackoffer) Attempt() int {
	return bo.attempt
}

type resetTSBackoffer struct {
	attempt      int
	delayTime    time.Duration
	maxDelayTime time.Duration
}

func newResetTSBackoffer() utils.Backoffer {
	return &resetTSBackoffer{
		attempt:      resetTsRetryTime,
		delayTime:    resetTSWaitInterval,
		maxDelayTime: resetTSMaxWaitInterval,
	}
}

func (bo *resetTSBackoffer) NextBackoff(err error) time.Duration {
	bo.delayTime = 2 * bo.delayTime
	bo.attempt--
	if bo.delayTime > bo.maxDelayTime {
		return bo.maxDelayTime
	}
	return bo.delayTime
}

func (bo *resetTSBackoffer) Attempt() int {
	return bo.attempt
}
