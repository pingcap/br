// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"strings"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/rtree"
)

// pushDown wraps a backup task.
type pushDown struct {
	mgr    ClientMgr
	respCh chan *backup.BackupResponse
	errCh  chan error
}

// newPushDown creates a push down backup.
func newPushDown(mgr ClientMgr, cap int) *pushDown {
	return &pushDown{
		mgr:    mgr,
		respCh: make(chan *backup.BackupResponse, cap),
		errCh:  make(chan error, cap),
	}
}

// FullBackup make a full backup of a tikv cluster.
func (push *pushDown) pushBackup(
	ctx context.Context,
	req backup.BackupRequest,
	stores []*metapb.Store,
	updateCh glue.Progress,
) (rtree.RangeTree, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("pushDown.pushBackup", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// Push down backup tasks to all tikv instances.
	res := rtree.NewRangeTree()
	wg := new(sync.WaitGroup)
	for _, s := range stores {
		storeID := s.GetId()
		if s.GetState() != metapb.StoreState_Up {
			log.Warn("skip store", zap.Uint64("StoreID", storeID), zap.Stringer("State", s.GetState()))
			continue
		}
		client, err := push.mgr.GetBackupClient(ctx, storeID)
		if err != nil {
			log.Error("fail to connect store", zap.Uint64("StoreID", storeID))
			return res, errors.Trace(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := SendBackup(
				ctx, storeID, client, req,
				func(resp *backup.BackupResponse) error {
					// Forward all responses (including error).
					push.respCh <- resp
					return nil
				},
				func() (backup.BackupClient, error) {
					log.Warn("reset the connection in push", zap.Uint64("storeID", storeID))
					return push.mgr.ResetBackupClient(ctx, storeID)
				})
			if err != nil {
				push.errCh <- err
				return
			}
		}()
	}

	go func() {
		wg.Wait()
		// TODO: test concurrent receive response and close channel.
		close(push.respCh)
	}()

	for {
		select {
		case resp, ok := <-push.respCh:
			if !ok {
				// Finished.
				return res, nil
			}
			if resp.GetError() == nil {
				// None error means range has been backuped successfully.
				res.Put(
					resp.GetStartKey(), resp.GetEndKey(), resp.GetFiles())

				// Update progress
				updateCh.Inc()
			} else {
				errPb := resp.GetError()
				switch v := errPb.Detail.(type) {
				case *backup.Error_KvError:
					log.Warn("backup occur kv error", zap.Reflect("error", v))

				case *backup.Error_RegionError:
					log.Warn("backup occur region error", zap.Reflect("error", v))

				case *backup.Error_ClusterIdError:
					log.Error("backup occur cluster ID error", zap.Reflect("error", v))
					return res, errors.Annotatef(berrors.ErrKVClusterIDMismatch, "%v", errPb)

				default:
					// UNSAFE! TODO: Add a error type for failed to put file.
					if messageIsRetryableFailedToWrite(errPb.GetMsg()) {
						log.Warn("backup occur s3 storage error", zap.String("error", errPb.GetMsg()))
						continue
					}
					log.Error("backup occur unknown error", zap.String("error", errPb.GetMsg()))
					return res, errors.Annotatef(berrors.ErrKVUnknown, "%v", errPb)
				}
			}
		case err := <-push.errCh:
			return res, errors.Trace(err)
		}
	}
}

func messageIsRetryableFailedToWrite(msg string) bool {
	return strings.Contains(msg, "failed to put object") /* If failed to backup to S3... */ &&
		// ...Because of s3 stop or not start...
		(strings.Contains(msg, "Server closed") || strings.Contains(msg, "Connection refused"))
	// ...those condition would be retryable.
}
