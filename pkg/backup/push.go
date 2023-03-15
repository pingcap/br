// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"fmt"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/logutil"
	"github.com/pingcap/br/pkg/redact"
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/utils"
)

// pushDown wraps a backup task.
type pushDown struct {
	mgr    ClientMgr
	respCh chan responseAndStore
	errCh  chan error
}

type responseAndStore struct {
	Resp  *backuppb.BackupResponse
	Store *metapb.Store
}

func (r responseAndStore) GetResponse() *backuppb.BackupResponse {
	return r.Resp
}

func (r responseAndStore) GetStore() *metapb.Store {
	return r.Store
}

// newPushDown creates a push down backup.
func newPushDown(mgr ClientMgr, cap int) *pushDown {
	return &pushDown{
		mgr:    mgr,
		respCh: make(chan responseAndStore, cap),
		errCh:  make(chan error, cap),
	}
}

// FullBackup make a full backup of a tikv cluster.
func (push *pushDown) pushBackup(
	ctx context.Context,
	req backuppb.BackupRequest,
	stores []*metapb.Store,
	progressCallBack func(ProgressUnit),
) (rtree.RangeTree, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("pushDown.pushBackup", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// Push down backup tasks to all tikv instances.
	res := rtree.NewRangeTree()
	failpoint.Inject("noop-backup", func(_ failpoint.Value) {
		log.Warn("skipping normal backup, jump to fine-grained backup, meow :3", logutil.Key("start-key", req.StartKey), logutil.Key("end-key", req.EndKey))
		failpoint.Return(res, nil)
	})

	wg := new(sync.WaitGroup)
	for _, s := range stores {
		store := s
		storeID := s.GetId()
		if s.GetState() != metapb.StoreState_Up {
			log.Warn("skip store", zap.Uint64("StoreID", storeID), zap.Stringer("State", s.GetState()))
			continue
		}
		client, err := push.mgr.GetBackupClient(ctx, storeID)
		if err != nil {
			// BR should be able to backup even some of stores disconnected.
			// The regions managed by this store can be retried at fine-grained backup then.
			log.Warn("fail to connect store, skipping", zap.Uint64("StoreID", storeID), zap.Error(err))
			return res, nil
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := SendBackup(
				ctx, storeID, client, req,
				func(resp *backuppb.BackupResponse) error {
					// Forward all responses (including error).
					push.respCh <- responseAndStore{
						Resp:  resp,
						Store: store,
					}
					return nil
				},
				func() (backuppb.BackupClient, error) {
					log.Warn("reset the connection in push", zap.Uint64("storeID", storeID))
					return push.mgr.ResetBackupClient(ctx, storeID)
				})
			// Disconnected stores can be ignored.
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
		case respAndStore, ok := <-push.respCh:
			resp := respAndStore.GetResponse()
			store := respAndStore.GetStore()
			if !ok {
				// Finished.
				return res, nil
			}
			failpoint.Inject("backup-timeout-error", func(val failpoint.Value) {
				msg := val.(string)
				log.Debug("failpoint backup-timeout-error injected.", zap.String("msg", msg))
				resp.Error = &backuppb.Error{
					Msg: msg,
				}
			})
			failpoint.Inject("backup-storage-error", func(val failpoint.Value) {
				msg := val.(string)
				log.Debug("failpoint backup-storage-error injected.", zap.String("msg", msg))
				resp.Error = &backuppb.Error{
					Msg: msg,
				}
			})
			failpoint.Inject("tikv-rw-error", func(val failpoint.Value) {
				msg := val.(string)
				log.Debug("failpoint tikv-rw-error injected.", zap.String("msg", msg))
				resp.Error = &backuppb.Error{
					Msg: msg,
				}
			})
			if resp.GetError() == nil {
				// None error means range has been backuped successfully.
				res.Put(
					resp.GetStartKey(), resp.GetEndKey(), resp.GetFiles())

				// Update progress
				progressCallBack(RegionUnit)
			} else {
				errPb := resp.GetError()
				switch v := errPb.Detail.(type) {
				case *backuppb.Error_KvError:
					log.Warn("backup occur kv error", zap.Reflect("error", v))

				case *backuppb.Error_RegionError:
					log.Warn("backup occur region error", zap.Reflect("error", v))

				case *backuppb.Error_ClusterIdError:
					log.Error("backup occur cluster ID error", zap.Reflect("error", v))
					return res, errors.Annotatef(berrors.ErrKVClusterIDMismatch, "%v", errPb)
				default:
					if utils.MessageIsRetryableStorageError(errPb.GetMsg()) {
						log.Warn("backup occur storage error", zap.String("error", errPb.GetMsg()))
						continue
					}
					if utils.MessageIsNotFoundStorageError(errPb.GetMsg()) {
						errMsg := fmt.Sprintf("File or directory not found error occurs on TiKV Node(store id: %v; Address: %s)", store.GetId(), redact.String(store.GetAddress()))
						log.Error("", zap.String("error", berrors.ErrKVStorage.Error()+": "+errMsg),
							zap.String("work around", "please ensure br and tikv node share a same disk and the user of br and tikv has same uid."))
					}
					if utils.MessageIsPermissionDeniedStorageError(errPb.GetMsg()) {
						errMsg := fmt.Sprintf("I/O permission denied error occurs on TiKV Node(store id: %v; Address: %s)", store.GetId(), redact.String(store.GetAddress()))
						log.Error("", zap.String("error", berrors.ErrKVStorage.Error()+": "+errMsg),
							zap.String("work around", "please ensure tikv has permission to read from & write to the storage."))
					}
					return res, errors.Annotatef(berrors.ErrKVStorage, "error happen in store %v at %s: %s",
						store.GetId(),
						redact.String(store.GetAddress()),
						errPb.Msg,
					)
				}
			}
		case err := <-push.errCh:
			if !berrors.Is(err, berrors.ErrFailedToConnect) {
				return res, errors.Annotatef(err, "failed to backup range [%s, %s)", redact.Key(req.StartKey), redact.Key(req.EndKey))
			}
			log.Warn("skipping disconnected stores", logutil.ShortError(err))
			return res, nil
		}
	}
}
