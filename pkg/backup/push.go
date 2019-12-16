package backup

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// pushDown warps a backup task.
type pushDown struct {
	ctx    context.Context
	mgr    ClientMgr
	respCh chan *backup.BackupResponse
	errCh  chan error
}

// newPushDown creates a push down backup.
func newPushDown(ctx context.Context, mgr ClientMgr, cap int) *pushDown {
	log.Info("new backup client")
	return &pushDown{
		ctx:    ctx,
		mgr:    mgr,
		respCh: make(chan *backup.BackupResponse, cap),
		errCh:  make(chan error, cap),
	}
}

// FullBackup make a full backup of a tikv cluster.
func (push *pushDown) pushBackup(
	req backup.BackupRequest,
	stores []*metapb.Store,
	updateCh chan<- struct{},
) (RangeTree, error) {
	// Push down backup tasks to all tikv instances.
	res := newRangeTree()
	wg := new(sync.WaitGroup)
	for _, s := range stores {
		storeID := s.GetId()
		if s.GetState() != metapb.StoreState_Up {
			log.Warn("skip store", zap.Uint64("StoreID", storeID), zap.Stringer("State", s.GetState()))
			continue
		}
		client, err := push.mgr.GetBackupClient(push.ctx, storeID)
		if err != nil {
			log.Error("fail to connect store", zap.Uint64("StoreID", storeID))
			return res, errors.Trace(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := SendBackup(
				push.ctx, storeID, client, req,
				func(resp *backup.BackupResponse) error {
					// Forward all responses (including error).
					push.respCh <- resp
					return nil
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
				res.put(
					resp.GetStartKey(), resp.GetEndKey(), resp.GetFiles())

				// Update progress
				updateCh <- struct{}{}
			} else {
				errPb := resp.GetError()
				switch v := errPb.Detail.(type) {
				case *backup.Error_KvError:
					log.Error("backup occur kv error", zap.Reflect("error", v))

				case *backup.Error_RegionError:
					log.Error("backup occur region error",
						zap.Reflect("error", v))

				case *backup.Error_ClusterIdError:
					log.Error("backup occur cluster ID error",
						zap.Reflect("error", v))
					return res, errors.Errorf("%v", errPb)

				default:
					log.Error("backup occur unknown error",
						zap.String("error", errPb.GetMsg()))
					return res, errors.Errorf("%v", errPb)
				}
			}
		case err := <-push.errCh:
			return res, errors.Trace(err)
		}
	}
}
