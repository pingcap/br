package raw

import (
	"context"
	"sync"

	// "github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/br/pkg/meta"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// pushDown warps a backup task.
type pushDown struct {
	ctx    context.Context
	backer *meta.Backer
	respCh chan *backup.BackupResponse
	errCh  chan error
}

// newPushDown creates a push down backup.
func newPushDown(ctx context.Context, backer *meta.Backer, cap int) *pushDown {
	log.Info("new backup client")
	return &pushDown{
		ctx:    ctx,
		backer: backer,
		respCh: make(chan *backup.BackupResponse, cap),
		errCh:  make(chan error, cap),
	}
}

// FullBackup make a full backup of a tikv cluster.
func (push *pushDown) pushBackup(
	req backup.BackupRequest,
	stores ...*metapb.Store,
) (RangeTree, error) {
	// Push down backup tasks to all tikv instances.
	wg := sync.WaitGroup{}
	for _, s := range stores {
		storeID := s.GetId()
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := push.backer.SendBackup(
				push.ctx, storeID, req,
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

	res := newRangeTree()
	for {
		select {
		case resp, ok := <-push.respCh:
			if !ok {
				// Finished.
				return res, nil
			}
			if resp.GetError() == nil {
				// None error means range has been backuped successfully.
				res.putOk(
					resp.GetStartKey(), resp.GetEndKey(), resp.GetFiles())
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
