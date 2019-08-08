package raw

import (
	"context"
	"io"
	"sync"

	// "github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/overvenus/br/pkg/meta"
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

type result struct {
	ok  RangeTree
	err RangeTree
}

// FullBackup make a full backup of a tikv cluster.
func (push *pushDown) pushBackup(
	req backup.BackupRequest,
	stores ...*metapb.Store,
) (result, error) {
	// Push down backup tasks to all tikv instances.
	wg := sync.WaitGroup{}
	for _, s := range stores {
		client, err := push.backer.NewBackupClient(s.GetId())
		if err != nil {
			return result{}, errors.Trace(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("try backup", zap.Any("backup request", req))
			bcli, err := client.Backup(push.ctx, &req)
			if err != nil {
				push.errCh <- errors.Trace(err)
				return
			}
			for {
				resp, err := bcli.Recv()
				if err != nil {
					if err == io.EOF {
						break
					}
					push.errCh <- errors.Trace(err)
					return
				}
				// TODO: handle errors in the resp.
				log.Info("range backuped",
					zap.Any("StartKey", resp.GetStartKey()),
					zap.Any("EndKey", resp.GetEndKey()))
				push.respCh <- resp
			}
		}()
	}

	doneCh := make(chan bool, 1)
	go func() {
		wg.Wait()
		doneCh <- true
	}()

	res := result{
		ok:  newRangeTree(),
		err: newRangeTree(),
	}
	for {
		select {
		case <-doneCh:
			return res, nil
		case resp := <-push.respCh:
			// TODO: we need to make sure backup covers the whole range.
			if errPb := resp.GetError(); errPb != nil {
				switch v := errPb.Detail.(type) {
				case *backup.Error_KvError:
					log.Error("backup occur kv error", zap.Reflect("error", v))
					res.err.putErr(resp.GetStartKey(), resp.GetEndKey(),
						resp.GetError())

				case *backup.Error_RegionError:
					log.Error("backup occur region error",
						zap.Reflect("error", v))
					res.err.putErr(resp.GetStartKey(), resp.GetEndKey(),
						resp.GetError())

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
			res.ok.putOk(resp.GetStartKey(), resp.GetEndKey(), resp.GetFiles())
		case err := <-push.errCh:
			return res, errors.Trace(err)
		}
	}
}
