package raw

import (
	"context"
	"io"
	"sync"

	// "github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/google/btree"
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

// FullBackup make a full backup of a tikv cluster.
func (push *pushDown) pushBackup(
	req backup.BackupRequest,
	stores ...*metapb.Store,
) error {
	log.Info("full backup started")

	// Push down backup tasks to all tikv instances.
	wg := sync.WaitGroup{}
	for _, s := range stores {
		client, err := push.backer.NewBackupClient(s.GetId())
		if err != nil {
			return errors.Trace(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("try backup", zap.Any("backup request", req))
			bcli, err := client.Backup(push.ctx, &req)
			if err != nil {
				push.errCh <- errors.Trace(err)
			}
			for {
				resp, err := bcli.Recv()
				if err != nil {
					if err == io.EOF {
						break
					}
					push.errCh <- errors.Trace(err)
				}
				// TODO: handle errors in the resp.
				log.Info("range backuped",
					zap.Any("StartKey", resp.GetStartKey()),
					zap.Any("EndKey", resp.GetEndKey()))
				push.respCh <- resp
			}
			if err != nil {
				push.errCh <- err
			}
		}()
	}

	doneCh := make(chan bool, 1)
	go func() {
		wg.Wait()
		doneCh <- true
	}()
	bmap := btree.New(64)
	for {
		select {
		case <-doneCh:
			return nil
		case resp := <-push.respCh:
			// TODO: Insert resp into the bmap, we need to make sure backup
			//       covers the whole range.
			_ = resp
			_ = bmap
		case err := <-push.errCh:
			return errors.Trace(err)
		}
	}
}
