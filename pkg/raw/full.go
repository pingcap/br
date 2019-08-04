package raw

import (
	"context"
	"time"

	// "github.com/pingcap/kvproto/pkg/errorpb"

	"github.com/overvenus/br/pkg/meta"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"go.uber.org/zap"
)

// BackupClient is a client instructs TiKV how to do a backup.
type BackupClient struct {
	ctx    context.Context
	cancel context.CancelFunc

	backer    *meta.Backer
	clusterID uint64
	pdClient  pd.Client
}

// NewBackupClient returns a new backup client
func NewBackupClient(backer *meta.Backer) (*BackupClient, error) {
	log.Info("new backup client")
	ctx, cancel := context.WithCancel(backer.Context())
	pdClient := backer.GetPDClient()
	return &BackupClient{
		clusterID: pdClient.GetClusterID(ctx),
		backer:    backer,
		ctx:       ctx,
		cancel:    cancel,
		pdClient:  backer.GetPDClient(),
	}, nil
}

// BackupRange make a backup of the given key range.
func (bc *BackupClient) BackupRange(
	startKey, endKey []byte,
	path string,
) error {
	log.Info("full backup started")
	start := time.Now()
	ctx, cancel := context.WithCancel(bc.ctx)
	defer cancel()

	p, l, err := bc.pdClient.GetTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	ts := meta.Timestamp{
		Physical: p,
		Logical:  l,
	}
	backupTS := meta.EncodeTs(ts)
	allStores, err := bc.pdClient.GetAllStores(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	req := backup.BackupRequest{
		ClusterId:    bc.clusterID,
		StartKey:     startKey,
		EndKey:       endKey,
		StartVersion: backupTS,
		EndVersion:   backupTS,
		Path:         path,
	}
	push := newPushDown(ctx, bc.backer, len(allStores))
	if err := push.pushBackup(req, allStores...); err != nil {
		return err
	}
	log.Info("full backup finished",
		zap.Duration("take", time.Since(start)))
	return nil
}
