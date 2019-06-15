package raw

import (
	"context"
	"fmt"
	"time"

	"github.com/overvenus/br/pkg/meta"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"go.uber.org/zap"
)

type BackupClient struct {
	ctx    context.Context
	cancel context.CancelFunc

	clusterID uint64
	storeID   uint64
	client    backup.BackupClient
	pdClient  pd.Client
}

func NewBackupClient(backer *meta.Backer, storeID uint64) (*BackupClient, error) {
	client, err := backer.NewBackupClient(storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ctx, cancel := context.WithCancel(backer.Context())
	return &BackupClient{
		ctx:      ctx,
		cancel:   cancel,
		client:   client,
		pdClient: backer.GetPDClient(),
	}, nil
}

// Start starts backup.
func (bc *BackupClient) Start() error {
	req := &backup.BackupRequest{
		ClusterId: bc.clusterID,
		State:     backup.BackupState_StartFullBackup,
	}
	resp, err := bc.client.Backup(bc.ctx, req)
	if err != nil {
		return errors.Trace(err)
	}
	regionErr, err :=
		handleBackupError(resp, backup.BackupState_StartFullBackup)
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		return errors.Errorf("%+v", regionErr)
	}
	return nil
}

// Stop starts backup.
func (bc *BackupClient) Stop() error {
	req := &backup.BackupRequest{
		ClusterId: bc.clusterID,
		State:     backup.BackupState_Stop,
	}
	resp, err := bc.client.Backup(bc.ctx, req)
	if err != nil {
		return errors.Trace(err)
	}
	regionErr, err := handleBackupError(resp, backup.BackupState_Stop)
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		return errors.Errorf("%+v", regionErr)
	}
	bc.cancel()
	return nil
}

// FullBackup make a full backup of a tikv cluster.
func (bc *BackupClient) FullBackup() error {
	if err := bc.Start(); err != nil {
		return errors.Trace(err)
	}

	next := []byte("")
	started := false
	for len(next) != 0 || !started {
		region, _, err := bc.pdClient.GetRegion(bc.ctx, next)
		if err != nil {
			return errors.Trace(err)
		}
		fmt.Println(region)
		next = region.EndKey
		started = true
	}
	return nil
}

// BackupRegion starts backup a region
func (bc *BackupClient) BackupRegion(region *metapb.Region) error {
	// Try to find a backup peer.
	var peer *metapb.Peer
	for _, pr := range region.GetPeers() {
		if pr.GetStoreId() == bc.storeID {
			peer = pr
			break
		}
	}
	if peer == nil {
		return errors.Errorf("no backup peer %+v", region)
	}

	reqCtx := &kvrpcpb.Context{
		RegionId:    region.GetId(),
		RegionEpoch: region.GetRegionEpoch(),
		Peer:        peer,
	}
	req := &backup.BackupRegionRequest{
		Context: reqCtx,
	}

	for retry := 0; retry < 3; retry++ {
		resp, err := bc.client.BackupRegion(bc.ctx, req)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := handleBackupError(resp, backup.BackupState_StartFullBackup)
		if err != nil {
			log.Warn("gRPC error retry", zap.Error(err))
			// TODO: a better backoff
			time.Sleep(time.Second * 3)
			continue
		} else if regionErr != nil {
			log.Warn("region error retry", zap.Any("regioError", regionErr))
			if regionErr.GetEpochNotMatch() != nil {
				return errors.Errorf("%+v", regionErr)
			}
			// TODO: a better backoff
			time.Sleep(time.Second * 3)
			continue
		}
	}
	return nil
}

type getError interface {
	GetError() *backup.Error
}

func handleBackupError(
	err getError,
	expectState backup.BackupState,
) (*errorpb.Error, error) {
	errResp := err.GetError()
	if errResp != nil {
		switch errResp.GetDetail().(type) {
		case *backup.Error_ClusterIdError:
			return nil, errors.Errorf("%+v", errResp)
		case *backup.Error_RegionError:
			return errResp.GetDetail().(*backup.Error_RegionError).RegionError, nil
		case *backup.Error_StateStepError:
			stepErr := errResp.GetDetail().(*backup.Error_StateStepError)
			if stepErr.StateStepError.GetCurrent() != expectState {
				return nil, errors.Errorf("%+v", errResp)
			}
		}
	}
	return nil, nil
}
