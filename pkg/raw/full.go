package raw

import (
	"bytes"
	"context"
	"sync"
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

// BackupClient is a client instructs TiKV how to do a backup.
type BackupClient struct {
	ctx    context.Context
	cancel context.CancelFunc

	clusterID uint64
	storeID   uint64
	client    backup.BackupClient
	pdClient  pd.Client
}

// NewBackupClient returns a new backup client
func NewBackupClient(backer *meta.Backer, storeID uint64) (*BackupClient, error) {
	client, err := backer.NewBackupClient(storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("new backup client", zap.Uint64("storeID", storeID))
	ctx, cancel := context.WithCancel(backer.Context())
	pdClient := backer.GetPDClient()
	return &BackupClient{
		clusterID: pdClient.GetClusterID(ctx),
		storeID:   storeID,
		ctx:       ctx,
		cancel:    cancel,
		client:    client,
		pdClient:  backer.GetPDClient(),
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
func (bc *BackupClient) Stop() (*backup.BackupResponse, error) {
	req := &backup.BackupRequest{
		ClusterId: bc.clusterID,
		State:     backup.BackupState_Stop,
	}
	resp, err := bc.client.Backup(bc.ctx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionErr, err := handleBackupError(resp, backup.BackupState_Stop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if regionErr != nil {
		return nil, errors.Errorf("%+v", regionErr)
	}
	bc.cancel()
	return resp, nil
}

// FullBackup make a full backup of a tikv cluster.
func (bc *BackupClient) FullBackup(concurrency, batch int) error {
	if err := bc.Start(); err != nil {
		return errors.Trace(err)
	}
	log.Info("full backup started")
	start := time.Now()

	tasksCh := make(chan []*metapb.Region, concurrency)
	errCh := make(chan error, concurrency)

	wg := sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasksCh {
				for _, region := range task {
					endKey := region.GetEndKey()
					for {
						needRetry, err := bc.BackupRegion(region)
						if err != nil {
							errCh <- err
							return
						}
						if needRetry {
							newRegion, _, err := bc.pdClient.GetRegion(bc.ctx, region.GetStartKey())
							if err != nil {
								errCh <- err
								return
							}
							log.Info("region changed", zap.Any("region", region), zap.Any("newRegion", newRegion))
							region = newRegion
							continue
						}
						if bytes.Compare(region.GetEndKey(), endKey) < 0 {
							newRegion, _, err := bc.pdClient.GetRegion(bc.ctx, region.GetEndKey())
							if err != nil {
								errCh <- err
								return
							}
							log.Info("region splitted", zap.Any("region", region), zap.Any("newRegion", newRegion))
							region = newRegion
							continue
						}
						break
					}
				}
			}
		}()
	}

	next := []byte("")
	started := false
	for len(next) != 0 || !started {
		regions, _, err := bc.ScanRegions(bc.ctx, next, batch)
		if err != nil {
			return errors.Trace(err)
		}
		if len(regions) == 0 {
			break
		}
		tasksCh <- regions
		next = regions[len(regions) - 1].GetEndKey()
		started = true
	}

	close(tasksCh)
	doneCh := make(chan bool, 1)
	go func () {
		wg.Wait()
		doneCh <- true
	}()
	select {
	case <-doneCh:
	case err := <-errCh:
		return err
	}
	resp, err := bc.Stop()
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("full backup finished",
		zap.Duration("take", time.Since(start)),
		zap.Uint64("dependence", resp.GetCurrentDependency()))
	return nil
}

// BackupRegion starts backup a region
// If it returns true with a nil error, caller needs to retry with the latest
// region.
func (bc *BackupClient) BackupRegion(region *metapb.Region) (bool, error) {
	log.Info("start backup region", zap.Any("region", region))
	// Try to find a backup peer.
	var peer *metapb.Peer
	for _, pr := range region.GetPeers() {
		if pr.GetStoreId() == bc.storeID {
			peer = pr
			break
		}
	}
	if peer == nil {
		return false, errors.Errorf("no backup peer in store %d %s",
			bc.storeID, region.GetPeers())
	}
	epoch := region.GetRegionEpoch()
	reqCtx := &kvrpcpb.Context{
		RegionId:    region.GetId(),
		RegionEpoch: epoch,
		Peer:        peer,
	}
	req := &backup.BackupRegionRequest{
		Context: reqCtx,
	}

	var regionErr *errorpb.Error
	var err error
	start := time.Now()
	for retry := 0; retry < 5; retry++ {
		var resp *backup.BackupRegionResponse
		resp, err = bc.client.BackupRegion(bc.ctx, req)
		if err != nil {
			backupRegionCounters.WithLabelValues("grpc_error").Inc()
			// TODO: handle region not found.
			// Just retry blindly.
			log.Warn("other error retry", zap.Error(err))
			// TODO: a better backoff
			time.Sleep(time.Second * 3)
			continue
		}
		regionErr, err = handleBackupError(resp, backup.BackupState_StartFullBackup)
		if err != nil {
			log.Warn("other error retry", zap.Error(err))
			backupRegionCounters.WithLabelValues("other_retry").Inc()
			// TODO: a better backoff
			time.Sleep(time.Second * 3)
			continue
		} else if regionErr != nil {
			log.Warn("region error retry", zap.Any("regioError", regionErr))
			enm := regionErr.GetEpochNotMatch()
			if enm != nil {
				backupRegionCounters.WithLabelValues("epoch_error").Inc()
				for _, r := range enm.GetCurrentRegions() {
					if r.GetId() == region.GetId() &&
						r.GetRegionEpoch().GetVersion() != epoch.GetVersion() {
						// Region's range has changed, caller need retry.
						return true, nil
					}
				}
				return false, errors.Errorf("%+v", regionErr)
			}
			backupRegionCounters.WithLabelValues("region_retry").Inc()
			// TODO: a better backoff
			time.Sleep(time.Second * 3)
			continue
		}
		break
	}
	if err != nil {
		return true, errors.Trace(err)
	}
	if regionErr != nil {
		return true, errors.Errorf("%s", regionErr)
	}
	dur := time.Since(start)
	backupRegionCounters.WithLabelValues("ok").Inc()
	backupRegionHistogram.Observe(dur.Seconds())
	log.Info("finish backup region",
		zap.Any("regionID", region.GetId()),
		zap.Duration("take", dur))
	return false, nil
}

// ScanRegions gets a list of regions, starts from the region that contains key.
// Limit limits the maximum number of regions returned.
func (bc *BackupClient) ScanRegions(
	ctx context.Context, key []byte, limit int,
) ([]*metapb.Region, []*metapb.Peer, error) {
	regions := make([]*metapb.Region, 0, limit)
	leaders := make([]*metapb.Peer, 0, limit)
	for i := 0; i < limit; i++ {
		region, leader, err := bc.pdClient.GetRegion(bc.ctx, key)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		regions = append(regions, region)
		leaders = append(leaders, leader)
		key = region.GetEndKey()
		if len(key) == 0 {
			break
		}
	}
	return regions, leaders, nil
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
