// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v3/pkg/codec"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

const importScanRegionTime = 10 * time.Second
const scanRegionPaginationLimit = int(128)

// ImporterClient is used to import a file to TiKV
type ImporterClient interface {
	DownloadSST(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.DownloadRequest,
	) (*import_sstpb.DownloadResponse, error)

	IngestSST(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.IngestRequest,
	) (*import_sstpb.IngestResponse, error)

	SetDownloadSpeedLimit(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.SetDownloadSpeedLimitRequest,
	) (*import_sstpb.SetDownloadSpeedLimitResponse, error)
}

type importClient struct {
	mu         sync.Mutex
	metaClient SplitClient
	clients    map[uint64]import_sstpb.ImportSSTClient
	tlsConf    *tls.Config
}

// NewImportClient returns a new ImporterClient
func NewImportClient(metaClient SplitClient, tlsConf *tls.Config) ImporterClient {
	return &importClient{
		metaClient: metaClient,
		clients:    make(map[uint64]import_sstpb.ImportSSTClient),
		tlsConf:    tlsConf,
	}
}

func (ic *importClient) DownloadSST(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.DownloadRequest,
) (*import_sstpb.DownloadResponse, error) {
	client, err := ic.getImportClient(ctx, storeID)
	if err != nil {
		return nil, err
	}
	return client.Download(ctx, req)
}

func (ic *importClient) SetDownloadSpeedLimit(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.SetDownloadSpeedLimitRequest,
) (*import_sstpb.SetDownloadSpeedLimitResponse, error) {
	client, err := ic.getImportClient(ctx, storeID)
	if err != nil {
		return nil, err
	}
	return client.SetDownloadSpeedLimit(ctx, req)
}

func (ic *importClient) IngestSST(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.IngestRequest,
) (*import_sstpb.IngestResponse, error) {
	client, err := ic.getImportClient(ctx, storeID)
	if err != nil {
		return nil, err
	}
	return client.Ingest(ctx, req)
}

func (ic *importClient) getImportClient(
	ctx context.Context,
	storeID uint64,
) (import_sstpb.ImportSSTClient, error) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	client, ok := ic.clients[storeID]
	if ok {
		return client, nil
	}
	store, err := ic.metaClient.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	opt := grpc.WithInsecure()
	if ic.tlsConf != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(ic.tlsConf))
	}
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	conn, err := grpc.Dial(addr, opt)
	if err != nil {
		return nil, err
	}
	client = import_sstpb.NewImportSSTClient(conn)
	ic.clients[storeID] = client
	return client, err
}

// FileImporter used to import a file to TiKV.
type FileImporter struct {
	metaClient   SplitClient
	importClient ImporterClient
	backend      *backup.StorageBackend
	rateLimit    uint64

	isRawKvMode bool
	rawStartKey []byte
	rawEndKey   []byte

	ctx    context.Context
	cancel context.CancelFunc
}

// NewFileImporter returns a new file importClient.
func NewFileImporter(
	ctx context.Context,
	metaClient SplitClient,
	importClient ImporterClient,
	backend *backup.StorageBackend,
	isRawKvMode bool,
	rateLimit uint64,
) FileImporter {
	ctx, cancel := context.WithCancel(ctx)
	return FileImporter{
		metaClient:   metaClient,
		backend:      backend,
		ctx:          ctx,
		cancel:       cancel,
		importClient: importClient,
		isRawKvMode:  isRawKvMode,
		rateLimit:    rateLimit,
	}
}

// SetRawRange sets the range to be restored in raw kv mode.
func (importer *FileImporter) SetRawRange(startKey, endKey []byte) error {
	if !importer.isRawKvMode {
		return errors.New("file importer is not in raw kv mode")
	}
	importer.rawStartKey = startKey
	importer.rawEndKey = endKey
	return nil
}

// Import tries to import a file.
// All rules must contain encoded keys.
func (importer *FileImporter) Import(
	file *backup.File,
	rejectStoreMap map[uint64]bool,
	rewriteRules *RewriteRules,
) error {
	log.Debug("import file", zap.Stringer("file", file))
	// Rewrite the start key and end key of file to scan regions
	var startKey, endKey []byte
	var err error
	if importer.isRawKvMode {
		startKey = file.StartKey
		endKey = file.EndKey
	} else {
		startKey, endKey, err = rewriteFileKeys(file, rewriteRules)
		// if not truncateRowKey here, if will scan one more region
		// TODO need more test to check here
		// startKey = truncateRowKey(startKey)
		// endKey = truncateRowKey(endKey)
	}
	if err != nil {
		return err
	}
	log.Debug("rewrite file keys",
		zap.Stringer("file", file),
		zap.Binary("startKey", startKey),
		zap.Binary("endKey", endKey))

	needReject := len(rejectStoreMap) > 0

	err = utils.WithRetry(importer.ctx, func() error {
		ctx, cancel := context.WithTimeout(importer.ctx, importScanRegionTime)
		defer cancel()
		// Scan regions covered by the file range
		regionInfos, errScanRegion := paginateScanRegion(
			ctx, importer.metaClient, startKey, endKey, scanRegionPaginationLimit)
		if errScanRegion != nil {
			return errors.Trace(errScanRegion)
		}

		if needReject {
			// TODO remove when TiFlash support restore
			startTime := time.Now()
			log.Info("start to wait for removing rejected stores", zap.Reflect("rejectStores", rejectStoreMap))
			for _, region := range regionInfos {
				if !waitForRemoveRejectStores(ctx, importer.metaClient, region, rejectStoreMap) {
					log.Error("waiting for removing rejected stores failed",
						zap.Stringer("region", region.Region))
					return errors.New("waiting for removing rejected stores failed")
				}
			}
			log.Info("waiting for removing rejected stores done",
				zap.Int("regions", len(regionInfos)), zap.Duration("take", time.Since(startTime)))
			needReject = false
		}

		log.Debug("scan regions", zap.Stringer("file", file), zap.Int("count", len(regionInfos)))
		// Try to download and ingest the file in every region
	regionLoop:
		for _, regionInfo := range regionInfos {
			info := regionInfo
			// Try to download file.
			var downloadMeta *import_sstpb.SSTMeta
			errDownload := utils.WithRetry(importer.ctx, func() error {
				var e error
				if importer.isRawKvMode {
					downloadMeta, e = importer.downloadRawKVSST(info, file)
				} else {
					downloadMeta, e = importer.downloadSST(info, file, rewriteRules)
				}
				return e
			}, newDownloadSSTBackoffer())
			if errDownload != nil {
				for _, e := range multierr.Errors(errDownload) {
					switch e {
					case errRewriteRuleNotFound, errRangeIsEmpty:
						// Skip this region
						log.Warn("download file skipped",
							zap.Stringer("file", file),
							zap.Stringer("region", info.Region),
							zap.Binary("startKey", startKey),
							zap.Binary("endKey", endKey),
							zap.Error(errDownload))
						continue regionLoop
					}
				}
				log.Error("download file failed",
					zap.Stringer("file", file),
					zap.Stringer("region", info.Region),
					zap.Binary("startKey", startKey),
					zap.Binary("endKey", endKey),
					zap.Error(errDownload))
				return errDownload
			}

			ingestResp, errIngest := importer.ingestSST(downloadMeta, info)
		ingestRetry:
			for errIngest == nil {
				errPb := ingestResp.GetError()
				if errPb == nil {
					// Ingest success
					break ingestRetry
				}
				switch {
				case errPb.NotLeader != nil:
					// If error is `NotLeader`, update the region info and retry
					var newInfo *RegionInfo
					if newLeader := errPb.GetNotLeader().GetLeader(); newLeader != nil {
						newInfo = &RegionInfo{
							Leader: newLeader,
							Region: info.Region,
						}
					} else {
						// Slow path, get region from PD
						newInfo, errIngest = importer.metaClient.GetRegion(
							importer.ctx, info.Region.GetStartKey())
						if errIngest != nil {
							break ingestRetry
						}
					}
					log.Debug("ingest sst returns not leader error, retry it",
						zap.Stringer("region", info.Region),
						zap.Stringer("newLeader", newInfo.Leader))

					if !checkRegionEpoch(newInfo, info) {
						errIngest = errors.AddStack(errEpochNotMatch)
						break ingestRetry
					}
					ingestResp, errIngest = importer.ingestSST(downloadMeta, newInfo)
				case errPb.EpochNotMatch != nil:
					// TODO handle epoch not match error
					//      1. retry download if needed
					//      2. retry ingest
					errIngest = errors.AddStack(errEpochNotMatch)
					break ingestRetry
				case errPb.KeyNotInRegion != nil:
					errIngest = errors.AddStack(errKeyNotInRegion)
					break ingestRetry
				default:
					// Other errors like `ServerIsBusy`, `RegionNotFound`, etc. should be retryable
					errIngest = errors.Annotatef(errIngestFailed, "ingest error %s", errPb)
					break ingestRetry
				}
			}

			if errIngest != nil {
				log.Error("ingest file failed",
					zap.Stringer("file", file),
					zap.Stringer("range", downloadMeta.GetRange()),
					zap.Stringer("region", info.Region),
					zap.Error(errIngest))
				return errIngest
			}
			summary.CollectSuccessUnit(summary.TotalKV, 1, file.TotalKvs)
			summary.CollectSuccessUnit(summary.TotalBytes, 1, file.TotalBytes)
		}
		return nil
	}, newImportSSTBackoffer())
	return err
}

func (importer *FileImporter) setDownloadSpeedLimit(storeID uint64) error {
	req := &import_sstpb.SetDownloadSpeedLimitRequest{
		SpeedLimit: importer.rateLimit,
	}
	_, err := importer.importClient.SetDownloadSpeedLimit(importer.ctx, storeID, req)
	return err
}

func (importer *FileImporter) downloadSST(
	regionInfo *RegionInfo,
	file *backup.File,
	rewriteRules *RewriteRules,
) (*import_sstpb.SSTMeta, error) {
	id, err := uuid.New().MarshalBinary()
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Assume one region reflects to one rewrite rule
	_, key, err := codec.DecodeBytes(regionInfo.Region.GetStartKey())
	if err != nil {
		return nil, err
	}
	regionRule := matchNewPrefix(key, rewriteRules)
	if regionRule == nil {
		return nil, errors.Trace(errRewriteRuleNotFound)
	}
	rule := import_sstpb.RewriteRule{
		OldKeyPrefix: encodeKeyPrefix(regionRule.GetOldKeyPrefix()),
		NewKeyPrefix: encodeKeyPrefix(regionRule.GetNewKeyPrefix()),
	}
	sstMeta := getSSTMetaFromFile(id, file, regionInfo.Region, &rule)

	req := &import_sstpb.DownloadRequest{
		Sst:            sstMeta,
		StorageBackend: importer.backend,
		Name:           file.GetName(),
		RewriteRule:    rule,
	}
	log.Debug("download SST",
		zap.Stringer("sstMeta", &sstMeta),
		zap.Stringer("file", file),
		zap.Stringer("region", regionInfo.Region),
	)
	var resp *import_sstpb.DownloadResponse
	for _, peer := range regionInfo.Region.GetPeers() {
		resp, err = importer.importClient.DownloadSST(importer.ctx, peer.GetStoreId(), req)
		if err != nil {
			return nil, errors.Annotatef(errGrpc, "%s", err)
		}
		if resp.GetError() != nil {
			return nil, errors.Annotate(errDownloadFailed, resp.GetError().GetMessage())
		}
		if resp.GetIsEmpty() {
			return nil, errors.Trace(errRangeIsEmpty)
		}
	}
	sstMeta.Range.Start = truncateTS(resp.Range.GetStart())
	sstMeta.Range.End = truncateTS(resp.Range.GetEnd())
	return &sstMeta, nil
}

func (importer *FileImporter) downloadRawKVSST(
	regionInfo *RegionInfo,
	file *backup.File,
) (*import_sstpb.SSTMeta, error) {
	id, err := uuid.New().MarshalBinary()
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Empty rule
	var rule import_sstpb.RewriteRule
	sstMeta := getSSTMetaFromFile(id, file, regionInfo.Region, &rule)

	// Cut the SST file's range to fit in the restoring range.
	if bytes.Compare(importer.rawStartKey, sstMeta.Range.GetStart()) > 0 {
		sstMeta.Range.Start = importer.rawStartKey
	}
	// TODO: importer.RawEndKey is exclusive but sstMeta.Range.End is inclusive. How to exclude importer.RawEndKey?
	if len(importer.rawEndKey) > 0 && bytes.Compare(importer.rawEndKey, sstMeta.Range.GetEnd()) < 0 {
		sstMeta.Range.End = importer.rawEndKey
	}
	if bytes.Compare(sstMeta.Range.GetStart(), sstMeta.Range.GetEnd()) > 0 {
		return nil, errors.Trace(errRangeIsEmpty)
	}

	req := &import_sstpb.DownloadRequest{
		Sst:            sstMeta,
		StorageBackend: importer.backend,
		Name:           file.GetName(),
		RewriteRule:    rule,
	}
	log.Debug("download SST",
		zap.Stringer("sstMeta", &sstMeta),
		zap.Stringer("region", regionInfo.Region),
	)
	var resp *import_sstpb.DownloadResponse
	for _, peer := range regionInfo.Region.GetPeers() {
		resp, err = importer.importClient.DownloadSST(importer.ctx, peer.GetStoreId(), req)
		if err != nil {
			return nil, errors.Annotatef(errGrpc, "%s", err)
		}
		if resp.GetError() != nil {
			return nil, errors.Annotate(errDownloadFailed, resp.GetError().GetMessage())
		}
		if resp.GetIsEmpty() {
			return nil, errors.Trace(errRangeIsEmpty)
		}
	}
	sstMeta.Range.Start = resp.Range.GetStart()
	sstMeta.Range.End = resp.Range.GetEnd()
	return &sstMeta, nil
}

func (importer *FileImporter) ingestSST(
	sstMeta *import_sstpb.SSTMeta,
	regionInfo *RegionInfo,
) (*import_sstpb.IngestResponse, error) {
	leader := regionInfo.Leader
	if leader == nil {
		leader = regionInfo.Region.GetPeers()[0]
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    regionInfo.Region.GetId(),
		RegionEpoch: regionInfo.Region.GetRegionEpoch(),
		Peer:        leader,
	}
	req := &import_sstpb.IngestRequest{
		Context: reqCtx,
		Sst:     sstMeta,
	}
	log.Debug("ingest SST", zap.Stringer("sstMeta", sstMeta), zap.Reflect("leader", leader))
	resp, err := importer.importClient.IngestSST(importer.ctx, leader.GetStoreId(), req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func checkRegionEpoch(new, old *RegionInfo) bool {
	if new.Region.GetId() == old.Region.GetId() &&
		new.Region.GetRegionEpoch().GetVersion() == old.Region.GetRegionEpoch().GetVersion() &&
		new.Region.GetRegionEpoch().GetConfVer() == old.Region.GetRegionEpoch().GetConfVer() {
		return true
	}
	return false
}
