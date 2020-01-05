package restore

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/codec"
	restore_util "github.com/pingcap/tidb-tools/pkg/restore-util"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/br/pkg/summary"
)

var (
	errNotLeader           = errors.New("not leader")
	errEpochNotMatch       = errors.New("epoch not match")
	errRewriteRuleNotFound = errors.New("rewrite rule not found")
	errRangeIsEmpty        = errors.New("range is empty")
)

const (
	importScanResgionTime     = 10 * time.Second
	importFileRetryTimes      = 16
	importFileWaitInterval    = 10 * time.Millisecond
	importFileMaxWaitInterval = 1 * time.Second

	downloadSSTRetryTimes      = 8
	downloadSSTWaitInterval    = 10 * time.Millisecond
	downloadSSTMaxWaitInterval = 1 * time.Second
)

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
	metaClient restore_util.Client
	clients    map[uint64]import_sstpb.ImportSSTClient
}

// NewImportClient returns a new ImporterClient
func NewImportClient(metaClient restore_util.Client) ImporterClient {
	return &importClient{
		metaClient: metaClient,
		clients:    make(map[uint64]import_sstpb.ImportSSTClient),
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
	conn, err := grpc.Dial(store.GetAddress(), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client = import_sstpb.NewImportSSTClient(conn)
	ic.clients[storeID] = client
	return client, err
}

// FileImporter used to import a file to TiKV.
type FileImporter struct {
	metaClient   restore_util.Client
	importClient ImporterClient
	backend      *backup.StorageBackend
	rateLimit    uint64

	ctx    context.Context
	cancel context.CancelFunc
}

// NewFileImporter returns a new file importClient.
func NewFileImporter(
	ctx context.Context,
	metaClient restore_util.Client,
	importClient ImporterClient,
	backend *backup.StorageBackend,
	rateLimit uint64,
) FileImporter {
	ctx, cancel := context.WithCancel(ctx)
	return FileImporter{
		metaClient:   metaClient,
		backend:      backend,
		ctx:          ctx,
		cancel:       cancel,
		importClient: importClient,
		rateLimit:    rateLimit,
	}
}

// Import tries to import a file.
// All rules must contain encoded keys.
func (importer *FileImporter) Import(file *backup.File, rewriteRules *restore_util.RewriteRules) error {
	log.Debug("import file", zap.Stringer("file", file))
	// Rewrite the start key and end key of file to scan regions
	startKey, endKey, err := rewriteFileKeys(file, rewriteRules)
	if err != nil {
		return err
	}
	log.Debug("rewrite file keys",
		zap.Stringer("file", file),
		zap.ByteString("startKey", startKey),
		zap.ByteString("endKey", endKey),
	)
	err = withRetry(func() error {
		ctx, cancel := context.WithTimeout(importer.ctx, importScanResgionTime)
		defer cancel()
		// Scan regions covered by the file range
		regionInfos, err := importer.metaClient.ScanRegions(ctx, startKey, endKey, 0)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("scan regions", zap.Stringer("file", file), zap.Int("count", len(regionInfos)))
		// Try to download and ingest the file in every region
		for _, regionInfo := range regionInfos {
			var downloadMeta *import_sstpb.SSTMeta
			info := regionInfo
			// Try to download file.
			err = withRetry(func() error {
				var err error
				var isEmpty bool
				downloadMeta, isEmpty, err = importer.downloadSST(info, file, rewriteRules)
				if err != nil {
					if err != errRewriteRuleNotFound {
						log.Warn("download file failed",
							zap.Stringer("file", file),
							zap.Stringer("region", info.Region),
							zap.ByteString("startKey", startKey),
							zap.ByteString("endKey", endKey),
							zap.Error(err),
						)
					}
					return err
				}
				if isEmpty {
					log.Info(
						"file don't have any key in this region, skip it",
						zap.Stringer("file", file),
						zap.Stringer("region", info.Region),
					)
					return errRangeIsEmpty
				}
				return nil
			}, func(e error) bool {
				// Scan regions may return some regions which cannot match any rewrite rule,
				// like [t{tableID}, t{tableID}_r), those regions should be skipped
				return e != errRewriteRuleNotFound && e != errRangeIsEmpty
			}, downloadSSTRetryTimes, downloadSSTWaitInterval, downloadSSTMaxWaitInterval)
			if err != nil {
				if err == errRewriteRuleNotFound || err == errRangeIsEmpty {
					// Skip this region
					continue
				}
				return err
			}
			err = importer.ingestSST(downloadMeta, info)
			if err != nil {
				log.Warn("ingest file failed",
					zap.Stringer("file", file),
					zap.Stringer("range", downloadMeta.GetRange()),
					zap.Stringer("region", info.Region),
					zap.Error(err),
				)
				return err
			}
			summary.CollectSuccessUnit(summary.TotalKV, file.TotalKvs)
			summary.CollectSuccessUnit(summary.TotalBytes, file.TotalBytes)
		}
		return nil
	}, func(e error) bool {
		return true
	}, importFileRetryTimes, importFileWaitInterval, importFileMaxWaitInterval)
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
	regionInfo *restore_util.RegionInfo,
	file *backup.File,
	rewriteRules *restore_util.RewriteRules,
) (*import_sstpb.SSTMeta, bool, error) {
	id, err := uuid.New().MarshalBinary()
	if err != nil {
		return nil, true, errors.Trace(err)
	}
	// Assume one region reflects to one rewrite rule
	_, key, err := codec.DecodeBytes(regionInfo.Region.GetStartKey())
	if err != nil {
		return nil, true, err
	}
	regionRule := matchNewPrefix(key, rewriteRules)
	if regionRule == nil {
		log.Debug("cannot find rewrite rule, skip region",
			zap.Stringer("region", regionInfo.Region),
			zap.Array("tableRule", rules(rewriteRules.Table)),
			zap.Array("dataRule", rules(rewriteRules.Data)),
			zap.ByteString("key", key),
		)
		return nil, true, errRewriteRuleNotFound
	}
	rule := import_sstpb.RewriteRule{
		OldKeyPrefix: encodeKeyPrefix(regionRule.GetOldKeyPrefix()),
		NewKeyPrefix: encodeKeyPrefix(regionRule.GetNewKeyPrefix()),
	}
	sstMeta := getSSTMetaFromFile(id, file, regionInfo.Region, &rule)
	sstMeta.RegionId = regionInfo.Region.GetId()
	sstMeta.RegionEpoch = regionInfo.Region.GetRegionEpoch()
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
			return nil, true, err
		}
		if resp.GetIsEmpty() {
			return &sstMeta, true, nil
		}
	}
	sstMeta.Range.Start = truncateTS(resp.Range.GetStart())
	sstMeta.Range.End = truncateTS(resp.Range.GetEnd())
	return &sstMeta, false, nil
}

func (importer *FileImporter) ingestSST(
	sstMeta *import_sstpb.SSTMeta,
	regionInfo *restore_util.RegionInfo,
) error {
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
	log.Debug("download SST", zap.Stringer("sstMeta", sstMeta))
	resp, err := importer.importClient.IngestSST(importer.ctx, leader.GetStoreId(), req)
	if err != nil {
		return err
	}
	respErr := resp.GetError()
	if respErr != nil {
		if respErr.EpochNotMatch != nil {
			return errEpochNotMatch
		}
		if respErr.NotLeader != nil {
			return errNotLeader
		}
		return errors.Errorf("ingest failed: %v", respErr)
	}
	return nil
}
