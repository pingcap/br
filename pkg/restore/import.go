package restore

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/codec"
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
	conn, err := grpc.Dial(store.GetAddress(), opt)
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

	ctx    context.Context
	cancel context.CancelFunc
}

// NewFileImporter returns a new file importClient.
func NewFileImporter(
	ctx context.Context,
	metaClient SplitClient,
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
func (importer *FileImporter) Import(file *backup.File, rewriteRules *RewriteRules) error {
	log.Debug("import file", zap.Stringer("file", file))
	// Rewrite the start key and end key of file to scan regions
	startKey, endKey, err := rewriteFileKeys(file, rewriteRules)
	if err != nil {
		return err
	}
	log.Debug("rewrite file keys",
		zap.Stringer("file", file),
		zap.Binary("startKey", startKey),
		zap.Binary("endKey", endKey))
	err = utils.WithRetry(importer.ctx, func() error {
		ctx, cancel := context.WithTimeout(importer.ctx, importScanRegionTime)
		defer cancel()
		// Scan regions covered by the file range
		regionInfos, err1 := paginateScanRegion(
			ctx, importer.metaClient, startKey, endKey, scanRegionPaginationLimit)
		if err1 != nil {
			return errors.Trace(err1)
		}
		log.Debug("scan regions", zap.Stringer("file", file), zap.Int("count", len(regionInfos)))
		// Try to download and ingest the file in every region
		for _, regionInfo := range regionInfos {
			info := regionInfo
			// Try to download file.
			var downloadMeta *import_sstpb.SSTMeta
			err1 = utils.WithRetry(importer.ctx, func() error {
				var e error
				downloadMeta, e = importer.downloadSST(info, file, rewriteRules)
				return e
			}, newDownloadSSTBackoffer())
			if err1 != nil {
				if err1 == errRewriteRuleNotFound || err1 == errRangeIsEmpty {
					// Skip this region
					continue
				}
				log.Error("download file failed",
					zap.Stringer("file", file),
					zap.Stringer("region", info.Region),
					zap.Binary("startKey", startKey),
					zap.Binary("endKey", endKey),
					zap.Error(err1))
				return err1
			}
			err1 = importer.ingestSST(downloadMeta, info)
			// If error is `NotLeader`, update the region info and retry
			for errors.Cause(err1) == errNotLeader {
				log.Debug("ingest sst returns not leader error, retry it",
					zap.Stringer("region", info.Region))
				var newInfo *RegionInfo
				newInfo, err1 = importer.metaClient.GetRegion(importer.ctx, info.Region.GetStartKey())
				if err1 != nil {
					break
				}
				if !checkRegionEpoch(newInfo, info) {
					err1 = errEpochNotMatch
					break
				}
				err1 = importer.ingestSST(downloadMeta, newInfo)
			}
			if err1 != nil {
				log.Error("ingest file failed",
					zap.Stringer("file", file),
					zap.Stringer("range", downloadMeta.GetRange()),
					zap.Stringer("region", info.Region),
					zap.Error(err1))
				return err1
			}
			summary.CollectSuccessUnit(summary.TotalKV, file.TotalKvs)
			summary.CollectSuccessUnit(summary.TotalBytes, file.TotalBytes)
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
		zap.Stringer("region", regionInfo.Region),
	)
	var resp *import_sstpb.DownloadResponse
	for _, peer := range regionInfo.Region.GetPeers() {
		resp, err = importer.importClient.DownloadSST(importer.ctx, peer.GetStoreId(), req)
		if err != nil {
			return nil, extractDownloadSSTError(err)
		}
		if resp.GetIsEmpty() {
			return nil, errors.Trace(errRangeIsEmpty)
		}
	}
	sstMeta.Range.Start = truncateTS(resp.Range.GetStart())
	sstMeta.Range.End = truncateTS(resp.Range.GetEnd())
	return &sstMeta, nil
}

func (importer *FileImporter) ingestSST(
	sstMeta *import_sstpb.SSTMeta,
	regionInfo *RegionInfo,
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
		if strings.Contains(err.Error(), "RegionNotFound") {
			return errors.Trace(errRegionNotFound)
		}
		return errors.Trace(err)
	}
	respErr := resp.GetError()
	if respErr != nil {
		log.Debug("ingest sst resp error", zap.Stringer("error", respErr))
		if respErr.GetKeyNotInRegion() != nil {
			return errors.Trace(errKeyNotInRegion)
		}
		if respErr.GetNotLeader() != nil {
			return errors.Trace(errNotLeader)
		}
		return errors.Wrap(errResp, respErr.String())
	}
	return nil
}

func checkRegionEpoch(new, old *RegionInfo) bool {
	if new.Region.GetId() == old.Region.GetId() &&
		new.Region.GetRegionEpoch().GetVersion() == old.Region.GetRegionEpoch().GetVersion() &&
		new.Region.GetRegionEpoch().GetConfVer() == old.Region.GetRegionEpoch().GetConfVer() {
		return true
	}
	return false
}

func extractDownloadSSTError(e error) error {
	err := errGrpc
	switch {
	case strings.Contains(e.Error(), "bad format"):
		err = errBadFormat
	case strings.Contains(e.Error(), "wrong prefix"):
		err = errWrongKeyPrefix
	case strings.Contains(e.Error(), "corrupted"):
		err = errFileCorrupted
	case strings.Contains(e.Error(), "Cannot read"):
		err = errCannotRead
	}
	return errors.Trace(err)
}
