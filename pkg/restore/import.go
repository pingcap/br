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
	restore_util "github.com/pingcap/tidb-tools/pkg/restore-util"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	errNotLeader           = errors.New("not leader")
	errEpochNotMatch       = errors.New("epoch not match")
	errRewriteRuleNotFound = errors.New("rewrite rule not found")
	errRangeIsEmpty        = errors.New("range is empty")
)

const (
	importScanRegionTime      = 10 * time.Second
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
	fileURL      string

	ctx    context.Context
	cancel context.CancelFunc
}

// NewFileImporter returns a new file importClient.
func NewFileImporter(
	ctx context.Context,
	metaClient restore_util.Client,
	importClient ImporterClient,
	fileURL string,
) FileImporter {
	ctx, cancel := context.WithCancel(ctx)
	return FileImporter{
		metaClient:   metaClient,
		fileURL:      fileURL,
		ctx:          ctx,
		cancel:       cancel,
		importClient: importClient,
	}
}

// Import tries to import a file.
// All rules must contain encoded keys.
func (importer *FileImporter) Import(file *backup.File, rewriteRules *restore_util.RewriteRules) error {
	// Rewrite the start key and end key of file to scan regions
	scanStartKey, ok := rewriteRawKeyWithNewPrefix(file.GetStartKey(), rewriteRules)
	if !ok {
		log.Error("cannot find a rewrite rule for file start key", zap.Stringer("file", file))
		return errRewriteRuleNotFound
	}
	scanEndKey, ok := rewriteRawKeyWithNewPrefix(file.GetEndKey(), rewriteRules)
	if !ok {
		log.Error("cannot find a rewrite rule for file end key", zap.Stringer("file", file))
		return errRewriteRuleNotFound
	}
	err := withRetry(func() error {
		ctx, cancel := context.WithTimeout(importer.ctx, importScanRegionTime)
		defer cancel()
		// Scan regions covered by the file range
		regionInfos, err := importer.metaClient.ScanRegions(ctx, scanStartKey, scanEndKey, 0)
		if err != nil {
			return errors.Trace(err)
		}
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
							zap.ByteString("scanStartKey", scanStartKey),
							zap.ByteString("scanEndKey", scanEndKey),
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
		}
		return nil
	}, func(e error) bool {
		return true
	}, importFileRetryTimes, importFileWaitInterval, importFileMaxWaitInterval)
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
	regionRule := findRegionRewriteRule(regionInfo.Region, rewriteRules)
	if regionRule == nil {
		return nil, true, errRewriteRuleNotFound
	}
	sstMeta := getSSTMetaFromFile(id, file, regionInfo.Region, regionRule)
	sstMeta.RegionId = regionInfo.Region.GetId()
	sstMeta.RegionEpoch = regionInfo.Region.GetRegionEpoch()
	req := &import_sstpb.DownloadRequest{
		Sst:         sstMeta,
		Url:         importer.fileURL,
		Name:        file.GetName(),
		RewriteRule: *regionRule,
	}
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
	fileMeta *import_sstpb.SSTMeta,
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
		Sst:     fileMeta,
	}
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
