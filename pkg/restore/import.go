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
	importScanResgionTime     = 10 * time.Second
	importFileRetryTimes      = 16
	importFileWaitInterval    = 10 * time.Millisecond
	importFileMaxWaitInterval = 1 * time.Second

	downloadSSTRetryTimes      = 8
	downloadSSTWaitInterval    = 10 * time.Millisecond
	downloadSSTMaxWaitInterval = 1 * time.Second
)

// FileImporter used to import a file to TiKV.
type FileImporter struct {
	mu            sync.Mutex
	client        restore_util.Client
	fileURL       string
	importClients map[uint64]import_sstpb.ImportSSTClient

	ctx    context.Context
	cancel context.CancelFunc
}

// NewFileImporter returns a new file importer.
func NewFileImporter(ctx context.Context, client restore_util.Client, fileURL string) FileImporter {
	ctx, cancel := context.WithCancel(ctx)
	return FileImporter{
		client:        client,
		fileURL:       fileURL,
		ctx:           ctx,
		cancel:        cancel,
		importClients: make(map[uint64]import_sstpb.ImportSSTClient),
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
		ctx, cancel := context.WithTimeout(importer.ctx, importScanResgionTime)
		defer cancel()
		// Scan regions covered by the file range
		regionInfos, err := importer.client.ScanRegions(ctx, scanStartKey, scanEndKey, 0)
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
				return e != errRewriteRuleNotFound &&
					// Skip empty files
					e != errRangeIsEmpty
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

func (importer *FileImporter) getImportClient(
	storeID uint64,
) (import_sstpb.ImportSSTClient, error) {
	importer.mu.Lock()
	defer importer.mu.Unlock()
	client, ok := importer.importClients[storeID]
	if ok {
		return client, nil
	}
	store, err := importer.client.GetStore(importer.ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	conn, err := grpc.Dial(store.GetAddress(), grpc.WithInsecure())
	if err != nil {
		return nil, errors.Trace(err)
	}
	client = import_sstpb.NewImportSSTClient(conn)
	importer.importClients[storeID] = client
	return client, errors.Trace(err)
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
		client, err := importer.getImportClient(peer.GetStoreId())
		if err != nil {
			return nil, true, err
		}
		resp, err = client.Download(importer.ctx, req)
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
	client, err := importer.getImportClient(leader.GetStoreId())
	if err != nil {
		return err
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
	resp, err := client.Ingest(importer.ctx, req)
	if err != nil {
		return errors.Trace(err)
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
