package restore

import (
	"context"
	"sync"
	"time"

	restore_util "github.com/5kbpers/tidb-tools/pkg/restore-util"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/prometheus/common/log"
	"github.com/twinj/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	errNotLeader           error = errors.New("not leader")
	errEpochNotMatch       error = errors.New("epoch not match")
	errRewriteRuleNotFound error = errors.New("rewrite rule not found")
)

const (
	importFileRetryTimes   = 8
	importFileWaitInterval = 10 * time.Millisecond

	downloadSSTRetryTimes   = 3
	downloadSSTWaitInterval = 10 * time.Millisecond
)

type FileImporter struct {
	mu            sync.Mutex
	client        restore_util.Client
	fileURL       string
	importClients map[uint64]import_sstpb.ImportSSTClient

	ctx    context.Context
	cancel context.CancelFunc
}

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
	err := withRetry(func() error {
		startKey := file.GetStartKey()
		endKey := file.GetEndKey()
		startTableID := tablecodec.DecodeTableID(startKey)
		endTableID := tablecodec.DecodeTableID(endKey)
		if startTableID != endTableID {
			// endTableID probably is startTableID + 1, we may don't know its rewrite rule, so we use a key which is
			// bigger than all of the data keys as endKey here.
			endKey = append(tablecodec.GenTablePrefix(startTableID), 0xff)
		}
		regionInfos, err := importer.client.ScanRegions(
			importer.ctx,
			rewriteRawKeyWithNewPrefix(startKey, rewriteRules),
			rewriteRawKeyWithNewPrefix(endKey, rewriteRules),
			0,
		)
		if err != nil {
			return errors.Trace(err)
		}
		returnErrs := make([]error, len(regionInfos))
		var wg sync.WaitGroup
		for i, info := range regionInfos {
			wg.Add(1)
			go func(n int, regionInfo *restore_util.RegionInfo) {
				defer wg.Done()
				fileMeta, isEmpty, err := importer.downloadSST(regionInfo, file, rewriteRules)
				if err != nil {
					if err != errRewriteRuleNotFound {
						returnErrs[n] = err
						log.Warn("download file failed",
							zap.Reflect("file", file),
							zap.Reflect("region", regionInfo.Region),
							zap.Reflect("tableRewriteRules", rules(rewriteRules.Table)),
							zap.Reflect("dataRewriteRules", rules(rewriteRules.Data)),
							zap.Error(err),
						)
					}
					return
				}
				if isEmpty {
					log.Warn(
						"file don't have key in this region, skip it",
						zap.Reflect("file", file),
						zap.Reflect("region", regionInfo.Region),
						zap.Reflect("tableRewriteRules", rules(rewriteRules.Table)),
						zap.Reflect("dataRewriteRules", rules(rewriteRules.Data)),
					)
					return
				}
				returnErrs[n] = withRetry(func() error {
					err = importer.ingestSST(fileMeta, regionInfo, rewriteRules)
					if err != nil {
						log.Warn("ingest file failed",
							zap.Reflect("file", file),
							zap.Reflect("region", regionInfo.Region),
							zap.Reflect("tableRewriteRules", rules(rewriteRules.Table)),
							zap.Reflect("dataRewriteRules", rules(rewriteRules.Data)),
							zap.Error(err),
						)
						return err
					}
					return nil
				}, func(e error) bool {
					if e == errEpochNotMatch {
						return false
					}
					return true
				}, downloadSSTRetryTimes, downloadSSTWaitInterval)
			}(i, info)
		}
		wg.Wait()
		for _, err = range returnErrs {
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}, func(e error) bool {
		return true
	}, importFileRetryTimes, importFileWaitInterval)
	return errors.Trace(err)
}

func (importer *FileImporter) getImportClient(storeID uint64) (import_sstpb.ImportSSTClient, error) {
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

func (importer *FileImporter) downloadSST(regionInfo *restore_util.RegionInfo, file *backup.File, rewriteRules *restore_util.RewriteRules) (*import_sstpb.SSTMeta, bool, error) {
	id := uuid.NewV4().Bytes()
	regionRule := findRegionRewriteRule(regionInfo.Region, rewriteRules)
	if regionRule == nil {
		return nil, true, errRewriteRuleNotFound
	}
	sstMeta := getSSTMetaFromFile(id, file, regionRule)
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
			return nil, true, errors.Trace(err)
		}
		resp, err = client.Download(importer.ctx, req)
		if err != nil {
			return nil, true, errors.Trace(err)
		}
		if resp.GetIsEmpty() {
			return nil, true, nil
		}
		sstMeta.Range = &resp.Range
	}
	return &sstMeta, false, nil
}

func (importer *FileImporter) ingestSST(fileMeta *import_sstpb.SSTMeta, regionInfo *restore_util.RegionInfo, rewriteRules *restore_util.RewriteRules) error {
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
