package restore

import (
	"context"
	"strings"
	"sync"
	"time"

	restore_util "github.com/5kbpers/tidb-tools/pkg/restore-util"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/twinj/uuid"
	"google.golang.org/grpc"
)

var (
	notLeaderError           error = errors.New("not leader")
	epochNotMatchError       error = errors.New("epoch not match")
	rewriteRuleNotFoundError error = errors.New("rewrite rule not found")
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
// All rules must contain encoded keys
func (importer *FileImporter) Import(file *backup.File, rewriteRules *restore_util.RewriteRules) error {
	err := WithRetry(func() error {
		regionInfos, err := importer.client.ScanRegions(
			importer.ctx,
			rewriteRawKeyWithNewPrefix(file.GetStartKey(), rewriteRules),
			rewriteRawKeyWithNewPrefix(file.GetEndKey(), rewriteRules),
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
				id, err := importer.downloadSST(regionInfo, file, rewriteRules)
				if err != nil {
					if strings.Contains(err.Error(), "Cannot create sst file with no entries") {
						return
					}
					if err != rewriteRuleNotFoundError {
						returnErrs[n] = err
					}
					return
				}
				returnErrs[n] = WithRetry(func() error {
					return importer.ingestSST(id, regionInfo, file, rewriteRules)
				}, func(e error) bool {
					if e == epochNotMatchError {
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

func (importer *FileImporter) downloadSST(regionInfo *restore_util.RegionInfo, file *backup.File, rewriteRules *restore_util.RewriteRules) ([]byte, error) {
	id := uuid.NewV4().Bytes()
	for _, peer := range regionInfo.Region.GetPeers() {
		client, err := importer.getImportClient(peer.GetStoreId())
		if err != nil {
			return nil, errors.Trace(err)
		}
		regionRule := findRegionRewriteRule(regionInfo.Region, rewriteRules)
		if regionRule == nil {
			return nil, rewriteRuleNotFoundError
		}
		sstMeta := getSSTMetaFromFile(id, file, regionInfo.Region, rewriteRules, false)
		sstMeta.RegionId = regionInfo.Region.GetId()
		sstMeta.RegionEpoch = regionInfo.Region.GetRegionEpoch()
		req := &import_sstpb.DownloadRequest{
			Sst:         sstMeta,
			Url:         importer.fileURL,
			Name:        file.GetName(),
			RewriteRule: *regionRule,
		}
		_, err = client.Download(importer.ctx, req)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return id, nil
}

func (importer *FileImporter) ingestSST(id []byte, regionInfo *restore_util.RegionInfo, file *backup.File, rewriteRules *restore_util.RewriteRules) error {
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
	fileMeta := getSSTMetaFromFile(id, file, regionInfo.Region, rewriteRules, true)
	fileMeta.RegionId = regionInfo.Region.GetId()
	fileMeta.RegionEpoch = regionInfo.Region.GetRegionEpoch()
	// TODO: remove this line after update kvproto
	fileMeta.Range.End = fileMeta.Range.Start
	req := &import_sstpb.IngestRequest{
		Context: reqCtx,
		Sst:     &fileMeta,
	}
	resp, err := client.Ingest(importer.ctx, req)
	if err != nil {
		return errors.Trace(err)
	}
	respErr := resp.GetError()
	if respErr != nil {
		if respErr.EpochNotMatch != nil {
			return epochNotMatchError
		}
		if respErr.NotLeader != nil {
			return notLeaderError
		}
		return errors.Errorf("ingest failed: %v", respErr)
	}
	return nil
}
