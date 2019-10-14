package restore

import (
	"bytes"
	"context"
	"sync"
	"time"

	restore_util "github.com/5kbpers/tidb-tools/pkg/restore-util"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"google.golang.org/grpc"
)

var notLeaderError error = errors.New("not leader")
var epochNotMatchError error = errors.New("epoch not match")

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
		client:  client,
		fileURL: fileURL,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (importer *FileImporter) Import(file *File, recordRule import_sstpb.RewriteRule, indexRules []import_sstpb.RewriteRule) error {
	err := WithRetry(func() error {
		regionInfos, err := importer.getRegions(file)
		if err != nil {
			return err
		}
		returnErrs := make([]error, len(regionInfos))
		var wg sync.WaitGroup
		for i, info := range regionInfos {
			wg.Add(1)
			go func(n int, regionInfo *restore_util.RegionInfo) {
				defer wg.Done()
				rule := findRewriteRule(regionInfo.Region, recordRule, indexRules)
				returnErrs[n] = importer.downloadSST(regionInfo, file, rule)
				if returnErrs[n] != nil {
					return
				}
				returnErrs[n] = WithRetry(func() error {
					return importer.ingestSST(regionInfo, file)
				}, func(e error) bool {
					if e == epochNotMatchError {
						return false
					}
					return true
				}, 3, time.Millisecond*100)
			}(i, info)
		}
		wg.Wait()
		for _, err = range returnErrs {
			if err != nil {
				return err
			}
		}
		return nil
	}, func(e error) bool {
		return true
	}, 3, time.Millisecond*100)
	return err
}

func (importer *FileImporter) getRegions(file *File) ([]*restore_util.RegionInfo, error) {
	overlappedRegions := make([]*restore_util.RegionInfo, 0)
	startKey := file.Meta.GetStartKey()
GetRegions:
	for {
		regionInfos, err := importer.client.ScanRegions(importer.ctx, startKey, 3)
		if err != nil {
			return nil, err
		}
		for i := range regionInfos {
			if len(file.Meta.GetEndKey()) == 0 ||
				bytes.Compare(regionInfos[i].Region.GetStartKey(), file.Meta.GetEndKey()) < 0 {
				overlappedRegions = append(overlappedRegions, regionInfos[i])
				if len(regionInfos[i].Region.GetEndKey()) == 0 {
					break GetRegions
				}
				startKey = regionInfos[i].Region.GetEndKey()
			} else {
				break GetRegions
			}
		}
	}
	return overlappedRegions, nil
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
		return nil, err
	}
	conn, err := grpc.Dial(store.GetAddress(), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client = import_sstpb.NewImportSSTClient(conn)
	importer.importClients[storeID] = client
	return client, err
}

func (importer *FileImporter) downloadSST(regionInfo *restore_util.RegionInfo, file *File, rule import_sstpb.RewriteRule) error {
	for _, peer := range regionInfo.Region.GetPeers() {
		client, err := importer.getImportClient(peer.GetStoreId())
		if err != nil {
			return err
		}
		fileMeta := GetSSTMetaFromFile(file, regionInfo.Region)
		fileMeta.RegionId = regionInfo.Region.GetId()
		fileMeta.RegionEpoch = regionInfo.Region.GetRegionEpoch()
		req := &import_sstpb.DownloadRequest{
			Sst:         fileMeta,
			Url:         importer.fileURL,
			Name:        file.Meta.GetName(),
			RewriteRule: rule,
		}
		_, err = client.Download(importer.ctx, req)
		if err != nil {
			return err
		}
	}
	return nil
}

func (importer *FileImporter) ingestSST(regionInfo *restore_util.RegionInfo, file *File) error {
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
	fileMeta := GetSSTMetaFromFile(file, regionInfo.Region)
	fileMeta.RegionId = regionInfo.Region.GetId()
	fileMeta.RegionEpoch = regionInfo.Region.GetRegionEpoch()
	req := &import_sstpb.IngestRequest{
		Context: reqCtx,
		Sst:     &fileMeta,
	}
	resp, err := client.Ingest(importer.ctx, req)
	if err != nil {
		return err
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

func findRewriteRule(region *metapb.Region, recordRule import_sstpb.RewriteRule, indexRules []import_sstpb.RewriteRule) import_sstpb.RewriteRule {
	if len(region.GetStartKey()) != 0 || len(region.GetEndKey()) != 0 {
		key := region.GetStartKey()
		if len(key) == 0 {
			key = region.GetEndKey()
		}
		for _, ir := range indexRules {
			if bytes.HasPrefix(key, ir.GetNewKeyPrefix()) {
				return ir
			}
		}
		if bytes.HasPrefix(key, recordRule.GetNewKeyPrefix()) {
			return recordRule
		}
	}
	return import_sstpb.RewriteRule{}
}
