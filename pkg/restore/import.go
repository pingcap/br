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
	"github.com/tikv/pd/pkg/codec"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/logutil"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

const (
	importScanRegionTime      = 10 * time.Second
	scanRegionPaginationLimit = int(128)
	gRPCBackOffMaxDelay       = 3 * time.Second
)

// ImporterClient is used to import a file to TiKV.
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

	GetImportClient(
		ctx context.Context,
		storeID uint64,
	) (import_sstpb.ImportSSTClient, error)
}

type importClient struct {
	mu         sync.Mutex
	metaClient SplitClient
	clients    map[uint64]import_sstpb.ImportSSTClient
	tlsConf    *tls.Config

	keepaliveConf keepalive.ClientParameters
}

// NewImportClient returns a new ImporterClient.
func NewImportClient(metaClient SplitClient, tlsConf *tls.Config, keepaliveConf keepalive.ClientParameters) ImporterClient {
	return &importClient{
		metaClient:    metaClient,
		clients:       make(map[uint64]import_sstpb.ImportSSTClient),
		tlsConf:       tlsConf,
		keepaliveConf: keepaliveConf,
	}
}

func (ic *importClient) DownloadSST(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.DownloadRequest,
) (*import_sstpb.DownloadResponse, error) {
	client, err := ic.GetImportClient(ctx, storeID)
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
	client, err := ic.GetImportClient(ctx, storeID)
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
	client, err := ic.GetImportClient(ctx, storeID)
	if err != nil {
		return nil, err
	}
	return client.Ingest(ctx, req)
}

func (ic *importClient) GetImportClient(
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
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	conn, err := grpc.DialContext(
		ctx,
		addr,
		opt,
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(ic.keepaliveConf),
	)
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
}

// NewFileImporter returns a new file importClient.
func NewFileImporter(
	metaClient SplitClient,
	importClient ImporterClient,
	backend *backup.StorageBackend,
	isRawKvMode bool,
	rateLimit uint64,
) FileImporter {
	return FileImporter{
		metaClient:   metaClient,
		backend:      backend,
		importClient: importClient,
		isRawKvMode:  isRawKvMode,
		rateLimit:    rateLimit,
	}
}

// SetRawRange sets the range to be restored in raw kv mode.
func (importer *FileImporter) SetRawRange(startKey, endKey []byte) error {
	if !importer.isRawKvMode {
		return errors.Annotate(berrors.ErrRestoreModeMismatch, "file importer is not in raw kv mode")
	}
	importer.rawStartKey = startKey
	importer.rawEndKey = endKey
	return nil
}

// Import tries to import a file.
// All rules must contain encoded keys.
func (importer *FileImporter) Import(
	ctx context.Context,
	file *backup.File,
	rewriteRules *RewriteRules,
) error {
	log.Debug("import file", logutil.File(file))
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
		logutil.File(file),
		zap.Stringer("startKey", logutil.WrapKey(startKey)),
		zap.Stringer("endKey", logutil.WrapKey(endKey)))

	err = utils.WithRetry(ctx, func() error {
		tctx, cancel := context.WithTimeout(ctx, importScanRegionTime)
		defer cancel()
		// Scan regions covered by the file range
		regionInfos, errScanRegion := PaginateScanRegion(
			tctx, importer.metaClient, startKey, endKey, scanRegionPaginationLimit)
		if errScanRegion != nil {
			return errors.Trace(errScanRegion)
		}

		log.Debug("scan regions", logutil.File(file), zap.Int("count", len(regionInfos)))
		// Try to download and ingest the file in every region
	regionLoop:
		for _, regionInfo := range regionInfos {
			info := regionInfo
			// Try to download file.
			var downloadMeta *import_sstpb.SSTMeta
			errDownload := utils.WithRetry(ctx, func() error {
				var e error
				if importer.isRawKvMode {
					downloadMeta, e = importer.downloadRawKVSST(ctx, info, file)
				} else {
					downloadMeta, e = importer.downloadSST(ctx, info, file, rewriteRules)
				}
				return e
			}, newDownloadSSTBackoffer())
			if errDownload != nil {
				for _, e := range multierr.Errors(errDownload) {
					switch errors.Cause(e) {
					case berrors.ErrKVRewriteRuleNotFound, berrors.ErrKVRangeIsEmpty:
						// Skip this region
						log.Warn("download file skipped",
							logutil.File(file),
							logutil.Region(info.Region),
							zap.Stringer("startKey", logutil.WrapKey(startKey)),
							zap.Stringer("endKey", logutil.WrapKey(endKey)),
							logutil.ShortError(e))
						continue regionLoop
					}
				}
				log.Error("download file failed",
					logutil.File(file),
					logutil.Region(info.Region),
					zap.Stringer("startKey", logutil.WrapKey(startKey)),
					zap.Stringer("endKey", logutil.WrapKey(endKey)),
					logutil.ShortError(errDownload))
				return errDownload
			}

			ingestResp, errIngest := importer.ingestSST(ctx, downloadMeta, info)
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
							ctx, info.Region.GetStartKey())
						if errIngest != nil {
							break ingestRetry
						}
						// do not get region info, wait a second and continue
						if newInfo == nil {
							log.Warn("get region by key return nil", zap.Reflect("region", info.Region))
							time.Sleep(time.Second)
							continue
						}
					}
					log.Debug("ingest sst returns not leader error, retry it",
						logutil.Region(info.Region),
						zap.Stringer("newLeader", newInfo.Leader))

					if !checkRegionEpoch(newInfo, info) {
						errIngest = errors.Trace(berrors.ErrKVEpochNotMatch)
						break ingestRetry
					}
					ingestResp, errIngest = importer.ingestSST(ctx, downloadMeta, newInfo)
				case errPb.EpochNotMatch != nil:
					// TODO handle epoch not match error
					//      1. retry download if needed
					//      2. retry ingest
					errIngest = errors.Trace(berrors.ErrKVEpochNotMatch)
					break ingestRetry
				case errPb.KeyNotInRegion != nil:
					errIngest = errors.Trace(berrors.ErrKVKeyNotInRegion)
					break ingestRetry
				default:
					// Other errors like `ServerIsBusy`, `RegionNotFound`, etc. should be retryable
					errIngest = errors.Annotatef(berrors.ErrKVIngestFailed, "ingest error %s", errPb)
					break ingestRetry
				}
			}

			if errIngest != nil {
				log.Error("ingest file failed",
					logutil.File(file),
					zap.Stringer("range", downloadMeta.GetRange()),
					logutil.Region(info.Region),
					zap.Error(errIngest))
				return errIngest
			}
		}
		summary.CollectSuccessUnit(summary.TotalKV, 1, file.TotalKvs)
		summary.CollectSuccessUnit(summary.TotalBytes, 1, file.TotalBytes)
		return nil
	}, newImportSSTBackoffer())
	return err
}

func (importer *FileImporter) setDownloadSpeedLimit(ctx context.Context, storeID uint64) error {
	req := &import_sstpb.SetDownloadSpeedLimitRequest{
		SpeedLimit: importer.rateLimit,
	}
	_, err := importer.importClient.SetDownloadSpeedLimit(ctx, storeID, req)
	return err
}

func (importer *FileImporter) downloadSST(
	ctx context.Context,
	regionInfo *RegionInfo,
	file *backup.File,
	rewriteRules *RewriteRules,
) (*import_sstpb.SSTMeta, error) {
	uid := uuid.New()
	id := uid[:]
	// Assume one region reflects to one rewrite rule
	_, key, err := codec.DecodeBytes(regionInfo.Region.GetStartKey())
	if err != nil {
		return nil, err
	}
	regionRule := matchNewPrefix(key, rewriteRules)
	if regionRule == nil {
		return nil, errors.Trace(berrors.ErrKVRewriteRuleNotFound)
	}
	rule := import_sstpb.RewriteRule{
		OldKeyPrefix: encodeKeyPrefix(regionRule.GetOldKeyPrefix()),
		NewKeyPrefix: encodeKeyPrefix(regionRule.GetNewKeyPrefix()),
	}
	sstMeta := GetSSTMetaFromFile(id, file, regionInfo.Region, &rule)

	req := &import_sstpb.DownloadRequest{
		Sst:            sstMeta,
		StorageBackend: importer.backend,
		Name:           file.GetName(),
		RewriteRule:    rule,
	}
	log.Debug("download SST",
		logutil.SSTMeta(&sstMeta),
		logutil.File(file),
		logutil.Region(regionInfo.Region),
	)
	var resp *import_sstpb.DownloadResponse
	for _, peer := range regionInfo.Region.GetPeers() {
		resp, err = importer.importClient.DownloadSST(ctx, peer.GetStoreId(), req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if resp.GetError() != nil {
			return nil, errors.Annotate(berrors.ErrKVDownloadFailed, resp.GetError().GetMessage())
		}
		if resp.GetIsEmpty() {
			return nil, errors.Trace(berrors.ErrKVRangeIsEmpty)
		}
	}
	sstMeta.Range.Start = truncateTS(resp.Range.GetStart())
	sstMeta.Range.End = truncateTS(resp.Range.GetEnd())
	return &sstMeta, nil
}

func (importer *FileImporter) downloadRawKVSST(
	ctx context.Context,
	regionInfo *RegionInfo,
	file *backup.File,
) (*import_sstpb.SSTMeta, error) {
	uid := uuid.New()
	id := uid[:]
	// Empty rule
	var rule import_sstpb.RewriteRule
	sstMeta := GetSSTMetaFromFile(id, file, regionInfo.Region, &rule)

	// Cut the SST file's range to fit in the restoring range.
	if bytes.Compare(importer.rawStartKey, sstMeta.Range.GetStart()) > 0 {
		sstMeta.Range.Start = importer.rawStartKey
	}
	if len(importer.rawEndKey) > 0 &&
		(len(sstMeta.Range.GetEnd()) == 0 || bytes.Compare(importer.rawEndKey, sstMeta.Range.GetEnd()) <= 0) {
		sstMeta.Range.End = importer.rawEndKey
		sstMeta.EndKeyExclusive = true
	}
	if bytes.Compare(sstMeta.Range.GetStart(), sstMeta.Range.GetEnd()) > 0 {
		return nil, errors.Trace(berrors.ErrKVRangeIsEmpty)
	}

	req := &import_sstpb.DownloadRequest{
		Sst:            sstMeta,
		StorageBackend: importer.backend,
		Name:           file.GetName(),
		RewriteRule:    rule,
		IsRawKv:        true,
	}
	log.Debug("download SST", logutil.SSTMeta(&sstMeta), logutil.Region(regionInfo.Region))
	var err error
	var resp *import_sstpb.DownloadResponse
	for _, peer := range regionInfo.Region.GetPeers() {
		resp, err = importer.importClient.DownloadSST(ctx, peer.GetStoreId(), req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if resp.GetError() != nil {
			return nil, errors.Annotate(berrors.ErrKVDownloadFailed, resp.GetError().GetMessage())
		}
		if resp.GetIsEmpty() {
			return nil, errors.Trace(berrors.ErrKVRangeIsEmpty)
		}
	}
	sstMeta.Range.Start = resp.Range.GetStart()
	sstMeta.Range.End = resp.Range.GetEnd()
	return &sstMeta, nil
}

func (importer *FileImporter) ingestSST(
	ctx context.Context,
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
	log.Debug("ingest SST", logutil.SSTMeta(sstMeta), zap.Reflect("leader", leader))
	resp, err := importer.importClient.IngestSST(ctx, leader.GetStoreId(), req)
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
