// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/util/codec"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/br/pkg/restore/cdclog"
	"github.com/pingcap/br/pkg/utils"
)

const (
	tableLogPrefix = "t_"
	logPrefix      = "cdclog"

	metaFile      = "log.meta"
	ddlEventsDir  = "ddls"
	ddlFilePrefix = "ddl"

	dialTimeout          = 5 * time.Second
	gRPCKeepAliveTime    = 10 * time.Second
	gRPCKeepAliveTimeout = 3 * time.Second
	gRPCBackOffMaxDelay  = 3 * time.Second

	maxUint64 = ^uint64(0)

	maxRetryTimes = 3
)

type LogMeta struct {
	Names            map[int64]string `json:"names"`
	GlobalResolvedTS uint64           `json:"global_resolved_ts"`
}

type grpcClis struct {
	mu   sync.Mutex
	clis map[uint64]*grpc.ClientConn
}

// LogClient sends requests to restore files.
type LogClient struct {
	restoreClient *Client
	splitClient   SplitClient

	// range of log backup
	startTS uint64
	endTs   uint64

	batchWriteKVPairs int

	// meta info parsed from log backup
	meta         *LogMeta
	eventPullers map[int64]*cdclog.EventPuller
	tableBuffers map[int64]*cdclog.TableBuffer
	grpcClis     grpcClis

	tableFilter filter.Filter
}

// NewLogRestoreClient returns a new LogRestoreClient.
func NewLogRestoreClient(
	ctx context.Context,
	restoreClient *Client,
	startTs uint64,
	endTS uint64,
	tableFilter filter.Filter,
) (*LogClient, error) {
	var err error
	if endTS == 0 {
		// means restore all log data,
		// so we get current ts from restore cluster
		endTS, err = restoreClient.GetTS(ctx)
		if err != nil {
			return nil, err
		}
	}

	splitClient := NewSplitClient(restoreClient.GetPDClient(), restoreClient.GetTLSConfig())

	lc := &LogClient{
		restoreClient: restoreClient,
		splitClient:   splitClient,
		startTS:       startTs,
		endTs:         endTS,
		// TODO make it config
		batchWriteKVPairs: 0,
		meta:              new(LogMeta),
		eventPullers:      make(map[int64]*cdclog.EventPuller),
		tableBuffers:      make(map[int64]*cdclog.TableBuffer),
		tableFilter:       tableFilter,
	}
	lc.grpcClis.clis = make(map[uint64]*grpc.ClientConn)
	return lc, nil
}

func (l *LogClient) getGrpcConnLocked(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	store, err := l.splitClient.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithInsecure()
	if l.restoreClient.tlsConf != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(l.restoreClient.tlsConf))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)

	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	conn, err := grpc.DialContext(
		ctx,
		store.GetAddress(),
		opt,
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                gRPCKeepAliveTime,
			Timeout:             gRPCKeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	cancel()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Cache the conn.
	l.grpcClis.clis[storeID] = conn
	return conn, nil
}

func (l *LogClient) getImportClient(ctx context.Context, peer *metapb.Peer) (sst.ImportSSTClient, error) {
	l.grpcClis.mu.Lock()
	defer l.grpcClis.mu.Unlock()
	var err error

	conn, ok := l.grpcClis.clis[peer.GetStoreId()]
	if !ok {
		conn, err = l.getGrpcConnLocked(ctx, peer.GetStoreId())
		if err != nil {
			log.L().Error("could not get grpc connect ", zap.Uint64("storeId", peer.GetStoreId()))
			return nil, err
		}
	}
	return sst.NewImportSSTClient(conn), nil
}

func (l *LogClient) tsInRange(ts uint64) bool {
	if ts < l.startTS || ts > l.endTs {
		return false
	}
	return true
}

func (l *LogClient) needRestoreDDL(fileName string) (bool, error) {
	names := strings.Split(fileName, ".")
	if len(names) != 2 {
		log.Warn("found wrong format of ddl file", zap.String("file", fileName))
		return false, nil
	}
	if names[0] != ddlFilePrefix {
		log.Warn("file doesn't start with ddl", zap.String("file", fileName))
		return false, nil
	}
	ts, err := strconv.ParseUint(names[1], 10, 64)
	if err != nil {
		return false, errors.AddStack(err)
	}
	ts = maxUint64 - ts
	if l.tsInRange(ts) {
		return true, nil
	}
	return false, nil
}

func (l *LogClient) collectDDLFiles(ctx context.Context) ([]string, error) {
	ddlFiles := make([]string, 0)
	err := l.restoreClient.storage.WalkDir(ctx, ddlEventsDir, -1, func(path string, size int64) error {
		fileName := filepath.Base(path)
		shouldRestore, err := l.needRestoreDDL(fileName)
		if err != nil {
			return err
		}
		if shouldRestore {
			ddlFiles = append(ddlFiles, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ddlFiles, nil
}

func (l *LogClient) needRestoreRowChange(fileName string) (bool, error) {
	names := strings.Split(fileName, ".")
	if len(names) != 2 {
		log.Warn("found wrong format of row changes file", zap.String("file", fileName))
		return false, nil
	}
	if names[0] != logPrefix {
		log.Warn("file doesn't start with row changes file", zap.String("file", fileName))
		return false, nil
	}
	ts, err := strconv.ParseUint(names[1], 10, 64)
	if err != nil {
		return false, errors.AddStack(err)
	}
	if l.tsInRange(ts) {
		return true, nil
	}
	return false, nil
}

func (l *LogClient) collectRowChangeFiles(ctx context.Context) (map[int64][]string, error) {
	// we should collect all related tables row change files
	// by log meta info and by given table filter
	rowChangeFiles := make(map[int64][]string)

	// need collect restore tableIDs
	tableIDs := make([]int64, 0, len(l.meta.Names))
	for tableID, name := range l.meta.Names {
		schema, table := parseQuoteName(name)
		if !l.tableFilter.MatchTable(schema, table) {
			log.Info("filter tables",
				zap.String("schema", schema),
				zap.String("table", table),
				zap.Int64("tableID", tableID),
			)
			continue
		}
		tableIDs = append(tableIDs, tableID)
	}

	for _, tID := range tableIDs {
		tableID := tID
		// FIXME update log meta logic here
		dir := fmt.Sprintf("%s%d", tableLogPrefix, tableID)
		err := l.restoreClient.storage.WalkDir(ctx, dir, -1, func(path string, size int64) error {
			fileName := filepath.Base(path)
			shouldRestore, err := l.needRestoreRowChange(fileName)
			if err != nil {
				return err
			}
			if shouldRestore {
				if _, ok := rowChangeFiles[tableID]; ok {
					rowChangeFiles[tableID] = append(rowChangeFiles[tableID], path)
				} else {
					rowChangeFiles[tableID] = []string{path}
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return rowChangeFiles, nil
}

func (l *LogClient) writeToTiKV(ctx context.Context, kvs cdclog.KvPairs, region *RegionInfo) ([]*sst.SSTMeta, error) {
	firstKey := codec.EncodeBytes([]byte{}, kvs[0].Key)
	lastKey := codec.EncodeBytes([]byte{}, kvs[len(kvs)-1].Key)

	meta := &sst.SSTMeta{
		Uuid:        uuid.NewV4().Bytes(),
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Range: &sst.Range{
			Start: firstKey,
			End:   lastKey,
		},
	}

	leaderID := region.Leader.GetId()
	clients := make([]sst.ImportSST_WriteClient, 0, len(region.Region.GetPeers()))
	requests := make([]*sst.WriteRequest, 0, len(region.Region.GetPeers()))
	for _, peer := range region.Region.GetPeers() {
		cli, err := l.getImportClient(ctx, peer)
		if err != nil {
			return nil, err
		}

		wstream, err := cli.Write(ctx)
		if err != nil {
			return nil, err
		}

		// Bind uuid for this write request
		req := &sst.WriteRequest{
			Chunk: &sst.WriteRequest_Meta{
				Meta: meta,
			},
		}
		if err = wstream.Send(req); err != nil {
			return nil, err
		}
		req.Chunk = &sst.WriteRequest_Batch{
			Batch: &sst.WriteBatch{
				// FIXME commit ts
				CommitTs: 0,
			},
		}
		clients = append(clients, wstream)
		requests = append(requests, req)
	}

	bytesBuf := NewBytesBuffer()
	defer bytesBuf.Destroy()
	pairs := make([]*sst.Pair, 0, l.batchWriteKVPairs)
	count := 0
	size := int64(0)
	totalCount := 0
	firstLoop := true
	for _, kv := range kvs {
		size += int64(len(kv.Key) + len(kv.Val))
		// here we reuse the `*sst.Pair`s to optimize object allocation
		if firstLoop {
			pair := &sst.Pair{
				Key:   bytesBuf.AddBytes(kv.Key),
				Value: bytesBuf.AddBytes(kv.Val),
			}
			pairs = append(pairs, pair)
		} else {
			pairs[count].Key = bytesBuf.AddBytes(kv.Key)
			pairs[count].Value = bytesBuf.AddBytes(kv.Val)
		}
		count++
		totalCount++

		if count >= l.batchWriteKVPairs {
			for i := range clients {
				requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs
				if err := clients[i].Send(requests[i]); err != nil {
					return nil, err
				}
			}
			count = 0
			bytesBuf.Reset()
			firstLoop = false
		}
	}
	if count > 0 {
		for i := range clients {
			requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
			if err := clients[i].Send(requests[i]); err != nil {
				return nil, err
			}
		}
	}

	var leaderPeerMetas []*sst.SSTMeta
	for i, wStream := range clients {
		if resp, closeErr := wStream.CloseAndRecv(); closeErr != nil {
			return nil, closeErr
		} else if leaderID == region.Region.Peers[i].GetId() {
			leaderPeerMetas = resp.Metas
			log.L().Debug("get metas after write kv stream to tikv", zap.Reflect("metas", leaderPeerMetas))
		}
	}

	log.L().Debug("write to kv", zap.Reflect("region", region), zap.Uint64("leader", leaderID),
		zap.Reflect("meta", meta), zap.Reflect("return metas", leaderPeerMetas),
		zap.Int("kv_pairs", totalCount), zap.Int64("total_bytes", size),
		zap.Int64("buf_size", bytesBuf.TotalSize()))

	return leaderPeerMetas, nil
}

func (l *LogClient) Ingest(ctx context.Context, meta *sst.SSTMeta, region *RegionInfo) (*sst.IngestResponse, error) {
	leader := region.Leader
	if leader == nil {
		leader = region.Region.GetPeers()[0]
	}

	cli, err := l.getImportClient(ctx, leader)
	if err != nil {
		return nil, err
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Peer:        leader,
	}

	req := &sst.IngestRequest{
		Context: reqCtx,
		Sst:     meta,
	}
	resp, err := cli.Ingest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (l *LogClient) doWriteAndIngest(ctx context.Context, kvs cdclog.KvPairs, region *RegionInfo) error {
	var startKey, endKey []byte
	if len(region.Region.StartKey) > 0 {
		_, startKey, _ = codec.DecodeBytes(region.Region.StartKey, []byte{})
	}
	if len(region.Region.EndKey) > 0 {
		_, endKey, _ = codec.DecodeBytes(region.Region.EndKey, []byte{})
	}

	var start, end int
	// TODO use binary sort
	for i, kv := range kvs {
		if bytes.Compare(kv.Key, startKey) > 0 {
			start = i
			break
		}
	}
	for i, kv := range kvs {
		if bytes.Compare(kv.Key, endKey) < 0 {
			end = i
			break
		}
	}

	metas, err := l.writeToTiKV(ctx, kvs[start:end], region)
	if err != nil {
		log.L().Warn("write to tikv failed", zap.Error(err))
		return err
	}

	for _, meta := range metas {
		for i := 0; i < maxRetryTimes; i++ {
			log.L().Debug("ingest meta", zap.Reflect("meta", meta))
			resp, err := l.Ingest(ctx, meta, region)
			if err != nil {
				log.L().Warn("ingest failed", zap.Error(err), zap.Reflect("meta", meta),
					zap.Reflect("region", region))
				continue
			}
			needRetry, newRegion, errIngest := isIngestRetryable(resp, region, meta)
			if errIngest == nil {
				// ingest next meta
				break
			}
			if !needRetry {
				log.L().Warn("ingest failed noretry", zap.Error(errIngest), zap.Reflect("meta", meta),
					zap.Reflect("region", region))
				// met non-retryable error retry whole Write procedure
				return errIngest
			}
			// retry with not leader and epoch not match error
			if newRegion != nil && i < maxRetryTimes-1 {
				region = newRegion
			} else {
				log.L().Warn("retry ingest due to",
					zap.Reflect("meta", meta), zap.Reflect("region", region),
					zap.Reflect("new region", newRegion), zap.Error(errIngest))
				return errIngest
			}
		}
	}
	return nil
}

func (l *LogClient) writeAndIngestPairs(ctx context.Context, kvs cdclog.KvPairs) error {
	var (
		regions []*RegionInfo
		err     error
	)

	pairStart := kvs[0].Key
	pairEnd := kvs[len(kvs)-1].Key

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
WriteAndIngest:
	for retry := 0; retry < maxRetryTimes; retry++ {
		if retry != 0 {
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		startKey := codec.EncodeBytes([]byte{}, pairStart)
		endKey := codec.EncodeBytes([]byte{}, nextKey(pairEnd))
		regions, err = PaginateScanRegion(ctx, l.splitClient, startKey, endKey, 128)
		if err != nil || len(regions) == 0 {
			log.L().Warn("scan region failed", zap.Error(err), zap.Int("region_len", len(regions)))
			continue WriteAndIngest
		}

		shouldWait := false
		errChan := make(chan error, len(regions))
		for _, region := range regions {
			log.L().Debug("get region", zap.Int("retry", retry), zap.Binary("startKey", startKey),
				zap.Binary("endKey", endKey), zap.Uint64("id", region.Region.GetId()),
				zap.Stringer("epoch", region.Region.GetRegionEpoch()), zap.Binary("start", region.Region.GetStartKey()),
				zap.Binary("end", region.Region.GetEndKey()), zap.Reflect("peers", region.Region.GetPeers()))

			// generate new uuid for concurrent write to tikv
			if len(regions) == 1 {
				if err = l.doWriteAndIngest(ctx, kvs, region); err != nil {
					continue WriteAndIngest
				}
			} else {
				shouldWait = true
				go func(r *RegionInfo) {
					errChan <- l.doWriteAndIngest(ctx, kvs, r)
				}(region)
			}
		}
		if shouldWait {
			shouldRetry := false
			for i := 0; i < len(regions); i++ {
				err1 := <-errChan
				if err1 != nil {
					err = err1
					log.L().Warn("should retry this range", zap.Int("retry", retry), zap.Error(err))
					shouldRetry = true
				}
			}
			if !shouldRetry {
				return nil
			}
			continue WriteAndIngest
		}
		return nil
	}
	if err == nil {
		err = errors.New("all retry failed")
	}
	return err
}

func (l *LogClient) writeRows(ctx context.Context, rows cdclog.Rows) error {
	kvs := rows.(cdclog.KvPairs)
	// sort kvs in memory
	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})
	return l.writeAndIngestPairs(ctx, kvs)
}

func (l *LogClient) applyKVChanges(ctx context.Context, tableID int64) error {
	log.Debug("apply kv changes to tikv",
		zap.Any("table", l.tableBuffers[tableID]),
	)
	dataKVs := cdclog.MakeRowsFromKvPairs(nil)
	indexKVs := cdclog.MakeRowsFromKvPairs(nil)

	tableBuffer := l.tableBuffers[tableID]

	var dataChecksum, indexChecksum cdclog.KVChecksum
	for _, p := range tableBuffer.KvPairs {
		p.ClassifyAndAppend(&dataKVs, &dataChecksum, &indexKVs, &indexChecksum)
	}

	err := l.writeRows(ctx, dataKVs)
	if err != nil {
		return errors.Trace(err)
	}
	dataKVs = dataKVs.Clear()

	err = l.writeRows(ctx, indexKVs)
	if err != nil {
		return errors.Trace(err)
	}
	indexKVs = indexKVs.Clear()

	tableBuffer.Clear()

	return nil
}

func (l *LogClient) restoreTableFromPuller(ctx context.Context, tableID int64, puller *cdclog.EventPuller) error {
	for {
		item, err := puller.PullOneEvent(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if item == nil {
			log.Info("[restoreFromPuller] nothing in puller")
			return nil
		}
		if !l.tsInRange(item.TS) {
			log.Warn("[restoreFromPuller] ts not in given range",
				zap.Uint64("start ts", l.startTS),
				zap.Uint64("end ts", l.endTs),
				zap.Uint64("item ts", item.TS),
			)
			return nil
		}

		switch item.ItemType {
		case cdclog.DDL:
			name := l.meta.Names[tableID]
			schema, table := parseQuoteName(name)
			// ddl not influence on this schema/table
			if schema != item.Schema || table != item.Table {
				log.Info("[restoreFromPuller] meet unrelated ddl, and continue pulling",
					zap.String("item table", item.Table),
					zap.String("table", table),
					zap.String("item schema", item.Schema),
					zap.String("schema", schema),
					zap.Int64("current table id", tableID),
				)
				continue
			}
			ddl := item.Meta.(*cdclog.MessageDDL)
			log.Debug("[restoreFromPuller] exec ddl", zap.String("query", ddl.Query))

			// wait all previous kvs ingest finished
			err = l.applyKVChanges(ctx, tableID)
			if err != nil {
				return errors.Trace(err)
			}

			err = l.restoreClient.db.se.Execute(ctx, ddl.Query)
			if err != nil {
				return errors.Trace(err)
			}

		case cdclog.RowChanged:
			err := l.tableBuffers[tableID].Append(ctx, item)
			if err != nil {
				return errors.Trace(err)
			}
			if l.tableBuffers[tableID].ShouldApply() {
				err = l.applyKVChanges(ctx, tableID)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}

func (l *LogClient) restoreTables(ctx context.Context) error {
	// 1. decode cdclog with in ts range
	// 2. dispatch cdclog events to table level concurrently
	// 		a. encode row changed files to kvpairs and ingest into tikv
	// 		b. exec ddl
	// TODO change it concurrency to config
	workerPool := utils.NewWorkerPool(128, "table log restore")
	var eg *errgroup.Group
	for tableID, puller := range l.eventPullers {
		pullerReplica := puller
		tableIDReplica := tableID
		workerPool.ApplyOnErrorGroup(eg, func() error {
			return l.restoreTableFromPuller(ctx, tableIDReplica, pullerReplica)
		})
	}
	return nil
}

// RestoreLogData restore specify log data from storage.
func (l *LogClient) RestoreLogData(ctx context.Context, dom *domain.Domain) error {
	// 1. Retrieve log data from storage
	// 2. Find proper data by TS range
	// 3. Encode and ingest data to tikv

	// parse meta file
	data, err := l.restoreClient.storage.Read(ctx, metaFile)
	if err != nil {
		return errors.Trace(err)
	}
	err = json.Unmarshal(data, l.meta)
	if err != nil {
		return errors.Trace(err)
	}

	if l.startTS > l.meta.GlobalResolvedTS {
		return errors.Errorf("start ts:%d is greater than resolved ts:%d",
			l.startTS, l.meta.GlobalResolvedTS)
	}
	if l.endTs > l.meta.GlobalResolvedTS {
		log.Info("end ts is greater than resolved ts,"+
			" to keep consistency we only recover data until resolved ts",
			zap.Uint64("end ts", l.endTs),
			zap.Uint64("resolved ts", l.meta.GlobalResolvedTS))
		l.endTs = l.meta.GlobalResolvedTS
	}

	// collect ddl files
	ddlFiles, err := l.collectDDLFiles(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// collect row change files
	rowChangesFiles, err := l.collectRowChangeFiles(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// create event puller to apply changes concurrently
	for tableID, files := range rowChangesFiles {
		name := l.meta.Names[tableID]
		schema, table := parseQuoteName(name)
		l.eventPullers[tableID], err = cdclog.NewEventPuller(ctx, schema, table, ddlFiles, files, l.restoreClient.storage)
		if err != nil {
			return errors.Trace(err)
		}
		// use table name to get table info
		tableInfo, err := dom.InfoSchema().TableByName(model.NewCIStr(schema), model.NewCIStr(table))
		if err != nil {
			return errors.Trace(err)
		}
		l.tableBuffers[tableID] = cdclog.NewTableBuffer(tableInfo)
	}
	// restore files
	return l.restoreTables(ctx)
}

func isIngestRetryable(resp *sst.IngestResponse, region *RegionInfo, meta *sst.SSTMeta) (bool, *RegionInfo, error) {
	if resp.GetError() == nil {
		return false, nil, nil
	}

	var newRegion *RegionInfo
	switch errPb := resp.GetError(); {
	case errPb.NotLeader != nil:
		if newLeader := errPb.GetNotLeader().GetLeader(); newLeader != nil {
			newRegion = &RegionInfo{
				Leader: newLeader,
				Region: region.Region,
			}
			return true, newRegion, errors.Errorf("not leader: %s", errPb.GetMessage())
		}
	case errPb.EpochNotMatch != nil:
		if currentRegions := errPb.GetEpochNotMatch().GetCurrentRegions(); currentRegions != nil {
			var currentRegion *metapb.Region
			for _, r := range currentRegions {
				if insideRegion(r, meta) {
					currentRegion = r
					break
				}
			}
			if currentRegion != nil {
				var newLeader *metapb.Peer
				for _, p := range currentRegion.Peers {
					if p.GetStoreId() == region.Leader.GetStoreId() {
						newLeader = p
						break
					}
				}
				if newLeader != nil {
					newRegion = &RegionInfo{
						Leader: newLeader,
						Region: currentRegion,
					}
				}
			}
		}
		return true, newRegion, errors.Errorf("epoch not match: %s", errPb.GetMessage())
	}
	return false, nil, errors.Errorf("non retryable error: %s", resp.GetError().GetMessage())
}

func insideRegion(region *metapb.Region, meta *sst.SSTMeta) bool {
	rg := meta.GetRange()
	return keyInsideRegion(region, rg.GetStart()) && keyInsideRegion(region, rg.GetEnd())
}
