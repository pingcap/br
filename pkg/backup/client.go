// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/pingcap/errors"
	kvproto "github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

// ClientMgr manages connections needed by backup.
type ClientMgr interface {
	GetBackupClient(ctx context.Context, storeID uint64) (kvproto.BackupClient, error)
	GetPDClient() pd.Client
	GetTiKV() tikv.Storage
	GetLockResolver() *tikv.LockResolver
	Close()
}

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	backupFineGrainedMaxBackoff = 80000
)

// Client is a client instructs TiKV how to do a backup.
type Client struct {
	mgr       ClientMgr
	clusterID uint64

	backupMeta kvproto.BackupMeta
	storage    storage.ExternalStorage
	backend    *kvproto.StorageBackend
}

// NewBackupClient returns a new backup client
func NewBackupClient(ctx context.Context, mgr ClientMgr) (*Client, error) {
	log.Info("new backup client")
	pdClient := mgr.GetPDClient()
	clusterID := pdClient.GetClusterID(ctx)
	return &Client{
		clusterID: clusterID,
		mgr:       mgr,
	}, nil
}

// GetTS returns the latest timestamp.
func (bc *Client) GetTS(ctx context.Context, duration time.Duration, ts uint64) (uint64, error) {
	var (
		backupTS uint64
		err      error
	)
	if ts > 0 {
		backupTS = ts
	} else {
		p, l, err := bc.mgr.GetPDClient().GetTS(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		backupTS = oracle.ComposeTS(p, l)

		switch {
		case duration < 0:
			return 0, errors.New("negative timeago is not allowed")
		case duration > 0:
			log.Info("backup time ago", zap.Duration("timeago", duration))

			backupTime := oracle.GetTimeFromTS(backupTS)
			backupAgo := backupTime.Add(-duration)
			if backupTS < oracle.ComposeTS(oracle.GetPhysical(backupAgo), l) {
				return 0, errors.New("backup ts overflow please choose a smaller timeago")
			}
			backupTS = oracle.ComposeTS(oracle.GetPhysical(backupAgo), l)
		}
	}

	// check backup time do not exceed GCSafePoint
	err = CheckGCSafepoint(ctx, bc.mgr.GetPDClient(), backupTS)
	if err != nil {
		return 0, errors.Trace(err)
	}
	log.Info("backup encode timestamp", zap.Uint64("BackupTS", backupTS))
	return backupTS, nil
}

// SetStorage set ExternalStorage for client
func (bc *Client) SetStorage(ctx context.Context, backend *kvproto.StorageBackend, sendCreds bool) error {
	var err error
	bc.storage, err = storage.Create(ctx, backend, sendCreds)
	if err != nil {
		return err
	}
	// backupmeta already exists
	exist, err := bc.storage.FileExists(ctx, utils.MetaFile)
	if err != nil {
		return errors.Annotatef(err, "error occurred when checking %s file", utils.MetaFile)
	}
	if exist {
		return errors.New("backup meta exists, may be some backup files in the path already")
	}
	bc.backend = backend
	return nil
}

// SaveBackupMeta saves the current backup meta at the given path.
func (bc *Client) SaveBackupMeta(ctx context.Context, ddlJobs []*model.Job) error {
	ddlJobsData, err := json.Marshal(ddlJobs)
	if err != nil {
		return errors.Trace(err)
	}
	bc.backupMeta.Ddls = ddlJobsData
	backupMetaData, err := proto.Marshal(&bc.backupMeta)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("backup meta",
		zap.Reflect("meta", bc.backupMeta))
	backendURL := storage.FormatBackendURL(bc.backend)
	log.Info("save backup meta", zap.Stringer("path", &backendURL), zap.Int("jobs", len(ddlJobs)))
	return bc.storage.Write(ctx, utils.MetaFile, backupMetaData)
}

func buildTableRanges(tbl *model.TableInfo) ([]kv.KeyRange, error) {
	pis := tbl.GetPartitionInfo()
	if pis == nil {
		// Short path, no partition.
		return appendRanges(tbl, tbl.ID)
	}

	ranges := make([]kv.KeyRange, 0, len(pis.Definitions)*(len(tbl.Indices)+1)+1)
	for _, def := range pis.Definitions {
		rgs, err := appendRanges(tbl, def.ID)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, rgs...)
	}
	return ranges, nil
}

func appendRanges(tbl *model.TableInfo, tblID int64) ([]kv.KeyRange, error) {
	ranges := ranger.FullIntRange(false)
	kvRanges := distsql.TableRangesToKVRanges(tblID, ranges, nil)
	for _, index := range tbl.Indices {
		if index.State != model.StatePublic {
			continue
		}
		ranges = ranger.FullRange()
		idxRanges, err := distsql.IndexRangesToKVRanges(nil, tblID, index.ID, ranges, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		kvRanges = append(kvRanges, idxRanges...)
	}
	return kvRanges, nil

}

// BuildBackupRangeAndSchema gets the range and schema of tables.
func BuildBackupRangeAndSchema(
	dom *domain.Domain,
	storage kv.Storage,
	tableFilter *filter.Filter,
	backupTS uint64,
) ([]rtree.Range, *Schemas, error) {
	info, err := dom.GetSnapshotInfoSchema(backupTS)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	ranges := make([]rtree.Range, 0)
	backupSchemas := newBackupSchemas()
	for _, dbInfo := range info.AllSchemas() {
		// skip system databases
		if util.IsMemOrSysDB(dbInfo.Name.L) {
			continue
		}

		var dbData []byte
		idAlloc := autoid.NewAllocator(storage, dbInfo.ID, false, autoid.RowIDAllocType)

		for _, tableInfo := range dbInfo.Tables {
			if !tableFilter.Match(&filter.Table{Schema: dbInfo.Name.L, Name: tableInfo.Name.L}) {
				// Skip tables other than the given table.
				continue
			}
			globalAutoID, err := idAlloc.NextGlobalAutoID(tableInfo.ID)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			tableInfo.AutoIncID = globalAutoID
			log.Info("change table AutoIncID",
				zap.Stringer("db", dbInfo.Name),
				zap.Stringer("table", tableInfo.Name),
				zap.Int64("AutoIncID", globalAutoID))

			if dbData == nil {
				dbData, err = json.Marshal(dbInfo)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
			}
			tableData, err := json.Marshal(tableInfo)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			schema := kvproto.Schema{
				Db:    dbData,
				Table: tableData,
			}
			backupSchemas.pushPending(schema, dbInfo.Name.L, tableInfo.Name.L)

			tableRanges, err := buildTableRanges(tableInfo)
			if err != nil {
				return nil, nil, err
			}
			for _, r := range tableRanges {
				ranges = append(ranges, rtree.Range{
					StartKey: r.StartKey,
					EndKey:   r.EndKey,
				})
			}
		}
	}

	if backupSchemas.Len() == 0 {
		return nil, nil, errors.New("nothing to backup")
	}
	return ranges, backupSchemas, nil
}

// GetBackupDDLJobs returns the ddl jobs are done in (lastBackupTS, backupTS]
func GetBackupDDLJobs(dom *domain.Domain, lastBackupTS, backupTS uint64) ([]*model.Job, error) {
	snapMeta, err := dom.GetSnapshotMeta(backupTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	lastSnapMeta, err := dom.GetSnapshotMeta(lastBackupTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	lastSchemaVersion, err := lastSnapMeta.GetSchemaVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	allJobs := make([]*model.Job, 0)
	defaultJobs, err := snapMeta.GetAllDDLJobsInQueue(meta.DefaultJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debug("get default jobs", zap.Int("jobs", len(defaultJobs)))
	allJobs = append(allJobs, defaultJobs...)
	addIndexJobs, err := snapMeta.GetAllDDLJobsInQueue(meta.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debug("get add index jobs", zap.Int("jobs", len(addIndexJobs)))
	allJobs = append(allJobs, addIndexJobs...)
	historyJobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debug("get history jobs", zap.Int("jobs", len(historyJobs)))
	allJobs = append(allJobs, historyJobs...)

	completedJobs := make([]*model.Job, 0)
	for _, job := range allJobs {
		if (job.State == model.JobStateDone || job.State == model.JobStateSynced) &&
			(job.BinlogInfo != nil && job.BinlogInfo.SchemaVersion > lastSchemaVersion) {
			completedJobs = append(completedJobs, job)
		}
	}
	log.Debug("get completed jobs", zap.Int("jobs", len(completedJobs)))
	return completedJobs, nil
}

// BackupRanges make a backup of the given key ranges.
func (bc *Client) BackupRanges(
	ctx context.Context,
	ranges []rtree.Range,
	req kvproto.BackupRequest,
	updateCh chan<- struct{},
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("Backup Ranges", zap.Duration("take", elapsed))
	}()

	errCh := make(chan error)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for _, r := range ranges {
			err := bc.BackupRange(
				ctx, r.StartKey, r.EndKey, req, updateCh)
			if err != nil {
				errCh <- err
				return
			}
		}
		close(errCh)
	}()

	// Check GC safepoint every 5s.
	t := time.NewTicker(time.Second * 5)
	defer t.Stop()

	finished := false
	for {
		err := CheckGCSafepoint(ctx, bc.mgr.GetPDClient(), req.EndVersion)
		if err != nil {
			log.Error("check GC safepoint failed", zap.Error(err))
			return err
		}
		if req.StartVersion > 0 {
			err = CheckGCSafepoint(ctx, bc.mgr.GetPDClient(), req.StartVersion)
			if err != nil {
				log.Error("Check gc safepoint for last backup ts failed", zap.Error(err))
				return err
			}
		}
		if finished {
			// Return error (if there is any) before finishing backup.
			return err
		}
		select {
		case err, ok := <-errCh:
			if !ok {
				// Before finish backup, we have to make sure
				// the backup ts does not fall behind with GC safepoint.
				finished = true
			}
			if err != nil {
				return err
			}
		case <-t.C:
		}
	}
}

// BackupRange make a backup of the given key range.
func (bc *Client) BackupRange(
	ctx context.Context,
	startKey, endKey []byte,
	req kvproto.BackupRequest,
	updateCh chan<- struct{},
) (err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("backup range finished", zap.Duration("take", elapsed))
		key := "range start:" + hex.EncodeToString(startKey) + " end:" + hex.EncodeToString(endKey)
		if err != nil {
			summary.CollectFailureUnit(key, err)
		} else {
			summary.CollectSuccessUnit(key, elapsed)
		}
	}()
	log.Info("backup started",
		zap.Binary("StartKey", startKey),
		zap.Binary("EndKey", endKey),
		zap.Uint64("RateLimit", req.RateLimit),
		zap.Uint32("Concurrency", req.Concurrency))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var allStores []*metapb.Store
	allStores, err = bc.mgr.GetPDClient().GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return errors.Trace(err)
	}

	req.ClusterId = bc.clusterID
	req.StartKey = startKey
	req.EndKey = endKey
	req.StorageBackend = bc.backend

	push := newPushDown(ctx, bc.mgr, len(allStores))

	var results rtree.RangeTree
	results, err = push.pushBackup(req, allStores, updateCh)
	if err != nil {
		return err
	}
	log.Info("finish backup push down", zap.Int("Ok", results.Len()))

	// Find and backup remaining ranges.
	// TODO: test fine grained backup.
	err = bc.fineGrainedBackup(
		ctx, startKey, endKey, req.StartVersion,
		req.EndVersion, req.RateLimit, req.Concurrency, results, updateCh)
	if err != nil {
		return err
	}

	bc.backupMeta.StartVersion = req.StartVersion
	bc.backupMeta.EndVersion = req.EndVersion
	bc.backupMeta.IsRawKv = req.IsRawKv
	if req.IsRawKv {
		bc.backupMeta.RawRanges = append(bc.backupMeta.RawRanges,
			&kvproto.RawRange{StartKey: startKey, EndKey: endKey, Cf: req.Cf})
		log.Info("backup raw ranges",
			zap.ByteString("startKey", startKey),
			zap.ByteString("endKey", endKey),
			zap.String("cf", req.Cf))
	} else {
		log.Info("backup time range",
			zap.Reflect("StartVersion", req.StartVersion),
			zap.Reflect("EndVersion", req.EndVersion))
	}

	results.Ascend(func(i btree.Item) bool {
		r := i.(*rtree.Range)
		bc.backupMeta.Files = append(bc.backupMeta.Files, r.Files...)
		return true
	})

	// Check if there are duplicated files.
	checkDupFiles(&results)

	return nil
}

func (bc *Client) findRegionLeader(
	ctx context.Context,
	key []byte) (*metapb.Peer, error) {
	// Keys are saved in encoded format in TiKV, so the key must be encoded
	// in order to find the correct region.
	key = codec.EncodeBytes([]byte{}, key)
	for i := 0; i < 5; i++ {
		// better backoff.
		_, leader, err := bc.mgr.GetPDClient().GetRegion(ctx, key)
		if err != nil {
			log.Error("find leader failed", zap.Error(err))
			time.Sleep(time.Millisecond * time.Duration(100*i))
			continue
		}
		if leader != nil {
			log.Info("find leader",
				zap.Reflect("Leader", leader), zap.Binary("Key", key))
			return leader, nil
		}
		log.Warn("no region found", zap.Binary("Key", key))
		time.Sleep(time.Millisecond * time.Duration(100*i))
		continue
	}
	return nil, errors.Errorf("can not find leader for key %v", key)
}

func (bc *Client) fineGrainedBackup(
	ctx context.Context,
	startKey, endKey []byte,
	lastBackupTS uint64,
	backupTS uint64,
	rateLimit uint64,
	concurrency uint32,
	rangeTree rtree.RangeTree,
	updateCh chan<- struct{},
) error {
	bo := tikv.NewBackoffer(ctx, backupFineGrainedMaxBackoff)
	for {
		// Step1, check whether there is any incomplete range
		incomplete := rangeTree.GetIncompleteRange(startKey, endKey)
		if len(incomplete) == 0 {
			return nil
		}
		log.Info("start fine grained backup", zap.Int("incomplete", len(incomplete)))
		// Step2, retry backup on incomplete range
		respCh := make(chan *kvproto.BackupResponse, 4)
		errCh := make(chan error, 4)
		retry := make(chan rtree.Range, 4)

		max := &struct {
			ms int
			mu sync.Mutex
		}{}
		wg := new(sync.WaitGroup)
		for i := 0; i < 4; i++ {
			wg.Add(1)
			fork, _ := bo.Fork()
			go func(boFork *tikv.Backoffer) {
				defer wg.Done()
				for rg := range retry {
					backoffMs, err :=
						bc.handleFineGrained(ctx, boFork, rg, lastBackupTS, backupTS, rateLimit, concurrency, respCh)
					if err != nil {
						errCh <- err
						return
					}
					if backoffMs != 0 {
						max.mu.Lock()
						if max.ms < backoffMs {
							max.ms = backoffMs
						}
						max.mu.Unlock()
					}
				}
			}(fork)
		}

		// Dispatch rangs and wait
		go func() {
			for _, rg := range incomplete {
				retry <- rg
			}
			close(retry)
			wg.Wait()
			close(respCh)
		}()

	selectLoop:
		for {
			select {
			case err := <-errCh:
				// TODO: should we handle err here?
				return err
			case resp, ok := <-respCh:
				if !ok {
					// Finished.
					break selectLoop
				}
				if resp.Error != nil {
					log.Fatal("unexpected backup error",
						zap.Reflect("error", resp.Error))
				}
				log.Info("put fine grained range",
					zap.Binary("StartKey", resp.StartKey),
					zap.Binary("EndKey", resp.EndKey),
				)
				rangeTree.Put(resp.StartKey, resp.EndKey, resp.Files)

				// Update progress
				updateCh <- struct{}{}
			}
		}

		// Step3. Backoff if needed, then repeat.
		max.mu.Lock()
		ms := max.ms
		max.mu.Unlock()
		if ms != 0 {
			log.Info("handle fine grained", zap.Int("backoffMs", ms))
			err := bo.BackoffWithMaxSleep(2, /* magic boTxnLockFast */
				ms, errors.New("TODO: attach error"))
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func onBackupResponse(
	bo *tikv.Backoffer,
	backupTS uint64,
	lockResolver *tikv.LockResolver,
	resp *kvproto.BackupResponse,
) (*kvproto.BackupResponse, int, error) {
	log.Debug("onBackupResponse", zap.Reflect("resp", resp))
	if resp.Error == nil {
		return resp, 0, nil
	}
	backoffMs := 0
	switch v := resp.Error.Detail.(type) {
	case *kvproto.Error_KvError:
		if lockErr := v.KvError.Locked; lockErr != nil {
			// Try to resolve lock.
			log.Warn("backup occur kv error", zap.Reflect("error", v))
			msBeforeExpired, _, err1 := lockResolver.ResolveLocks(
				bo, backupTS, []*tikv.Lock{tikv.NewLock(lockErr)})
			if err1 != nil {
				return nil, 0, errors.Trace(err1)
			}
			if msBeforeExpired > 0 {
				backoffMs = int(msBeforeExpired)
			}
			return nil, backoffMs, nil
		}
		// Backup should not meet error other than KeyLocked.
		log.Error("unexpect kv error", zap.Reflect("KvError", v.KvError))
		return nil, backoffMs, errors.Errorf("onBackupResponse error %v", v)

	case *kvproto.Error_RegionError:
		regionErr := v.RegionError
		// Ignore following errors.
		if !(regionErr.EpochNotMatch != nil ||
			regionErr.NotLeader != nil ||
			regionErr.RegionNotFound != nil ||
			regionErr.ServerIsBusy != nil ||
			regionErr.StaleCommand != nil ||
			regionErr.StoreNotMatch != nil) {
			log.Error("unexpect region error",
				zap.Reflect("RegionError", regionErr))
			return nil, backoffMs, errors.Errorf("onBackupResponse error %v", v)
		}
		log.Warn("backup occur region error",
			zap.Reflect("RegionError", regionErr))
		// TODO: a better backoff.
		backoffMs = 1000 /* 1s */
		return nil, backoffMs, nil
	case *kvproto.Error_ClusterIdError:
		log.Error("backup occur cluster ID error",
			zap.Reflect("error", v))
		err := errors.Errorf("%v", resp.Error)
		return nil, 0, err
	default:
		log.Error("backup occur unknown error",
			zap.String("error", resp.Error.GetMsg()))
		err := errors.Errorf("%v", resp.Error)
		return nil, 0, err
	}
}

func (bc *Client) handleFineGrained(
	ctx context.Context,
	bo *tikv.Backoffer,
	rg rtree.Range,
	lastBackupTS uint64,
	backupTS uint64,
	rateLimit uint64,
	concurrency uint32,
	respCh chan<- *kvproto.BackupResponse,
) (int, error) {
	leader, pderr := bc.findRegionLeader(ctx, rg.StartKey)
	if pderr != nil {
		return 0, pderr
	}
	storeID := leader.GetStoreId()
	max := 0

	req := kvproto.BackupRequest{
		ClusterId:      bc.clusterID,
		StartKey:       rg.StartKey, // TODO: the range may cross region.
		EndKey:         rg.EndKey,
		StartVersion:   lastBackupTS,
		EndVersion:     backupTS,
		StorageBackend: bc.backend,
		RateLimit:      rateLimit,
		Concurrency:    concurrency,
	}
	lockResolver := bc.mgr.GetLockResolver()
	client, err := bc.mgr.GetBackupClient(ctx, storeID)
	if err != nil {
		log.Error("fail to connect store", zap.Uint64("StoreID", storeID))
		return 0, errors.Trace(err)
	}
	err = SendBackup(
		ctx, storeID, client, req,
		// Handle responses with the same backoffer.
		func(resp *kvproto.BackupResponse) error {
			response, backoffMs, err1 :=
				onBackupResponse(bo, backupTS, lockResolver, resp)
			if err1 != nil {
				return err1
			}
			if max < backoffMs {
				max = backoffMs
			}
			if response != nil {
				respCh <- response
			}
			return nil
		})
	if err != nil {
		return 0, err
	}
	return max, nil
}

// SendBackup send backup request to the given store.
// Stop receiving response if respFn returns error.
func SendBackup(
	ctx context.Context,
	storeID uint64,
	client kvproto.BackupClient,
	req kvproto.BackupRequest,
	respFn func(*kvproto.BackupResponse) error,
) error {
	log.Info("try backup", zap.Any("backup request", req))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	bcli, err := client.Backup(ctx, &req)
	if err != nil {
		log.Error("fail to backup", zap.Uint64("StoreID", storeID))
		return err
	}
	for {
		resp, err := bcli.Recv()
		if err != nil {
			if err == io.EOF {
				log.Info("backup streaming finish",
					zap.Uint64("StoreID", storeID))
				break
			}
			return errors.Trace(err)
		}
		// TODO: handle errors in the resp.
		log.Info("range backuped",
			zap.Any("StartKey", resp.GetStartKey()),
			zap.Any("EndKey", resp.GetEndKey()))
		err = respFn(resp)
		if err != nil {
			return err
		}
	}
	return nil
}

// FastChecksum check data integrity by xor all(sst_checksum) per table
func (bc *Client) FastChecksum() (bool, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		summary.CollectDuration("backup fast checksum", elapsed)
	}()

	dbs, err := utils.LoadBackupTables(&bc.backupMeta)
	if err != nil {
		return false, err
	}

	for _, schema := range bc.backupMeta.Schemas {
		dbInfo := &model.DBInfo{}
		err = json.Unmarshal(schema.Db, dbInfo)
		if err != nil {
			return false, err
		}
		tblInfo := &model.TableInfo{}
		err = json.Unmarshal(schema.Table, tblInfo)
		if err != nil {
			return false, err
		}
		tbl := dbs[dbInfo.Name.String()].GetTable(tblInfo.Name.String())

		checksum := uint64(0)
		totalKvs := uint64(0)
		totalBytes := uint64(0)
		for _, file := range tbl.Files {
			checksum ^= file.Crc64Xor
			totalKvs += file.TotalKvs
			totalBytes += file.TotalBytes
		}

		summary.CollectSuccessUnit(summary.TotalKV, totalKvs)
		summary.CollectSuccessUnit(summary.TotalBytes, totalBytes)

		if schema.Crc64Xor == checksum && schema.TotalKvs == totalKvs && schema.TotalBytes == totalBytes {
			log.Info("fast checksum success", zap.Stringer("db", dbInfo.Name), zap.Stringer("table", tblInfo.Name))
		} else {
			log.Error("failed in fast checksum",
				zap.String("database", dbInfo.Name.String()),
				zap.String("table", tblInfo.Name.String()),
				zap.Uint64("origin tidb crc64", schema.Crc64Xor),
				zap.Uint64("calculated crc64", checksum),
				zap.Uint64("origin tidb total kvs", schema.TotalKvs),
				zap.Uint64("calculated total kvs", totalKvs),
				zap.Uint64("origin tidb total bytes", schema.TotalBytes),
				zap.Uint64("calculated total bytes", totalBytes),
			)
			return false, nil
		}
	}

	return true, nil
}

// CompleteMeta wait response of admin checksum from TiDB to complete backup meta
func (bc *Client) CompleteMeta(backupSchemas *Schemas) error {
	schemas, err := backupSchemas.finishTableChecksum()
	if err != nil {
		return err
	}
	bc.backupMeta.Schemas = schemas
	return nil
}
