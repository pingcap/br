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
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
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
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/glue"
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

// Checksum is the checksum of some backup files calculated by CollectChecksums.
type Checksum struct {
	Crc64Xor   uint64
	TotalKvs   uint64
	TotalBytes uint64
}

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	backupFineGrainedMaxBackoff = 80000
)

// Client is a client instructs TiKV how to do a backup.
type Client struct {
	mgr       ClientMgr
	clusterID uint64

	storage storage.ExternalStorage
	backend *kvproto.StorageBackend

	gcTTL int64
}

// NewBackupClient returns a new backup client.
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
	err = CheckGCSafePoint(ctx, bc.mgr.GetPDClient(), backupTS)
	if err != nil {
		return 0, errors.Trace(err)
	}
	log.Info("backup encode timestamp", zap.Uint64("BackupTS", backupTS))
	return backupTS, nil
}

// SetLockFile set write lock file.
func (bc *Client) SetLockFile(ctx context.Context) error {
	return bc.storage.Write(ctx, utils.LockFile,
		[]byte("DO NOT DELETE\n"+
			"This file exists to remind other backup jobs won't use this path"))
}

// SetGCTTL set gcTTL for client.
func (bc *Client) SetGCTTL(ttl int64) {
	bc.gcTTL = ttl
}

// GetGCTTL get gcTTL for this backup.
func (bc *Client) GetGCTTL() int64 {
	return bc.gcTTL
}

// SetStorage set ExternalStorage for client.
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
	exist, err = bc.storage.FileExists(ctx, utils.LockFile)
	if err != nil {
		return errors.Annotatef(err, "error occurred when checking %s file", utils.LockFile)
	}
	if exist {
		return errors.New("backup lock exists, may be some backup files in the path already")
	}
	bc.backend = backend
	return nil
}

// BuildBackupMeta constructs the backup meta file from its components.
func BuildBackupMeta(
	req *kvproto.BackupRequest,
	files []*kvproto.File,
	rawRanges []*kvproto.RawRange,
	ddlJobs []*model.Job,
) (backupMeta kvproto.BackupMeta, err error) {
	backupMeta.StartVersion = req.StartVersion
	backupMeta.EndVersion = req.EndVersion
	backupMeta.IsRawKv = req.IsRawKv
	backupMeta.RawRanges = rawRanges
	backupMeta.Files = files
	backupMeta.Ddls, err = json.Marshal(ddlJobs)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	return
}

// SaveBackupMeta saves the current backup meta at the given path.
func (bc *Client) SaveBackupMeta(ctx context.Context, backupMeta *kvproto.BackupMeta) error {
	backupMetaData, err := proto.Marshal(backupMeta)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("backup meta", zap.Reflect("meta", backupMeta))
	backendURL := storage.FormatBackendURL(bc.backend)
	log.Info("save backup meta", zap.Stringer("path", &backendURL), zap.Int("size", len(backupMetaData)))
	return bc.storage.Write(ctx, utils.MetaFile, backupMetaData)
}

// BuildTableRanges returns the key ranges encompassing the entire table,
// and its partitions if exists.
func BuildTableRanges(tbl *model.TableInfo) ([]kv.KeyRange, error) {
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
	tableFilter filter.Filter,
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
		seqAlloc := autoid.NewAllocator(storage, dbInfo.ID, false, autoid.SequenceType)
		randAlloc := autoid.NewAllocator(storage, dbInfo.ID, false, autoid.AutoRandomType)

		for _, tableInfo := range dbInfo.Tables {
			if !tableFilter.MatchTable(dbInfo.Name.O, tableInfo.Name.O) {
				// Skip tables other than the given table.
				continue
			}
			var globalAutoID int64
			switch {
			case tableInfo.IsSequence():
				globalAutoID, err = seqAlloc.NextGlobalAutoID(tableInfo.ID)
			case tableInfo.IsView():
				// no auto ID for views.
			default:
				globalAutoID, err = idAlloc.NextGlobalAutoID(tableInfo.ID)
			}
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			tableInfo.AutoIncID = globalAutoID

			if tableInfo.PKIsHandle && tableInfo.ContainsAutoRandomBits() {
				// this table has auto_random id, we need backup and rebase in restoration
				var globalAutoRandID int64
				globalAutoRandID, err = randAlloc.NextGlobalAutoID(tableInfo.ID)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				tableInfo.AutoRandID = globalAutoRandID
				log.Info("change table AutoRandID",
					zap.Stringer("db", dbInfo.Name),
					zap.Stringer("table", tableInfo.Name),
					zap.Int64("AutoRandID", globalAutoRandID))
			}
			log.Info("change table AutoIncID",
				zap.Stringer("db", dbInfo.Name),
				zap.Stringer("table", tableInfo.Name),
				zap.Int64("AutoIncID", globalAutoID))

			// remove all non-public indices
			n := 0
			for _, index := range tableInfo.Indices {
				if index.State == model.StatePublic {
					tableInfo.Indices[n] = index
					n++
				}
			}
			tableInfo.Indices = tableInfo.Indices[:n]

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

			tableRanges, err := BuildTableRanges(tableInfo)
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
		log.Info("nothing to backup")
		return nil, nil, nil
	}
	return ranges, backupSchemas, nil
}

// GetBackupDDLJobs returns the ddl jobs are done in (lastBackupTS, backupTS].
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
	concurrency uint,
	updateCh glue.Progress,
) ([]*kvproto.File, error) {
	errCh := make(chan error)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// we collect all files in a single goroutine to avoid thread safety issues.
	filesCh := make(chan []*kvproto.File, concurrency)
	allFiles := make([]*kvproto.File, 0, len(ranges))
	go func() {
		init := time.Now()
		start, cur := init, init
		for files := range filesCh {
			cur, start = start, time.Now()
			allFiles = append(allFiles, files...)
			summary.CollectSuccessUnit("backup ranges", 1, start.Sub(cur))
		}
		log.Info("Backup Ranges", zap.Duration("take", cur.Sub(init)))
	}()

	go func() {
		defer close(filesCh)
		workerPool := utils.NewWorkerPool(concurrency, "Ranges")
		eg, ectx := errgroup.WithContext(ctx)
		for _, r := range ranges {
			sk, ek := r.StartKey, r.EndKey
			workerPool.ApplyOnErrorGroup(eg, func() error {
				files, err := bc.BackupRange(ectx, sk, ek, req, updateCh)
				if err == nil {
					filesCh <- files
				}
				return err
			})
		}
		if err := eg.Wait(); err != nil {
			errCh <- err
			return
		}
		close(errCh)
	}()

	// Check GC safepoint every 5s.
	t := time.NewTicker(time.Second * 5)
	defer t.Stop()

	backupTS := req.EndVersion
	// use lastBackupTS as safePoint if exists
	if req.StartVersion > 0 {
		backupTS = req.StartVersion
	}

	log.Info("current backup safePoint job",
		zap.Uint64("backupTS", backupTS))

	finished := false
	for {
		err := UpdateServiceSafePoint(ctx, bc.mgr.GetPDClient(), bc.GetGCTTL(), backupTS)
		if err != nil {
			log.Error("update GC safePoint with TTL failed", zap.Error(err))
			return nil, err
		}
		err = CheckGCSafePoint(ctx, bc.mgr.GetPDClient(), backupTS)
		if err != nil {
			log.Error("check GC safePoint failed", zap.Error(err))
			return nil, err
		}
		if finished {
			// Return error (if there is any) before finishing backup.
			return allFiles, err
		}
		select {
		case err, ok := <-errCh:
			if !ok {
				// Before finish backup, we have to make sure
				// the backup ts does not fall behind with GC safepoint.
				finished = true
			}
			if err != nil {
				return nil, err
			}
		case <-t.C:
		}
	}
}

// BackupRange make a backup of the given key range.
// Returns an array of files backed up.
func (bc *Client) BackupRange(
	ctx context.Context,
	startKey, endKey []byte,
	req kvproto.BackupRequest,
	updateCh glue.Progress,
) (files []*kvproto.File, err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("backup range finished", zap.Duration("take", elapsed))
		key := "range start:" + hex.EncodeToString(startKey) + " end:" + hex.EncodeToString(endKey)
		if err != nil {
			summary.CollectFailureUnit(key, err)
		}
	}()
	log.Info("backup started",
		zap.Stringer("StartKey", utils.WrapKey(startKey)),
		zap.Stringer("EndKey", utils.WrapKey(endKey)),
		zap.Uint64("RateLimit", req.RateLimit),
		zap.Uint32("Concurrency", req.Concurrency))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var allStores []*metapb.Store
	allStores, err = conn.GetAllTiKVStores(ctx, bc.mgr.GetPDClient(), conn.SkipTiFlash)
	if err != nil {
		return nil, errors.Trace(err)
	}

	req.ClusterId = bc.clusterID
	req.StartKey = startKey
	req.EndKey = endKey
	req.StorageBackend = bc.backend

	push := newPushDown(ctx, bc.mgr, len(allStores))

	var results rtree.RangeTree
	results, err = push.pushBackup(req, allStores, updateCh)
	if err != nil {
		return nil, err
	}
	log.Info("finish backup push down", zap.Int("Ok", results.Len()))

	// Find and backup remaining ranges.
	// TODO: test fine grained backup.
	err = bc.fineGrainedBackup(
		ctx, startKey, endKey, req.StartVersion,
		req.EndVersion, req.RateLimit, req.Concurrency, results, updateCh)
	if err != nil {
		return nil, err
	}

	if req.IsRawKv {
		log.Info("backup raw ranges",
			zap.Stringer("startKey", utils.WrapKey(startKey)),
			zap.Stringer("endKey", utils.WrapKey(endKey)),
			zap.String("cf", req.Cf))
	} else {
		log.Info("backup time range",
			zap.Reflect("StartVersion", req.StartVersion),
			zap.Reflect("EndVersion", req.EndVersion))
	}

	results.Ascend(func(i btree.Item) bool {
		r := i.(*rtree.Range)
		files = append(files, r.Files...)
		return true
	})

	// Check if there are duplicated files.
	checkDupFiles(&results)
	collectFileInfo(files)

	return files, nil
}

func (bc *Client) findRegionLeader(
	ctx context.Context,
	key []byte) (*metapb.Peer, error) {
	// Keys are saved in encoded format in TiKV, so the key must be encoded
	// in order to find the correct region.
	key = codec.EncodeBytes([]byte{}, key)
	for i := 0; i < 5; i++ {
		// better backoff.
		region, err := bc.mgr.GetPDClient().GetRegion(ctx, key)
		if err != nil {
			log.Error("find leader failed", zap.Error(err))
			time.Sleep(time.Millisecond * time.Duration(100*i))
			continue
		}
		if region.Leader != nil {
			log.Info("find leader",
				zap.Reflect("Leader", region.Leader), zap.Stringer("Key", utils.WrapKey(key)))
			return region.Leader, nil
		}
		log.Warn("no region found", zap.Stringer("Key", utils.WrapKey(key)))
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
	updateCh glue.Progress,
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
					zap.Stringer("StartKey", utils.WrapKey(resp.StartKey)),
					zap.Stringer("EndKey", utils.WrapKey(resp.EndKey)),
				)
				rangeTree.Put(resp.StartKey, resp.EndKey, resp.Files)

				// Update progress
				updateCh.Inc()
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
	log.Info("try backup",
		zap.Stringer("StartKey", utils.WrapKey(req.StartKey)),
		zap.Stringer("EndKey", utils.WrapKey(req.EndKey)),
		zap.Uint64("storeID", storeID),
	)
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
			zap.Stringer("StartKey", utils.WrapKey(resp.GetStartKey())),
			zap.Stringer("EndKey", utils.WrapKey(req.GetEndKey())))
		err = respFn(resp)
		if err != nil {
			return err
		}
	}
	return nil
}

// ChecksumMatches tests whether the "local" checksum matches the checksum from TiKV.
func ChecksumMatches(backupMeta *kvproto.BackupMeta, local []Checksum) (bool, error) {
	if len(local) != len(backupMeta.Schemas) {
		return false, nil
	}

	for i, schema := range backupMeta.Schemas {
		localChecksum := local[i]
		dbInfo := &model.DBInfo{}
		err := json.Unmarshal(schema.Db, dbInfo)
		if err != nil {
			log.Error("failed in fast checksum, and cannot parse db info.")
			return false, err
		}
		tblInfo := &model.TableInfo{}
		err = json.Unmarshal(schema.Table, tblInfo)
		if err != nil {
			log.Error("failed in fast checksum, and cannot parse table info.")
			return false, err
		}
		if localChecksum.Crc64Xor != schema.Crc64Xor ||
			localChecksum.TotalBytes != schema.TotalBytes ||
			localChecksum.TotalKvs != schema.TotalKvs {
			log.Error("failed in fast checksum",
				zap.Stringer("db", dbInfo.Name),
				zap.Stringer("table", tblInfo.Name),
				zap.Uint64("origin tidb crc64", schema.Crc64Xor),
				zap.Uint64("calculated crc64", localChecksum.Crc64Xor),
				zap.Uint64("origin tidb total kvs", schema.TotalKvs),
				zap.Uint64("calculated total kvs", localChecksum.TotalKvs),
				zap.Uint64("origin tidb total bytes", schema.TotalBytes),
				zap.Uint64("calculated total bytes", localChecksum.TotalBytes),
			)
			return false, nil
		}
		log.Info("fast checksum success",
			zap.String("database", dbInfo.Name.L),
			zap.String("table", tblInfo.Name.L))
	}
	return true, nil
}

// collectFileInfo collects ungrouped file summary information, like kv count and size.
func collectFileInfo(files []*kvproto.File) {
	for _, file := range files {
		summary.CollectSuccessUnit(summary.TotalKV, 1, file.TotalKvs)
		summary.CollectSuccessUnit(summary.TotalBytes, 1, file.TotalBytes)
	}
}

// CollectChecksums check data integrity by xor all(sst_checksum) per table
// it returns the checksum of all local files.
func CollectChecksums(backupMeta *kvproto.BackupMeta) ([]Checksum, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		summary.CollectDuration("backup fast checksum", elapsed)
	}()

	dbs, err := utils.LoadBackupTables(backupMeta)
	if err != nil {
		return nil, err
	}

	checksums := make([]Checksum, 0, len(backupMeta.Schemas))
	for _, schema := range backupMeta.Schemas {
		dbInfo := &model.DBInfo{}
		err = json.Unmarshal(schema.Db, dbInfo)
		if err != nil {
			return nil, err
		}
		tblInfo := &model.TableInfo{}
		err = json.Unmarshal(schema.Table, tblInfo)
		if err != nil {
			return nil, err
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

		summary.CollectSuccessUnit(summary.TotalKV, 1, totalKvs)
		summary.CollectSuccessUnit(summary.TotalBytes, 1, totalBytes)
		log.Info("fast checksum calculated", zap.Stringer("db", dbInfo.Name), zap.Stringer("table", tblInfo.Name))
		localChecksum := Checksum{
			Crc64Xor:   checksum,
			TotalKvs:   totalKvs,
			TotalBytes: totalBytes,
		}
		checksums = append(checksums, localChecksum)
	}

	return checksums, nil
}

// FilterSchema filter in-place schemas that doesn't have backup files
// this is useful during incremental backup, no files in backup means no files to restore
// so we can skip some DDL in restore to speed up restoration.
func FilterSchema(backupMeta *kvproto.BackupMeta) error {
	dbs, err := utils.LoadBackupTables(backupMeta)
	if err != nil {
		return err
	}
	schemas := make([]*kvproto.Schema, 0, len(backupMeta.Schemas))
	for _, schema := range backupMeta.Schemas {
		dbInfo := &model.DBInfo{}
		err := json.Unmarshal(schema.Db, dbInfo)
		if err != nil {
			return err
		}
		tblInfo := &model.TableInfo{}
		err = json.Unmarshal(schema.Table, tblInfo)
		if err != nil {
			return err
		}
		tbl := dbs[dbInfo.Name.String()].GetTable(tblInfo.Name.String())
		if len(tbl.Files) > 0 {
			schemas = append(schemas, schema)
		}
	}
	backupMeta.Schemas = schemas
	return nil
}
