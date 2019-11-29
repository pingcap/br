package backup

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/meta"
	"github.com/pingcap/br/pkg/utils"
)

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	backupFineGrainedMaxBackoff = 80000
)

// Client is a client instructs TiKV how to do a backup.
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	backer    *meta.Backer
	clusterID uint64
	pdClient  pd.Client
	dom       *domain.Domain

	backupMeta backup.BackupMeta
	storage    utils.ExternalStorage
}

// NewBackupClient returns a new backup client
func NewBackupClient(backer *meta.Backer) (*Client, error) {
	log.Info("new backup client")
	ctx, cancel := context.WithCancel(backer.Context())
	pdClient := backer.GetPDClient()
	// Do not run ddl worker in BR.
	ddl.RunWorker = false
	// Do not run stat worker in BR.
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(backer.GetTiKV())
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	return &Client{
		clusterID: pdClient.GetClusterID(ctx),
		backer:    backer,
		ctx:       ctx,
		cancel:    cancel,
		pdClient:  backer.GetPDClient(),
		dom:       dom,
	}, nil
}

// Close a backup client
func (bc *Client) Close() {
	bc.dom.Close()
	bc.cancel()
}

// GetDomain returns a domain
func (bc *Client) GetDomain() *domain.Domain {
	return bc.dom
}

// GetTS returns the latest timestamp.
func (bc *Client) GetTS(timeAgo string) (uint64, error) {
	p, l, err := bc.pdClient.GetTS(bc.ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if timeAgo != "" {
		duration, err := time.ParseDuration(timeAgo)
		if err != nil {
			return 0, errors.Trace(err)
		}
		t := duration.Nanoseconds() / int64(time.Millisecond)
		log.Info("backup time ago", zap.Int64("MillisecondsAgo", t))

		// check backup time do not exceed GCSafePoint
		safePoint, err := bc.backer.GetGCSafePoint(bc.ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if p-t < safePoint.Physical {
			return 0, errors.New("given backup time exceed GCSafePoint")
		}
		p -= t
	}

	ts := meta.Timestamp{
		Physical: p,
		Logical:  l,
	}
	backupTS := meta.EncodeTs(ts)
	log.Info("backup encode timestamp", zap.Uint64("BackupTS", backupTS))
	return backupTS, nil
}

// SetStorage set ExternalStorage for client
func (bc *Client) SetStorage(base string) error {
	var err error
	bc.storage, err = utils.CreateStorage(base)
	if err != nil {
		return err
	}
	// backupmeta already exists
	if exist := bc.storage.FileExists(utils.MetaFile); exist {
		return errors.New("backup meta exists, may be some backup files in the path already")
	}
	return nil
}

// SaveBackupMeta saves the current backup meta at the given path.
func (bc *Client) SaveBackupMeta(path string) error {
	bc.backupMeta.Path = path
	backupMetaData, err := proto.Marshal(&bc.backupMeta)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("backup meta",
		zap.Reflect("meta", bc.backupMeta))
	log.Info("save backup meta", zap.String("path", path))
	return bc.storage.Write(utils.MetaFile, backupMetaData)
}

type tableRange struct {
	startID, endID int64
}

func (tr tableRange) Range() Range {
	startKey := tablecodec.GenTablePrefix(tr.startID)
	endKey := tablecodec.GenTablePrefix(tr.endID)
	return Range{
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
	}
}

func buildTableRanges(tbl *model.TableInfo) []tableRange {
	pis := tbl.GetPartitionInfo()
	if pis == nil {
		// Short path, no partition.
		tableID := tbl.ID
		return []tableRange{{startID: tableID, endID: tableID + 1}}
	}

	ranges := make([]tableRange, 0, len(pis.Definitions))
	for _, def := range pis.Definitions {
		ranges = append(ranges,
			tableRange{
				startID: def.ID,
				endID:   def.ID + 1,
			})
	}
	return ranges
}

// BuildBackupRangeAndSchema gets the range and schema of tables.
func BuildBackupRangeAndSchema(
	dom *domain.Domain,
	storage kv.Storage,
	backupTS uint64,
	dbName, tableName string,
) ([]Range, *Schemas, error) {
	SystemDatabases := [3]string{
		"information_schema",
		"performance_schema",
		"mysql",
	}

	info, err := dom.GetSnapshotInfoSchema(backupTS)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	var dbInfos []*model.DBInfo
	var cTableName model.CIStr
	switch {
	case len(dbName) == 0 && len(tableName) != 0:
		return nil, nil, errors.New("no database is not specified")
	case len(dbName) != 0 && len(tableName) == 0:
		return nil, nil, errors.New("backup database is not supported")
	case len(dbName) != 0 && len(tableName) != 0:
		// backup table
		cTableName = model.NewCIStr(tableName)
		cDBName := model.NewCIStr(dbName)
		dbInfo, exist := info.SchemaByName(cDBName)
		if !exist {
			return nil, nil, errors.Errorf("schema %s not found", dbName)
		}
		dbInfos = append(dbInfos, dbInfo)
	case len(dbName) == 0 && len(tableName) == 0:
		// backup full
		dbInfos = info.AllSchemas()
	}

	ranges := make([]Range, 0)
	backupSchemas := newBackupSchemas()
LoadDb:
	for _, dbInfo := range dbInfos {
		// skip system databases
		for _, sysDbName := range SystemDatabases {
			if sysDbName == dbInfo.Name.L {
				continue LoadDb
			}
		}
		dbData, err := json.Marshal(dbInfo)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		idAlloc := autoid.NewAllocator(storage, dbInfo.ID, false)
		for _, tableInfo := range dbInfo.Tables {
			if len(cTableName.L) != 0 && cTableName.L != tableInfo.Name.L {
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

			tableData, err := json.Marshal(tableInfo)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			schema := backup.Schema{
				Db:    dbData,
				Table: tableData,
			}
			backupSchemas.pushPending(schema, dbInfo.Name.L, tableInfo.Name.L)

			// TODO: We may need to include [t<tableID>, t<tableID+1>)
			//       in order to backup global index.
			tableRanges := buildTableRanges(tableInfo)
			for _, r := range tableRanges {
				ranges = append(ranges, r.Range())
			}
		}
	}

	if len(cTableName.L) != 0 {
		// Must find the given table.
		if backupSchemas.Len() == 0 {
			return nil, nil, errors.Errorf("table %s not found", cTableName)
		}
	}
	return ranges, backupSchemas, nil
}

// BackupRanges make a backup of the given key ranges.
func (bc *Client) BackupRanges(
	ranges []Range,
	path string,
	backupTS uint64,
	rate uint64,
	concurrency uint32,
	updateCh chan<- struct{},
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("Backup Ranges", zap.Duration("take", elapsed))
	}()

	errCh := make(chan error)
	ctx, cancel := context.WithCancel(bc.ctx)
	defer cancel()
	go func() {
		for _, r := range ranges {
			err := bc.backupRange(
				ctx, r.StartKey, r.EndKey, path, backupTS, rate, concurrency, updateCh)
			if err != nil {
				errCh <- err
				return
			}
		}
		close(errCh)
	}()

	// Check GC safepoint every 30s.
	t := time.NewTicker(time.Second * 30)
	defer t.Stop()

	finished := false
	for {
		err := bc.backer.CheckGCSafepoint(ctx, backupTS)
		if err != nil {
			// Ignore the error since it retries every 30s.
			log.Warn("get GC safepoint failed", zap.Error(err))
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

// backupRange make a backup of the given key range.
func (bc *Client) backupRange(
	ctx context.Context,
	startKey, endKey []byte,
	path string,
	backupTS uint64,
	rateMBs uint64,
	concurrency uint32,
	updateCh chan<- struct{},
) error {
	// The unit of rate limit in protocol is bytes per second.
	rateLimit := rateMBs * 1024 * 1024
	log.Info("backup started",
		zap.Binary("StartKey", startKey),
		zap.Binary("EndKey", endKey),
		zap.Uint64("RateLimit", rateMBs),
		zap.Uint32("Concurrency", concurrency))
	start := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	allStores, err := bc.pdClient.GetAllStores(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	req := backup.BackupRequest{
		ClusterId:    bc.clusterID,
		StartKey:     startKey,
		EndKey:       endKey,
		StartVersion: backupTS,
		EndVersion:   backupTS,
		Path:         path,
		RateLimit:    rateLimit,
		Concurrency:  concurrency,
	}
	push := newPushDown(ctx, bc.backer, len(allStores))

	results, err := push.pushBackup(req, allStores, updateCh)
	if err != nil {
		return err
	}
	log.Info("finish backup push down", zap.Int("Ok", results.len()))

	// Find and backup remaining ranges.
	// TODO: test fine grained backup.
	err = bc.fineGrainedBackup(
		startKey, endKey,
		backupTS, path, rateLimit, concurrency, results, updateCh)
	if err != nil {
		return err
	}

	bc.backupMeta.StartVersion = backupTS
	bc.backupMeta.EndVersion = backupTS
	log.Info("backup time range",
		zap.Reflect("StartVersion", backupTS),
		zap.Reflect("EndVersion", backupTS))

	results.tree.Ascend(func(i btree.Item) bool {
		r := i.(*Range)
		bc.backupMeta.Files = append(bc.backupMeta.Files, r.Files...)
		return true
	})

	// Check if there are duplicated files.
	results.checkDupFiles()

	log.Info("backup range finished",
		zap.Duration("take", time.Since(start)))
	return nil
}

func (bc *Client) findRegionLeader(key []byte) (*metapb.Peer, error) {
	// Keys are saved in encoded format in TiKV, so the key must be encoded
	// in order to find the correct region.
	key = codec.EncodeBytes([]byte{}, key)
	for i := 0; i < 5; i++ {
		// better backoff.
		_, leader, err := bc.pdClient.GetRegion(bc.ctx, key)
		if err != nil {
			log.Error("find region failed", zap.Error(err))
			time.Sleep(time.Millisecond * time.Duration(100*i))
			continue
		}
		if leader != nil {
			log.Info("find region",
				zap.Reflect("Leader", leader), zap.Binary("Key", key))
			return leader, nil
		}
		log.Warn("no region found", zap.Binary("Key", key))
		time.Sleep(time.Millisecond * time.Duration(100*i))
		continue
	}
	return nil, errors.Errorf("can not find region for key %v", key)
}

func (bc *Client) fineGrainedBackup(
	startKey, endKey []byte,
	backupTS uint64,
	path string,
	rateLimit uint64,
	concurrency uint32,
	rangeTree RangeTree,
	updateCh chan<- struct{},
) error {
	bo := tikv.NewBackoffer(bc.ctx, backupFineGrainedMaxBackoff)
	for {
		// Step1, check whether there is any incomplete range
		incomplete := rangeTree.getIncompleteRange(startKey, endKey)
		if len(incomplete) == 0 {
			return nil
		}
		log.Info("start fine grained backup", zap.Int("incomplete", len(incomplete)))
		// Step2, retry backup on incomplete range
		respCh := make(chan *backup.BackupResponse, 4)
		errCh := make(chan error, 4)
		retry := make(chan Range, 4)

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
						bc.handleFineGrained(boFork, rg, backupTS, path, rateLimit, concurrency, respCh)
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
				rangeTree.putOk(resp.StartKey, resp.EndKey, resp.Files)

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
	lockResolver *tikv.LockResolver,
	resp *backup.BackupResponse,
) (*backup.BackupResponse, int, error) {
	log.Debug("onBackupResponse", zap.Reflect("resp", resp))
	if resp.Error == nil {
		return resp, 0, nil
	}
	backoffMs := 0
	switch v := resp.Error.Detail.(type) {
	case *backup.Error_KvError:
		if lockErr := v.KvError.Locked; lockErr != nil {
			// Try to resolve lock.
			log.Warn("backup occur kv error", zap.Reflect("error", v))
			msBeforeExpired, err1 := lockResolver.ResolveLocks(
				bo, []*tikv.Lock{tikv.NewLock(lockErr)})
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

	case *backup.Error_RegionError:
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
	case *backup.Error_ClusterIdError:
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
	bo *tikv.Backoffer,
	rg Range,
	backupTS uint64,
	path string,
	rateLimit uint64,
	concurrency uint32,
	respCh chan<- *backup.BackupResponse,
) (int, error) {
	leader, pderr := bc.findRegionLeader(rg.StartKey)
	if pderr != nil {
		return 0, pderr
	}
	max := 0
	req := backup.BackupRequest{
		ClusterId:    bc.clusterID,
		StartKey:     rg.StartKey, // TODO: the range may cross region.
		EndKey:       rg.EndKey,
		StartVersion: backupTS,
		EndVersion:   backupTS,
		Path:         path,
		RateLimit:    rateLimit,
		Concurrency:  concurrency,
	}
	lockResolver := bc.backer.GetLockResolver()
	err := bc.backer.SendBackup(
		bc.ctx, leader.GetStoreId(), req,
		// Handle responses with the same backoffer.
		func(resp *backup.BackupResponse) error {
			response, backoffMs, err :=
				onBackupResponse(bo, lockResolver, resp)
			if err != nil {
				return err
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

// GetRangeRegionCount get region count by pd http api
func (bc *Client) GetRangeRegionCount(startKey, endKey []byte) (int, error) {
	return bc.backer.GetRegionCount()
}

// FastChecksum check data integrity by xor all(sst_checksum) per table
func (bc *Client) FastChecksum() (bool, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("Backup Checksum", zap.Duration("take", elapsed))
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
