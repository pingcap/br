// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/br/pkg/checksum"
	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

// defaultChecksumConcurrency is the default number of the concurrent
// checksum tasks.
const defaultChecksumConcurrency = 64

// Client sends requests to restore files.
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	pdClient     pd.Client
	toolClient   SplitClient
	fileImporter FileImporter
	workerPool   *utils.WorkerPool
	tlsConf      *tls.Config

	databases  map[string]*utils.Database
	ddlJobs    []*model.Job
	backupMeta *backup.BackupMeta
	// TODO Remove this field or replace it with a []*DB,
	// since https://github.com/pingcap/br/pull/377 needs more DBs to speed up DDL execution.
	// And for now, we must inject a pool of DBs to `Client.GoCreateTables`, otherwise there would be a race condition.
	// Which is dirty: why we need DBs from different sources?
	// By replace it with a []*DB, we can remove the dirty parameter of `Client.GoCreateTable`,
	// along with them in some private functions.
	// Before you do it, you can firstly read discussions at
	// https://github.com/pingcap/br/pull/377#discussion_r446594501,
	// this probably isn't as easy and it seems like (however, not hard, too :D)
	db              *DB
	rateLimit       uint64
	isOnline        bool
	noSchema        bool
	hasSpeedLimited bool
	// Those fields should be removed after we have FULLY supportted TiFlash.
	// we place this field here to make a 'good' memory align, but mainly make golang-ci happy :)
	tiFlashRecordUpdated bool

	restoreStores []uint64

	// tables that has TiFlash and those TiFlash have been removed, should be written to disk.
	// Those fields should be removed after we have FULLY supportted TiFlash.
	tablesRemovedTiFlash []*backup.Schema

	storage            storage.ExternalStorage
	backend            *backup.StorageBackend
	switchModeInterval time.Duration
	switchCh           chan struct{}
}

// NewRestoreClient returns a new RestoreClient.
func NewRestoreClient(
	ctx context.Context,
	g glue.Glue,
	pdClient pd.Client,
	store kv.Storage,
	tlsConf *tls.Config,
) (*Client, error) {
	ctx, cancel := context.WithCancel(ctx)
	db, err := NewDB(g, store)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	return &Client{
		ctx:        ctx,
		cancel:     cancel,
		pdClient:   pdClient,
		toolClient: NewSplitClient(pdClient, tlsConf),
		db:         db,
		tlsConf:    tlsConf,
		switchCh:   make(chan struct{}),
	}, nil
}

// SetRateLimit to set rateLimit.
func (rc *Client) SetRateLimit(rateLimit uint64) {
	rc.rateLimit = rateLimit
}

// SetStorage set ExternalStorage for client.
func (rc *Client) SetStorage(ctx context.Context, backend *backup.StorageBackend, sendCreds bool) error {
	var err error
	rc.storage, err = storage.Create(ctx, backend, sendCreds)
	if err != nil {
		return err
	}
	rc.backend = backend
	return nil
}

// GetPDClient returns a pd client.
func (rc *Client) GetPDClient() pd.Client {
	return rc.pdClient
}

// IsOnline tells if it's a online restore.
func (rc *Client) IsOnline() bool {
	return rc.isOnline
}

// SetSwitchModeInterval set switch mode interval for client.
func (rc *Client) SetSwitchModeInterval(interval time.Duration) {
	rc.switchModeInterval = interval
}

// Close a client.
func (rc *Client) Close() {
	// rc.db can be nil in raw kv mode.
	if rc.db != nil {
		rc.db.Close()
	}
	rc.cancel()
	log.Info("Restore client closed")
}

// InitBackupMeta loads schemas from BackupMeta to initialize RestoreClient.
func (rc *Client) InitBackupMeta(backupMeta *backup.BackupMeta, backend *backup.StorageBackend) error {
	if !backupMeta.IsRawKv {
		databases, err := utils.LoadBackupTables(backupMeta)
		if err != nil {
			return errors.Trace(err)
		}
		rc.databases = databases

		var ddlJobs []*model.Job
		err = json.Unmarshal(backupMeta.GetDdls(), &ddlJobs)
		if err != nil {
			return errors.Trace(err)
		}
		rc.ddlJobs = ddlJobs
	}
	rc.backupMeta = backupMeta
	log.Info("load backupmeta", zap.Int("databases", len(rc.databases)), zap.Int("jobs", len(rc.ddlJobs)))

	metaClient := NewSplitClient(rc.pdClient, rc.tlsConf)
	importClient := NewImportClient(metaClient, rc.tlsConf)
	rc.fileImporter = NewFileImporter(rc.ctx, metaClient, importClient, backend, backupMeta.IsRawKv, rc.rateLimit)
	return nil
}

// IsRawKvMode checks whether the backup data is in raw kv format, in which case transactional recover is forbidden.
func (rc *Client) IsRawKvMode() bool {
	return rc.backupMeta.IsRawKv
}

// GetFilesInRawRange gets all files that are in the given range or intersects with the given range.
func (rc *Client) GetFilesInRawRange(startKey []byte, endKey []byte, cf string) ([]*backup.File, error) {
	if !rc.IsRawKvMode() {
		return nil, errors.New("the backup data is not in raw kv mode")
	}

	for _, rawRange := range rc.backupMeta.RawRanges {
		// First check whether the given range is backup-ed. If not, we cannot perform the restore.
		if rawRange.Cf != cf {
			continue
		}

		if (len(rawRange.EndKey) > 0 && bytes.Compare(startKey, rawRange.EndKey) >= 0) ||
			(len(endKey) > 0 && bytes.Compare(rawRange.StartKey, endKey) >= 0) {
			// The restoring range is totally out of the current range. Skip it.
			continue
		}

		if bytes.Compare(startKey, rawRange.StartKey) < 0 ||
			utils.CompareEndKey(endKey, rawRange.EndKey) > 0 {
			// Only partial of the restoring range is in the current backup-ed range. So the given range can't be fully
			// restored.
			return nil, errors.New("the given range to restore is not fully covered by the range that was backed up")
		}

		// We have found the range that contains the given range. Find all necessary files.
		files := make([]*backup.File, 0)

		for _, file := range rc.backupMeta.Files {
			if file.Cf != cf {
				continue
			}

			if len(file.EndKey) > 0 && bytes.Compare(file.EndKey, startKey) < 0 {
				// The file is before the range to be restored.
				continue
			}
			if len(endKey) > 0 && bytes.Compare(endKey, file.StartKey) <= 0 {
				// The file is after the range to be restored.
				// The specified endKey is exclusive, so when it equals to a file's startKey, the file is still skipped.
				continue
			}

			files = append(files, file)
		}

		// There should be at most one backed up range that covers the restoring range.
		return files, nil
	}

	return nil, errors.New("no backup data in the range")
}

// SetConcurrency sets the concurrency of dbs tables files.
func (rc *Client) SetConcurrency(c uint) {
	rc.workerPool = utils.NewWorkerPool(c, "file")
}

// EnableOnline sets the mode of restore to online.
func (rc *Client) EnableOnline() {
	rc.isOnline = true
}

// GetTLSConfig returns the tls config.
func (rc *Client) GetTLSConfig() *tls.Config {
	return rc.tlsConf
}

// GetTS gets a new timestamp from PD.
func (rc *Client) GetTS(ctx context.Context) (uint64, error) {
	p, l, err := rc.pdClient.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	restoreTS := oracle.ComposeTS(p, l)
	return restoreTS, nil
}

// ResetTS resets the timestamp of PD to a bigger value.
func (rc *Client) ResetTS(pdAddrs []string) error {
	restoreTS := rc.backupMeta.GetEndVersion()
	log.Info("reset pd timestamp", zap.Uint64("ts", restoreTS))
	i := 0
	return utils.WithRetry(rc.ctx, func() error {
		idx := i % len(pdAddrs)
		i++
		return utils.ResetTS(pdAddrs[idx], restoreTS, rc.tlsConf)
	}, newPDReqBackoffer())
}

// GetPlacementRules return the current placement rules.
func (rc *Client) GetPlacementRules(pdAddrs []string) ([]placement.Rule, error) {
	var placementRules []placement.Rule
	i := 0
	errRetry := utils.WithRetry(rc.ctx, func() error {
		var err error
		idx := i % len(pdAddrs)
		i++
		placementRules, err = utils.GetPlacementRules(pdAddrs[idx], rc.tlsConf)
		return err
	}, newPDReqBackoffer())
	return placementRules, errRetry
}

// GetDatabases returns all databases.
func (rc *Client) GetDatabases() []*utils.Database {
	dbs := make([]*utils.Database, 0, len(rc.databases))
	for _, db := range rc.databases {
		dbs = append(dbs, db)
	}
	return dbs
}

// GetDatabase returns a database by name.
func (rc *Client) GetDatabase(name string) *utils.Database {
	return rc.databases[name]
}

// GetDDLJobs returns ddl jobs.
func (rc *Client) GetDDLJobs() []*model.Job {
	return rc.ddlJobs
}

// GetTableSchema returns the schema of a table from TiDB.
func (rc *Client) GetTableSchema(
	dom *domain.Domain,
	dbName model.CIStr,
	tableName model.CIStr,
) (*model.TableInfo, error) {
	info, err := dom.GetSnapshotInfoSchema(math.MaxInt64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	table, err := info.TableByName(dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return table.Meta(), nil
}

// CreateDatabase creates a database.
func (rc *Client) CreateDatabase(db *model.DBInfo) error {
	if rc.IsSkipCreateSQL() {
		log.Info("skip create database", zap.Stringer("database", db.Name))
		return nil
	}
	return rc.db.CreateDatabase(rc.ctx, db)
}

// CreateTables creates multiple tables, and returns their rewrite rules.
func (rc *Client) CreateTables(
	dom *domain.Domain,
	tables []*utils.Table,
	newTS uint64,
) (*RewriteRules, []*model.TableInfo, error) {
	rewriteRules := &RewriteRules{
		Table: make([]*import_sstpb.RewriteRule, 0),
		Data:  make([]*import_sstpb.RewriteRule, 0),
	}
	newTables := make([]*model.TableInfo, 0, len(tables))
	errCh := make(chan error, 1)
	tbMapping := map[string]int{}
	for i, t := range tables {
		tbMapping[t.Info.Name.String()] = i
	}
	dataCh := rc.GoCreateTables(context.TODO(), dom, tables, newTS, nil, errCh)
	for et := range dataCh {
		rules := et.RewriteRule
		rewriteRules.Table = append(rewriteRules.Table, rules.Table...)
		rewriteRules.Data = append(rewriteRules.Data, rules.Data...)
		newTables = append(newTables, et.Table)
	}
	// Let's ensure that it won't break the original order.
	sort.Slice(newTables, func(i, j int) bool {
		return tbMapping[newTables[i].Name.String()] < tbMapping[newTables[j].Name.String()]
	})

	select {
	case err, ok := <-errCh:
		if ok {
			return nil, nil, err
		}
	default:
	}
	return rewriteRules, newTables, nil
}

func (rc *Client) createTable(
	ctx context.Context,
	db *DB,
	dom *domain.Domain,
	table *utils.Table,
	newTS uint64,
) (CreatedTable, error) {
	if db == nil {
		db = rc.db
	}

	if rc.IsSkipCreateSQL() {
		log.Info("skip create table and alter autoIncID", zap.Stringer("table", table.Info.Name))
	} else {
		// don't use rc.ctx here...
		// remove the ctx field of Client would be a great work,
		// we just take a small step here :<
		err := db.CreateTable(ctx, table)
		if err != nil {
			return CreatedTable{}, err
		}
	}
	newTableInfo, err := rc.GetTableSchema(dom, table.Db.Name, table.Info.Name)
	if err != nil {
		return CreatedTable{}, err
	}
	rules := GetRewriteRules(newTableInfo, table.Info, newTS)
	et := CreatedTable{
		RewriteRule: rules,
		Table:       newTableInfo,
		OldTable:    table,
	}
	return et, nil
}

// GoCreateTables create tables, and generate their information.
// this function will use workers as the same number of sessionPool,
// leave sessionPool nil to send DDLs sequential.
func (rc *Client) GoCreateTables(
	ctx context.Context,
	dom *domain.Domain,
	tables []*utils.Table,
	newTS uint64,
	dbPool []*DB,
	errCh chan<- error,
) <-chan CreatedTable {
	// Could we have a smaller size of tables?
	outCh := make(chan CreatedTable, len(tables))
	createOneTable := func(db *DB, t *utils.Table) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		rt, err := rc.createTable(ctx, db, dom, t, newTS)
		if err != nil {
			log.Error("create table failed",
				zap.Error(err),
				zap.Stringer("db", t.Db.Name),
				zap.Stringer("table", t.Info.Name))
			return err
		}
		log.Debug("table created and send to next",
			zap.Int("output chan size", len(outCh)),
			zap.Stringer("table", t.Info.Name),
			zap.Stringer("database", t.Db.Name))
		outCh <- rt
		return nil
	}
	startWork := func(t *utils.Table, done func()) {
		defer done()
		if err := createOneTable(nil, t); err != nil {
			errCh <- err
			return
		}
	}
	if len(dbPool) > 0 {
		workers := utils.NewWorkerPool(uint(len(dbPool)), "DDL workers")
		startWork = func(t *utils.Table, done func()) {
			workers.ApplyWithID(func(id uint64) {
				defer done()
				selectedDB := int(id) % len(dbPool)
				if err := createOneTable(dbPool[selectedDB], t); err != nil {
					errCh <- err
					return
				}
			})
		}
	}

	go func() {
		// TODO replace it with an errgroup
		wg := new(sync.WaitGroup)
		defer close(outCh)
		defer log.Info("all tables created")
		defer func() {
			if len(dbPool) > 0 {
				for _, db := range dbPool {
					db.Close()
				}
			}
		}()

		for _, table := range tables {
			tbl := table
			wg.Add(1)
			startWork(tbl, wg.Done)
		}
		wg.Wait()
	}()
	return outCh
}

// makeTiFlashOfTableRecord make a 'record' repsenting TiFlash of a table that has been removed.
// We doesn't record table ID here because when restore TiFlash replicas,
// we use `ALTER TABLE db.tbl SET TIFLASH_REPLICA = xxx` DDL, instead of use some internal TiDB API.
func makeTiFlashOfTableRecord(table *utils.Table, replica int) (*backup.Schema, error) {
	tableData, err := json.Marshal(table.Info)
	if err != nil {
		return nil, errors.Trace(err)
	}
	dbData, err := json.Marshal(table.Db)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result := &backup.Schema{
		Db:              dbData,
		Table:           tableData,
		Crc64Xor:        table.Crc64Xor,
		TotalKvs:        table.TotalKvs,
		TotalBytes:      table.TotalBytes,
		TiflashReplicas: uint32(replica),
	}
	return result, nil
}

// RemoveTiFlashOfTable removes TiFlash replica of some table,
// returns the removed count of TiFlash nodes.
// TODO: save the removed TiFlash information into disk.
// TODO: remove this after tiflash supports restore.
func (rc *Client) RemoveTiFlashOfTable(table CreatedTable, rule []placement.Rule) (int, error) {
	if rule := utils.SearchPlacementRule(table.Table.ID, rule, placement.Learner); rule != nil {
		if rule.Count > 0 {
			log.Info("remove TiFlash of table", zap.Int64("table ID", table.Table.ID), zap.Int("count", rule.Count))
			err := multierr.Combine(
				rc.db.AlterTiflashReplica(rc.ctx, table.OldTable, 0),
				rc.removeTiFlashOf(table.OldTable, rule.Count),
				rc.flushTiFlashRecord(),
			)
			if err != nil {
				return 0, errors.Trace(err)
			}
			return rule.Count, nil
		}
	}
	return 0, nil
}

func (rc *Client) removeTiFlashOf(table *utils.Table, replica int) error {
	tableRecord, err := makeTiFlashOfTableRecord(table, replica)
	if err != nil {
		return err
	}
	rc.tablesRemovedTiFlash = append(rc.tablesRemovedTiFlash, tableRecord)
	rc.tiFlashRecordUpdated = true
	return nil
}

func (rc *Client) flushTiFlashRecord() error {
	// Today nothing to do :D
	if !rc.tiFlashRecordUpdated {
		return nil
	}

	// should we make a deep copy here?
	// currently, write things directly to backup meta is OK since there seems nobody uses it.
	// But would it be better if we don't do it?
	rc.backupMeta.Schemas = rc.tablesRemovedTiFlash
	backupMetaData, err := proto.Marshal(rc.backupMeta)
	if err != nil {
		return errors.Trace(err)
	}
	backendURL := storage.FormatBackendURL(rc.backend)
	log.Info("update backup meta", zap.Stringer("path", &backendURL))
	err = rc.storage.Write(rc.ctx, utils.SavedMetaFile, backupMetaData)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RecoverTiFlashOfTable recovers TiFlash replica of some table.
// TODO: remove this after tiflash supports restore.
func (rc *Client) RecoverTiFlashOfTable(table *utils.Table) error {
	if table.TiFlashReplicas > 0 {
		err := rc.db.AlterTiflashReplica(rc.ctx, table, table.TiFlashReplicas)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// RecoverTiFlashReplica recovers all the tiflash replicas of a table
// TODO: remove this after tiflash supports restore.
func (rc *Client) RecoverTiFlashReplica(tables []*utils.Table) error {
	for _, table := range tables {
		if err := rc.RecoverTiFlashOfTable(table); err != nil {
			return err
		}
	}
	return nil
}

// ExecDDLs executes the queries of the ddl jobs.
func (rc *Client) ExecDDLs(ddlJobs []*model.Job) error {
	// Sort the ddl jobs by schema version in ascending order.
	sort.Slice(ddlJobs, func(i, j int) bool {
		return ddlJobs[i].BinlogInfo.SchemaVersion < ddlJobs[j].BinlogInfo.SchemaVersion
	})

	for _, job := range ddlJobs {
		err := rc.db.ExecDDL(rc.ctx, job)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("execute ddl query",
			zap.String("db", job.SchemaName),
			zap.String("query", job.Query),
			zap.Int64("historySchemaVersion", job.BinlogInfo.SchemaVersion))
	}
	return nil
}

func (rc *Client) setSpeedLimit() error {
	if !rc.hasSpeedLimited && rc.rateLimit != 0 {
		stores, err := conn.GetAllTiKVStores(rc.ctx, rc.pdClient, conn.SkipTiFlash)
		if err != nil {
			return err
		}
		for _, store := range stores {
			err = rc.fileImporter.setDownloadSpeedLimit(store.GetId())
			if err != nil {
				return err
			}
		}
		rc.hasSpeedLimited = true
	}
	return nil
}

// RestoreFiles tries to restore the files.
func (rc *Client) RestoreFiles(
	files []*backup.File,
	rewriteRules *RewriteRules,
	rejectStoreMap map[uint64]bool,
	updateCh glue.Progress,
) (err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if err == nil {
			log.Info("Restore Files",
				zap.Int("files", len(files)), zap.Duration("take", elapsed))
			summary.CollectSuccessUnit("files", len(files), elapsed)
		}
	}()

	log.Debug("start to restore files",
		zap.Int("files", len(files)),
	)
	errCh := make(chan error, len(files))
	wg := new(sync.WaitGroup)
	defer close(errCh)
	err = rc.setSpeedLimit()
	if err != nil {
		return err
	}

	for _, file := range files {
		wg.Add(1)
		fileReplica := file
		rc.workerPool.Apply(
			func() {
				defer wg.Done()
				select {
				case <-rc.ctx.Done():
					errCh <- rc.ctx.Err()
				case errCh <- rc.fileImporter.Import(fileReplica, rejectStoreMap, rewriteRules):
					updateCh.Inc()
				}
			})
	}
	for i := range files {
		err := <-errCh
		if err != nil {
			summary.CollectFailureUnit(fmt.Sprintf("file:%d", i), err)
			rc.cancel()
			wg.Wait()
			log.Error(
				"restore files failed",
				zap.Error(err),
			)
			return err
		}
	}
	wg.Wait()
	return nil
}

// RestoreRaw tries to restore raw keys in the specified range.
func (rc *Client) RestoreRaw(startKey []byte, endKey []byte, files []*backup.File, updateCh glue.Progress) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("Restore Raw",
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Duration("take", elapsed))
	}()
	errCh := make(chan error, len(files))
	wg := new(sync.WaitGroup)
	defer close(errCh)

	err := rc.fileImporter.SetRawRange(startKey, endKey)
	if err != nil {
		return errors.Trace(err)
	}

	emptyRules := &RewriteRules{}
	for _, file := range files {
		wg.Add(1)
		fileReplica := file
		rc.workerPool.Apply(
			func() {
				defer wg.Done()
				select {
				case <-rc.ctx.Done():
					errCh <- rc.ctx.Err()
				case errCh <- rc.fileImporter.Import(fileReplica, nil, emptyRules):
					updateCh.Inc()
				}
			})
	}
	for range files {
		err := <-errCh
		if err != nil {
			rc.cancel()
			wg.Wait()
			log.Error(
				"restore raw range failed",
				zap.String("startKey", hex.EncodeToString(startKey)),
				zap.String("endKey", hex.EncodeToString(endKey)),
				zap.Error(err),
			)
			return err
		}
	}
	log.Info(
		"finish to restore raw range",
		zap.String("startKey", hex.EncodeToString(startKey)),
		zap.String("endKey", hex.EncodeToString(endKey)),
	)
	return nil
}

// SwitchToImportMode switch tikv cluster to import mode.
func (rc *Client) SwitchToImportMode(ctx context.Context) {
	// tikv automatically switch to normal mode in every 10 minutes
	// so we need ping tikv in less than 10 minute
	go func() {
		tick := time.NewTicker(rc.switchModeInterval)

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				log.Info("switch to import mode")
				err := rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
				if err != nil {
					log.Warn("switch to import mode failed", zap.Error(err))
				}
			case <-rc.switchCh:
				log.Info("stop automatic switch to import mode")
				return
			}
		}
	}()
}

// SwitchToNormalMode switch tikv cluster to normal mode.
func (rc *Client) SwitchToNormalMode(ctx context.Context) error {
	close(rc.switchCh)
	return rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Normal)
}

func (rc *Client) switchTiKVMode(ctx context.Context, mode import_sstpb.SwitchMode) error {
	stores, err := conn.GetAllTiKVStores(ctx, rc.pdClient, conn.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = time.Second * 3
	for _, store := range stores {
		opt := grpc.WithInsecure()
		if rc.tlsConf != nil {
			opt = grpc.WithTransportCredentials(credentials.NewTLS(rc.tlsConf))
		}
		gctx, cancel := context.WithTimeout(ctx, time.Second*5)
		keepAlive := 10
		keepAliveTimeout := 3
		conn, err := grpc.DialContext(
			gctx,
			store.GetAddress(),
			opt,
			grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(keepAlive) * time.Second,
				Timeout: time.Duration(keepAliveTimeout) * time.Second,
			}),
		)
		cancel()
		if err != nil {
			return errors.Trace(err)
		}
		client := import_sstpb.NewImportSSTClient(conn)
		_, err = client.SwitchMode(ctx, &import_sstpb.SwitchModeRequest{
			Mode: mode,
		})
		if err != nil {
			return errors.Trace(err)
		}
		err = conn.Close()
		if err != nil {
			log.Error("close grpc connection failed in switch mode", zap.Error(err))
			continue
		}
	}
	return nil
}

// GoValidateChecksum forks a goroutine to validate checksum after restore.
// it returns a channel fires a struct{} when all things get done.
func (rc *Client) GoValidateChecksum(
	ctx context.Context,
	tableStream <-chan CreatedTable,
	kvClient kv.Client,
	errCh chan<- error,
	updateCh glue.Progress,
) <-chan struct{} {
	log.Info("Start to validate checksum")
	outCh := make(chan struct{}, 1)
	workers := utils.NewWorkerPool(defaultChecksumConcurrency, "RestoreChecksum")
	go func() {
		start := time.Now()
		wg := new(sync.WaitGroup)
		defer func() {
			log.Info("all checksum ended")
			wg.Wait()
			elapsed := time.Since(start)
			summary.CollectDuration("restore checksum", elapsed)
			outCh <- struct{}{}
			close(outCh)
		}()
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
			case tbl, ok := <-tableStream:
				if !ok {
					return
				}
				wg.Add(1)
				workers.Apply(func() {
					err := rc.execChecksum(ctx, tbl, kvClient)
					if err != nil {
						errCh <- err
					}
					updateCh.Inc()
					wg.Done()
				})
			}
		}
	}()
	return outCh
}

func (rc *Client) execChecksum(ctx context.Context, tbl CreatedTable, kvClient kv.Client) error {
	if tbl.OldTable.NoChecksum() {
		log.Warn("table has no checksum, skipping checksum",
			zap.Stringer("table", tbl.OldTable.Info.Name),
			zap.Stringer("database", tbl.OldTable.Db.Name),
		)
		return nil
	}

	startTS, err := rc.GetTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	exe, err := checksum.NewExecutorBuilder(tbl.Table, startTS).
		SetOldTable(tbl.OldTable).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	checksumResp, err := exe.Execute(ctx, kvClient, func() {
		// TODO: update progress here.
	})
	if err != nil {
		return errors.Trace(err)
	}

	table := tbl.OldTable
	if checksumResp.Checksum != table.Crc64Xor ||
		checksumResp.TotalKvs != table.TotalKvs ||
		checksumResp.TotalBytes != table.TotalBytes {
		log.Error("failed in validate checksum",
			zap.String("database", table.Db.Name.L),
			zap.String("table", table.Info.Name.L),
			zap.Uint64("origin tidb crc64", table.Crc64Xor),
			zap.Uint64("calculated crc64", checksumResp.Checksum),
			zap.Uint64("origin tidb total kvs", table.TotalKvs),
			zap.Uint64("calculated total kvs", checksumResp.TotalKvs),
			zap.Uint64("origin tidb total bytes", table.TotalBytes),
			zap.Uint64("calculated total bytes", checksumResp.TotalBytes),
		)
		return errors.New("failed to validate checksum")
	}
	return nil
}

const (
	restoreLabelKey   = "exclusive"
	restoreLabelValue = "restore"
)

// LoadRestoreStores loads the stores used to restore data.
func (rc *Client) LoadRestoreStores(ctx context.Context) error {
	if !rc.isOnline {
		return nil
	}

	stores, err := rc.pdClient.GetAllStores(ctx)
	if err != nil {
		return err
	}
	for _, s := range stores {
		if s.GetState() != metapb.StoreState_Up {
			continue
		}
		for _, l := range s.GetLabels() {
			if l.GetKey() == restoreLabelKey && l.GetValue() == restoreLabelValue {
				rc.restoreStores = append(rc.restoreStores, s.GetId())
				break
			}
		}
	}
	log.Info("load restore stores", zap.Uint64s("store-ids", rc.restoreStores))
	return nil
}

// ResetRestoreLabels removes the exclusive labels of the restore stores.
func (rc *Client) ResetRestoreLabels(ctx context.Context) error {
	if !rc.isOnline {
		return nil
	}
	log.Info("start reseting store labels")
	return rc.toolClient.SetStoresLabel(ctx, rc.restoreStores, restoreLabelKey, "")
}

// SetupPlacementRules sets rules for the tables' regions.
func (rc *Client) SetupPlacementRules(ctx context.Context, tables []*model.TableInfo) error {
	if !rc.isOnline || len(rc.restoreStores) == 0 {
		return nil
	}
	log.Info("start setting placement rules")
	rule, err := rc.toolClient.GetPlacementRule(ctx, "pd", "default")
	if err != nil {
		return err
	}
	rule.Index = 100
	rule.Override = true
	rule.LabelConstraints = append(rule.LabelConstraints, placement.LabelConstraint{
		Key:    restoreLabelKey,
		Op:     "in",
		Values: []string{restoreLabelValue},
	})
	for _, t := range tables {
		rule.ID = rc.getRuleID(t.ID)
		rule.StartKeyHex = hex.EncodeToString(codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID)))
		rule.EndKeyHex = hex.EncodeToString(codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID+1)))
		err = rc.toolClient.SetPlacementRule(ctx, rule)
		if err != nil {
			return err
		}
	}
	log.Info("finish setting placement rules")
	return nil
}

// WaitPlacementSchedule waits PD to move tables to restore stores.
func (rc *Client) WaitPlacementSchedule(ctx context.Context, tables []*model.TableInfo) error {
	if !rc.isOnline || len(rc.restoreStores) == 0 {
		return nil
	}
	log.Info("start waiting placement schedule")
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ok, progress, err := rc.checkRegions(ctx, tables)
			if err != nil {
				return err
			}
			if ok {
				log.Info("finish waiting placement schedule")
				return nil
			}
			log.Info("placement schedule progress: " + progress)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (rc *Client) checkRegions(ctx context.Context, tables []*model.TableInfo) (bool, string, error) {
	for i, t := range tables {
		start := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID))
		end := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID+1))
		ok, regionProgress, err := rc.checkRange(ctx, start, end)
		if err != nil {
			return false, "", err
		}
		if !ok {
			return false, fmt.Sprintf("table %v/%v, %s", i, len(tables), regionProgress), nil
		}
	}
	return true, "", nil
}

func (rc *Client) checkRange(ctx context.Context, start, end []byte) (bool, string, error) {
	regions, err := rc.toolClient.ScanRegions(ctx, start, end, -1)
	if err != nil {
		return false, "", err
	}
	for i, r := range regions {
	NEXT_PEER:
		for _, p := range r.Region.GetPeers() {
			for _, storeID := range rc.restoreStores {
				if p.GetStoreId() == storeID {
					continue NEXT_PEER
				}
			}
			return false, fmt.Sprintf("region %v/%v", i, len(regions)), nil
		}
	}
	return true, "", nil
}

// ResetPlacementRules removes placement rules for tables.
func (rc *Client) ResetPlacementRules(ctx context.Context, tables []*model.TableInfo) error {
	if !rc.isOnline || len(rc.restoreStores) == 0 {
		return nil
	}
	log.Info("start reseting placement rules")
	var failedTables []int64
	for _, t := range tables {
		err := rc.toolClient.DeletePlacementRule(ctx, "pd", rc.getRuleID(t.ID))
		if err != nil {
			log.Info("failed to delete placement rule for table", zap.Int64("table-id", t.ID))
			failedTables = append(failedTables, t.ID)
		}
	}
	if len(failedTables) > 0 {
		return errors.Errorf("failed to delete placement rules for tables %v", failedTables)
	}
	return nil
}

func (rc *Client) getRuleID(tableID int64) string {
	return "restore-t" + strconv.FormatInt(tableID, 10)
}

// IsIncremental returns whether this backup is incremental.
func (rc *Client) IsIncremental() bool {
	return !(rc.backupMeta.StartVersion == rc.backupMeta.EndVersion ||
		rc.backupMeta.StartVersion == 0)
}

// EnableSkipCreateSQL sets switch of skip create schema and tables.
func (rc *Client) EnableSkipCreateSQL() {
	rc.noSchema = true
}

// IsSkipCreateSQL returns whether we need skip create schema and tables in restore.
func (rc *Client) IsSkipCreateSQL() bool {
	return rc.noSchema
}
