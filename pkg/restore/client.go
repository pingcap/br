// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/server/schedule/placement"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	// This is dirty: why we need DBs from different sources?
	// By replace it with a []*DB, we can remove the dirty parameter of `Client.GoCreateTable`,
	// along with them in some private functions.
	// Before you do it, you can firstly read discussions at
	// https://github.com/pingcap/br/pull/377#discussion_r446594501,
	// this probably isn't as easy as it seems like (however, not hard, too :D)
	db              *DB
	rateLimit       uint64
	isOnline        bool
	noSchema        bool
	hasSpeedLimited bool

	restoreStores []uint64

	storage            storage.ExternalStorage
	backend            *backup.StorageBackend
	switchModeInterval time.Duration
	switchCh           chan struct{}
}

// NewRestoreClient returns a new RestoreClient.
func NewRestoreClient(
	g glue.Glue,
	pdClient pd.Client,
	store kv.Storage,
	tlsConf *tls.Config,
) (*Client, error) {
	db, err := NewDB(g, store)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Client{
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
	rc.fileImporter = NewFileImporter(metaClient, importClient, backend, backupMeta.IsRawKv, rc.rateLimit)
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
func (rc *Client) ResetTS(ctx context.Context, pdAddrs []string) error {
	restoreTS := rc.backupMeta.GetEndVersion()
	log.Info("reset pd timestamp", zap.Uint64("ts", restoreTS))
	i := 0
	return utils.WithRetry(ctx, func() error {
		idx := i % len(pdAddrs)
		i++
		return utils.ResetTS(pdAddrs[idx], restoreTS, rc.tlsConf)
	}, newPDReqBackoffer())
}

// GetPlacementRules return the current placement rules.
func (rc *Client) GetPlacementRules(ctx context.Context, pdAddrs []string) ([]placement.Rule, error) {
	var placementRules []placement.Rule
	i := 0
	errRetry := utils.WithRetry(ctx, func() error {
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
	info := dom.InfoSchema()
	table, err := info.TableByName(dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return table.Meta(), nil
}

// CreateDatabase creates a database.
func (rc *Client) CreateDatabase(ctx context.Context, db *model.DBInfo) error {
	if rc.IsSkipCreateSQL() {
		log.Info("skip create database", zap.Stringer("database", db.Name))
		return nil
	}
	return rc.db.CreateDatabase(ctx, db)
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
	log.Info("start create tables")
	outCh := make(chan CreatedTable, len(tables))
	createOneTable := func(c context.Context, db *DB, t *utils.Table) error {
		select {
		case <-c.Done():
			return c.Err()
		default:
		}
		rt, err := rc.createTable(c, db, dom, t, newTS)
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
	go func() {
		defer close(outCh)
		defer log.Debug("all tables are created")
		var err error
		if len(dbPool) > 0 {
			err = rc.createTablesWithDBPool(ctx, createOneTable, tables, dbPool)
		} else {
			err = rc.createTablesWithSoleDB(ctx, createOneTable, tables)
		}
		if err != nil {
			errCh <- err
		}
	}()
	return outCh
}

func (rc *Client) createTablesWithSoleDB(ctx context.Context,
	createOneTable func(ctx context.Context, db *DB, t *utils.Table) error,
	tables []*utils.Table) error {
	for _, t := range tables {
		if err := createOneTable(ctx, rc.db, t); err != nil {
			return err
		}
	}
	return nil
}

func (rc *Client) createTablesWithDBPool(ctx context.Context,
	createOneTable func(ctx context.Context, db *DB, t *utils.Table) error,
	tables []*utils.Table, dbPool []*DB) error {
	eg, ectx := errgroup.WithContext(ctx)
	workers := utils.NewWorkerPool(uint(len(dbPool)), "DDL workers")
	for _, t := range tables {
		table := t
		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			db := dbPool[id%uint64(len(dbPool))]
			return createOneTable(ectx, db, table)
		})
	}
	return eg.Wait()
}

// ExecDDLs executes the queries of the ddl jobs.
func (rc *Client) ExecDDLs(ctx context.Context, ddlJobs []*model.Job) error {
	// Sort the ddl jobs by schema version in ascending order.
	sort.Slice(ddlJobs, func(i, j int) bool {
		return ddlJobs[i].BinlogInfo.SchemaVersion < ddlJobs[j].BinlogInfo.SchemaVersion
	})

	for _, job := range ddlJobs {
		err := rc.db.ExecDDL(ctx, job)
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

func (rc *Client) setSpeedLimit(ctx context.Context) error {
	if !rc.hasSpeedLimited && rc.rateLimit != 0 {
		stores, err := conn.GetAllTiKVStores(ctx, rc.pdClient, conn.SkipTiFlash)
		if err != nil {
			return err
		}
		for _, store := range stores {
			err = rc.fileImporter.setDownloadSpeedLimit(ctx, store.GetId())
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
	ctx context.Context,
	files []*backup.File,
	rewriteRules *RewriteRules,
	updateCh glue.Progress,
) (err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if err == nil {
			log.Info("Restore files",
				zap.Duration("take", elapsed),
				utils.ZapFiles(files))
			summary.CollectSuccessUnit("files", len(files), elapsed)
		}
	}()

	log.Debug("start to restore files",
		zap.Int("files", len(files)),
	)
	eg, ectx := errgroup.WithContext(ctx)
	err = rc.setSpeedLimit(ctx)
	if err != nil {
		return err
	}

	for _, file := range files {
		fileReplica := file
		rc.workerPool.ApplyOnErrorGroup(eg,
			func() error {
				fileStart := time.Now()
				defer func() {
					log.Info("import file done", utils.ZapFile(fileReplica),
						zap.Duration("take", time.Since(fileStart)))
					updateCh.Inc()
				}()
				return rc.fileImporter.Import(ectx, fileReplica, rewriteRules)
			})
	}
	if err := eg.Wait(); err != nil {
		summary.CollectFailureUnit("file", err)
		log.Error(
			"restore files failed",
			zap.Error(err),
		)
		return err
	}
	return nil
}

// RestoreRaw tries to restore raw keys in the specified range.
func (rc *Client) RestoreRaw(
	ctx context.Context, startKey []byte, endKey []byte, files []*backup.File, updateCh glue.Progress,
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("Restore Raw",
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Duration("take", elapsed))
	}()
	errCh := make(chan error, len(files))
	eg, ectx := errgroup.WithContext(ctx)
	defer close(errCh)

	err := rc.fileImporter.SetRawRange(startKey, endKey)
	if err != nil {
		return errors.Trace(err)
	}

	for _, file := range files {
		fileReplica := file
		rc.workerPool.ApplyOnErrorGroup(eg,
			func() error {
				defer updateCh.Inc()
				return rc.fileImporter.Import(ectx, fileReplica, EmptyRewriteRule())
			})
	}
	if err := eg.Wait(); err != nil {
		log.Error(
			"restore raw range failed",
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Error(err),
		)
		return err
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
		defer tick.Stop()

		// [important!] switch tikv mode into import at the beginning
		log.Info("switch to import mode at beginning")
		err := rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
		if err != nil {
			log.Warn("switch to import mode failed", zap.Error(err))
		}

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
		wg, ectx := errgroup.WithContext(ctx)
		defer func() {
			log.Info("all checksum ended")
			if err := wg.Wait(); err != nil {
				errCh <- err
			}
			elapsed := time.Since(start)
			summary.CollectDuration("restore checksum", elapsed)
			summary.CollectSuccessUnit("table checksum", 1, elapsed)
			outCh <- struct{}{}
			close(outCh)
		}()
		for {
			select {
			// if we use ectx here, maybe canceled will mask real error.
			case <-ctx.Done():
				errCh <- ctx.Err()
			case tbl, ok := <-tableStream:
				if !ok {
					return
				}
				workers.ApplyOnErrorGroup(wg, func() error {
					err := rc.execChecksum(ectx, tbl, kvClient)
					if err != nil {
						return err
					}
					updateCh.Inc()
					return nil
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
