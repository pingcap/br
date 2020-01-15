package restore

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/client"
	restore_util "github.com/pingcap/tidb-tools/pkg/restore-util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/br/pkg/checksum"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

const (
	resetTsRetryTime       = 16
	resetTSWaitInterval    = 50 * time.Millisecond
	resetTSMaxWaitInterval = 500 * time.Millisecond

	// defaultChecksumConcurrency is the default number of the concurrent
	// checksum tasks.
	defaultChecksumConcurrency = 64
)

// Client sends requests to restore files
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	pdClient        pd.Client
	fileImporter    FileImporter
	workerPool      *utils.WorkerPool
	tableWorkerPool *utils.WorkerPool

	databases       map[string]*utils.Database
	backupMeta      *backup.BackupMeta
	db              *DB
	rateLimit       uint64
	isOnline        bool
	hasSpeedLimited bool
}

// NewRestoreClient returns a new RestoreClient
func NewRestoreClient(
	ctx context.Context,
	pdClient pd.Client,
	store kv.Storage,
) (*Client, error) {
	ctx, cancel := context.WithCancel(ctx)
	db, err := NewDB(store)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	return &Client{
		ctx:             ctx,
		cancel:          cancel,
		pdClient:        pdClient,
		tableWorkerPool: utils.NewWorkerPool(128, "table"),
		db:              db,
	}, nil
}

// SetRateLimit to set rateLimit.
func (rc *Client) SetRateLimit(rateLimit uint64) {
	rc.rateLimit = rateLimit
}

// GetPDClient returns a pd client.
func (rc *Client) GetPDClient() pd.Client {
	return rc.pdClient
}

// IsOnline tells if it's a online restore
func (rc *Client) IsOnline() bool {
	return rc.isOnline
}

// Close a client
func (rc *Client) Close() {
	rc.db.Close()
	rc.cancel()
	log.Info("Restore client closed")
}

// InitBackupMeta loads schemas from BackupMeta to initialize RestoreClient
func (rc *Client) InitBackupMeta(backupMeta *backup.BackupMeta, backend *backup.StorageBackend) error {
	databases, err := utils.LoadBackupTables(backupMeta)
	if err != nil {
		return errors.Trace(err)
	}
	rc.databases = databases
	rc.backupMeta = backupMeta

	metaClient := restore_util.NewClient(rc.pdClient)
	importClient := NewImportClient(metaClient)
	rc.fileImporter = NewFileImporter(rc.ctx, metaClient, importClient, backend, rc.rateLimit)
	return nil
}

// SetConcurrency sets the concurrency of dbs tables files
func (rc *Client) SetConcurrency(c uint) {
	rc.workerPool = utils.NewWorkerPool(c, "file")
}

// EnableOnline sets the mode of restore to online.
func (rc *Client) EnableOnline() {
	rc.isOnline = true
}

// GetTS gets a new timestamp from PD
func (rc *Client) GetTS(ctx context.Context) (uint64, error) {
	p, l, err := rc.pdClient.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	restoreTS := oracle.ComposeTS(p, l)
	return restoreTS, nil
}

// ResetTS resets the timestamp of PD to a bigger value
func (rc *Client) ResetTS(pdAddrs []string) error {
	restoreTS := rc.backupMeta.GetEndVersion()
	log.Info("reset pd timestamp", zap.Uint64("ts", restoreTS))
	i := 0
	return withRetry(func() error {
		idx := i % len(pdAddrs)
		return utils.ResetTS(pdAddrs[idx], restoreTS)
	}, func(e error) bool {
		i++
		return true
	}, resetTsRetryTime, resetTSWaitInterval, resetTSMaxWaitInterval)
}

// GetDatabases returns all databases.
func (rc *Client) GetDatabases() []*utils.Database {
	dbs := make([]*utils.Database, 0, len(rc.databases))
	for _, db := range rc.databases {
		dbs = append(dbs, db)
	}
	return dbs
}

// GetDatabase returns a database by name
func (rc *Client) GetDatabase(name string) *utils.Database {
	return rc.databases[name]
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
	return rc.db.CreateDatabase(rc.ctx, db)
}

// CreateTables creates multiple tables, and returns their rewrite rules.
func (rc *Client) CreateTables(
	dom *domain.Domain,
	tables []*utils.Table,
	newTS uint64,
) (*restore_util.RewriteRules, []*model.TableInfo, error) {
	rewriteRules := &restore_util.RewriteRules{
		Table: make([]*import_sstpb.RewriteRule, 0),
		Data:  make([]*import_sstpb.RewriteRule, 0),
	}
	newTables := make([]*model.TableInfo, 0, len(tables))
	for _, table := range tables {
		err := rc.db.CreateTable(rc.ctx, table)
		if err != nil {
			return nil, nil, err
		}
		newTableInfo, err := rc.GetTableSchema(dom, table.Db.Name, table.Schema.Name)
		if err != nil {
			return nil, nil, err
		}
		rules := GetRewriteRules(newTableInfo, table.Schema, newTS)
		rewriteRules.Table = append(rewriteRules.Table, rules.Table...)
		rewriteRules.Data = append(rewriteRules.Data, rules.Data...)
		newTables = append(newTables, newTableInfo)
	}
	return rewriteRules, newTables, nil
}

func (rc *Client) setSpeedLimit() error {
	if !rc.hasSpeedLimited && rc.rateLimit != 0 {
		stores, err := rc.pdClient.GetAllStores(rc.ctx, pd.WithExcludeTombstone())
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

// RestoreTable tries to restore the data of a table.
func (rc *Client) RestoreTable(
	table *utils.Table,
	rewriteRules *restore_util.RewriteRules,
	updateCh chan<- struct{},
) (err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("restore table",
			zap.Stringer("table", table.Schema.Name), zap.Duration("take", elapsed))
		key := fmt.Sprintf("%s.%s", table.Db.Name.String(), table.Schema.Name.String())
		if err != nil {
			summary.CollectFailureUnit(key, err)
		} else {
			summary.CollectSuccessUnit(key, elapsed)
		}
	}()

	log.Debug("start to restore table",
		zap.Stringer("table", table.Schema.Name),
		zap.Stringer("db", table.Db.Name),
		zap.Array("files", files(table.Files)),
	)
	errCh := make(chan error, len(table.Files))
	wg := new(sync.WaitGroup)
	defer close(errCh)
	err = rc.setSpeedLimit()
	if err != nil {
		return err
	}

	for _, file := range table.Files {
		wg.Add(1)
		fileReplica := file
		rc.workerPool.Apply(
			func() {
				defer wg.Done()
				select {
				case <-rc.ctx.Done():
					errCh <- nil
				case errCh <- rc.fileImporter.Import(fileReplica, rewriteRules):
					updateCh <- struct{}{}
				}
			})
	}
	for range table.Files {
		err := <-errCh
		if err != nil {
			rc.cancel()
			wg.Wait()
			log.Error(
				"restore table failed",
				zap.Stringer("table", table.Schema.Name),
				zap.Stringer("db", table.Db.Name),
				zap.Error(err),
			)
			return err
		}
	}
	log.Info(
		"finish to restore table",
		zap.Stringer("table", table.Schema.Name),
		zap.Stringer("db", table.Db.Name),
	)
	return nil
}

// RestoreDatabase tries to restore the data of a database
func (rc *Client) RestoreDatabase(
	db *utils.Database,
	rewriteRules *restore_util.RewriteRules,
	updateCh chan<- struct{},
) (err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("Restore Database", zap.Stringer("db", db.Schema.Name), zap.Duration("take", elapsed))
	}()
	errCh := make(chan error, len(db.Tables))
	wg := new(sync.WaitGroup)
	defer close(errCh)
	for _, table := range db.Tables {
		wg.Add(1)
		tblReplica := table
		rc.tableWorkerPool.Apply(func() {
			defer wg.Done()
			select {
			case <-rc.ctx.Done():
				errCh <- nil
			case errCh <- rc.RestoreTable(
				tblReplica, rewriteRules, updateCh):
			}
		})
	}
	for range db.Tables {
		err = <-errCh
		if err != nil {
			wg.Wait()
			return err
		}
	}
	return nil
}

// RestoreAll tries to restore all the data of backup files.
func (rc *Client) RestoreAll(
	rewriteRules *restore_util.RewriteRules,
	updateCh chan<- struct{},
) (err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("Restore All", zap.Duration("take", elapsed))
	}()
	errCh := make(chan error, len(rc.databases))
	wg := new(sync.WaitGroup)
	defer close(errCh)
	for _, db := range rc.databases {
		wg.Add(1)
		dbReplica := db
		rc.tableWorkerPool.Apply(func() {
			defer wg.Done()
			select {
			case <-rc.ctx.Done():
				errCh <- nil
			case errCh <- rc.RestoreDatabase(
				dbReplica, rewriteRules, updateCh):
			}
		})
	}

	for range rc.databases {
		err = <-errCh
		if err != nil {
			wg.Wait()
			return err
		}
	}
	return nil
}

//SwitchToImportMode switch tikv cluster to import mode
func (rc *Client) SwitchToImportMode(ctx context.Context) error {
	return rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
}

//SwitchToNormalMode switch tikv cluster to normal mode
func (rc *Client) SwitchToNormalMode(ctx context.Context) error {
	return rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Normal)
}

func (rc *Client) switchTiKVMode(ctx context.Context, mode import_sstpb.SwitchMode) error {
	stores, err := rc.pdClient.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return errors.Trace(err)
	}
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = time.Second * 3
	for _, store := range stores {
		opt := grpc.WithInsecure()
		gctx, cancel := context.WithTimeout(ctx, time.Second*5)
		keepAlive := 10
		keepAliveTimeout := 3
		conn, err := grpc.DialContext(
			gctx,
			store.GetAddress(),
			opt,
			grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                time.Duration(keepAlive) * time.Second,
				Timeout:             time.Duration(keepAliveTimeout) * time.Second,
				PermitWithoutStream: true,
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

//ValidateChecksum validate checksum after restore
func (rc *Client) ValidateChecksum(
	ctx context.Context,
	kvClient kv.Client,
	tables []*utils.Table,
	newTables []*model.TableInfo,
	updateCh chan<- struct{},
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		summary.CollectDuration("restore checksum", elapsed)
	}()

	log.Info("Start to validate checksum")
	wg := new(sync.WaitGroup)
	errCh := make(chan error)
	workers := utils.NewWorkerPool(defaultChecksumConcurrency, "RestoreChecksum")
	go func() {
		for i, t := range tables {
			table := t
			newTable := newTables[i]
			wg.Add(1)
			workers.Apply(func() {
				defer wg.Done()

				startTS, err := rc.GetTS(ctx)
				if err != nil {
					errCh <- errors.Trace(err)
					return
				}
				exe, err := checksum.NewExecutorBuilder(newTable, startTS).
					SetOldTable(table).
					Build()
				if err != nil {
					errCh <- errors.Trace(err)
					return
				}
				checksumResp, err := exe.Execute(ctx, kvClient, func() {
					// TODO: update progress here.
				})
				if err != nil {
					errCh <- errors.Trace(err)
					return
				}

				if checksumResp.Checksum != table.Crc64Xor ||
					checksumResp.TotalKvs != table.TotalKvs ||
					checksumResp.TotalBytes != table.TotalBytes {
					log.Error("failed in validate checksum",
						zap.String("database", table.Db.Name.L),
						zap.String("table", table.Schema.Name.L),
						zap.Uint64("origin tidb crc64", table.Crc64Xor),
						zap.Uint64("calculated crc64", checksumResp.Checksum),
						zap.Uint64("origin tidb total kvs", table.TotalKvs),
						zap.Uint64("calculated total kvs", checksumResp.TotalKvs),
						zap.Uint64("origin tidb total bytes", table.TotalBytes),
						zap.Uint64("calculated total bytes", checksumResp.TotalBytes),
					)
					errCh <- errors.New("failed to validate checksum")
					return
				}

				updateCh <- struct{}{}
			})
		}
		wg.Wait()
		close(errCh)
	}()
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	log.Info("validate checksum passed!!")
	return nil
}

// IsIncremental returns whether this backup is incremental
func (rc *Client) IsIncremental() bool {
	return !(rc.backupMeta.StartVersion == rc.backupMeta.EndVersion ||
		rc.backupMeta.StartVersion == 0)
}
