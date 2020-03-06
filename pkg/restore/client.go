package restore

import (
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

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/pd/server/schedule/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/br/pkg/checksum"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

// defaultChecksumConcurrency is the default number of the concurrent
// checksum tasks.
const defaultChecksumConcurrency = 64

// Client sends requests to restore files
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	pdClient     pd.Client
	toolClient   SplitClient
	fileImporter FileImporter
	workerPool   *utils.WorkerPool
	tlsConf      *tls.Config

	databases       map[string]*utils.Database
	ddlJobs         []*model.Job
	backupMeta      *backup.BackupMeta
	db              *DB
	rateLimit       uint64
	isOnline        bool
	hasSpeedLimited bool

	restoreStores []uint64
}

// NewRestoreClient returns a new RestoreClient
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
	var ddlJobs []*model.Job
	err = json.Unmarshal(backupMeta.GetDdls(), &ddlJobs)
	if err != nil {
		return errors.Trace(err)
	}
	rc.databases = databases
	rc.ddlJobs = ddlJobs
	rc.backupMeta = backupMeta
	log.Info("load backupmeta", zap.Int("databases", len(rc.databases)), zap.Int("jobs", len(rc.ddlJobs)))

	metaClient := NewSplitClient(rc.pdClient, rc.tlsConf)
	importClient := NewImportClient(metaClient, rc.tlsConf)
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

// GetTLSConfig returns the tls config
func (rc *Client) GetTLSConfig() *tls.Config {
	return rc.tlsConf
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
	return utils.WithRetry(rc.ctx, func() error {
		idx := i % len(pdAddrs)
		return utils.ResetTS(pdAddrs[idx], restoreTS, rc.tlsConf)
	}, newResetTSBackoffer())
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

// GetDDLJobs returns ddl jobs
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
	for _, table := range tables {
		err := rc.db.CreateTable(rc.ctx, table)
		if err != nil {
			return nil, nil, err
		}
		newTableInfo, err := rc.GetTableSchema(dom, table.Db.Name, table.Info.Name)
		if err != nil {
			return nil, nil, err
		}
		rules := GetRewriteRules(newTableInfo, table.Info, newTS)
		rewriteRules.Table = append(rewriteRules.Table, rules.Table...)
		rewriteRules.Data = append(rewriteRules.Data, rules.Data...)
		newTables = append(newTables, newTableInfo)
	}
	return rewriteRules, newTables, nil
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

// RestoreFiles tries to restore the files.
func (rc *Client) RestoreFiles(
	files []*backup.File,
	rewriteRules *RewriteRules,
	updateCh chan<- struct{},
) (err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if err == nil {
			log.Info("Restore Files",
				zap.Int("files", len(files)), zap.Duration("take", elapsed))
			summary.CollectSuccessUnit("files", elapsed)
		} else {
			summary.CollectFailureUnit("files", err)
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
					errCh <- nil
				case errCh <- rc.fileImporter.Import(fileReplica, rewriteRules):
					updateCh <- struct{}{}
				}
			})
	}
	for range files {
		err := <-errCh
		if err != nil {
			rc.cancel()
			wg.Wait()
			log.Error(
				"restore files failed",
				zap.Error(err),
			)
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
						zap.String("table", table.Info.Name.L),
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

// IsIncremental returns whether this backup is incremental
func (rc *Client) IsIncremental() bool {
	return !(rc.backupMeta.StartVersion == rc.backupMeta.EndVersion ||
		rc.backupMeta.StartVersion == 0)
}
