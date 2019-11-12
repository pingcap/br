package restore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/client"
	restore_util "github.com/pingcap/tidb-tools/pkg/restore-util"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/br/pkg/meta"
	"github.com/pingcap/br/pkg/utils"
)

const (
	resetTSURL             = "/pd/api/v1/admin/reset-ts"
	resetTsRetryTime       = 16
	resetTSWaitInterval    = 50 * time.Millisecond
	resetTSMaxWaitInterval = 500 * time.Millisecond
)

// Client sends requests to importer to restore files
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	pdClient         pd.Client
	pdAddrs          []string
	tikvCli          tikv.Storage
	fileImporter     FileImporter
	workerPool       *utils.WorkerPool
	regionWorkerPool *utils.WorkerPool

	databases  map[string]*utils.Database
	dbDSN      string
	backupMeta *backup.BackupMeta
	backer     *meta.Backer
}

// NewRestoreClient returns a new RestoreClient
func NewRestoreClient(ctx context.Context, pdAddrs string) (*Client, error) {
	ctx, cancel := context.WithCancel(ctx)
	addrs := strings.Split(pdAddrs, ",")
	backer, err := meta.NewBacker(ctx, addrs[0])
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	tikvCli, err := tikv.Driver{}.Open(
		// Disable GC because TiDB enables GC already.
		fmt.Sprintf("tikv://%s?disableGC=true", pdAddrs))
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	return &Client{
		ctx:      ctx,
		cancel:   cancel,
		pdClient: pdClient,
		pdAddrs:  addrs,
		tikvCli:  tikvCli.(tikv.Storage),
		backer:   backer,
	}, nil
}

// GetPDClient returns a pd client.
func (rc *Client) GetPDClient() pd.Client {
	return rc.pdClient
}

// InitBackupMeta loads schemas from BackupMeta to initialize RestoreClient
func (rc *Client) InitBackupMeta(backupMeta *backup.BackupMeta) error {
	databases, err := utils.LoadBackupTables(backupMeta)
	if err != nil {
		return errors.Trace(err)
	}
	rc.databases = databases
	rc.backupMeta = backupMeta

	client := restore_util.NewClient(rc.pdClient)
	rc.fileImporter = NewFileImporter(rc.ctx, client, backupMeta.GetPath())
	return nil
}

// SetDbDSN sets the DSN to connect the database to a new value
func (rc *Client) SetDbDSN(dsn string) {
	rc.dbDSN = dsn
}

// GetDbDSN returns a DSN to connect the database
func (rc *Client) GetDbDSN() string {
	return rc.dbDSN
}

// SetConcurrency sets the concurrency of dbs tables files
func (rc *Client) SetConcurrency(c uint) {
	rc.workerPool = utils.NewWorkerPool(c/2, "restore")
	rc.regionWorkerPool = utils.NewWorkerPool(c/2, "restore_region")
}

// GetTS gets a new timestamp from PD
func (rc *Client) GetTS() (uint64, error) {
	p, l, err := rc.pdClient.GetTS(rc.ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	ts := meta.Timestamp{
		Physical: p,
		Logical:  l,
	}
	restoreTS := meta.EncodeTs(ts)
	return restoreTS, nil
}

// ResetTS resets the timestamp of PD to a bigger value
func (rc *Client) ResetTS() error {
	restoreTS := rc.backupMeta.GetEndVersion()
	log.Info("reset pd timestamp", zap.Uint64("ts", restoreTS))
	req, err := json.Marshal(struct {
		TSO string `json:"tso,omitempty"`
	}{TSO: fmt.Sprintf("%d", restoreTS)})
	if err != nil {
		return err
	}
	// TODO: Support TLS
	reqURL := "http://" + rc.pdAddrs[0] + resetTSURL
	return withRetry(func() error {
		resp, err := http.Post(reqURL, "application/json", strings.NewReader(string(req)))
		if err != nil {
			return errors.Trace(err)
		}
		if resp.StatusCode != 200 && resp.StatusCode != 403 {
			buf := new(bytes.Buffer)
			_, err := buf.ReadFrom(resp.Body)
			return errors.Errorf("pd resets TS failed: req=%v, resp=%v, err=%v", string(req), buf.String(), err)
		}
		return nil
	}, func(e error) bool {
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
func (rc *Client) GetTableSchema(dbName model.CIStr, tableName model.CIStr) (*model.TableInfo, error) {
	dbSession, err := session.CreateSession(rc.tikvCli)
	if err != nil {
		return nil, errors.Trace(err)
	}
	do := domain.GetDomain(dbSession.(sessionctx.Context))
	ts, err := rc.GetTS()
	if err != nil {
		return nil, errors.Trace(err)
	}
	info, err := do.GetSnapshotInfoSchema(ts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	table, err := info.TableByName(dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return table.Meta(), nil
}

// CreateTables creates multiple tables, and returns their rewrite rules.
func (rc *Client) CreateTables(tables []*utils.Table) (*restore_util.RewriteRules, []*model.TableInfo, error) {
	rewriteRules := &restore_util.RewriteRules{
		Table: make([]*import_sstpb.RewriteRule, 0),
		Data:  make([]*import_sstpb.RewriteRule, 0),
	}
	newTables := make([]*model.TableInfo, 0, len(tables))
	openDBs := make(map[string]*sql.DB)
	defer func() {
		for _, db := range openDBs {
			_ = db.Close()
		}
	}()
	for _, table := range tables {
		var err error
		db, ok := openDBs[table.Db.Name.String()]
		if !ok {
			db, err = OpenDatabase(table.Db.Name.String(), rc.dbDSN)
			if err != nil {
				return nil, nil, err
			}
			openDBs[table.Db.Name.String()] = db
		}
		err = CreateTable(db, table)
		if err != nil {
			return nil, nil, err
		}
		err = AlterAutoIncID(db, table)
		if err != nil {
			return nil, nil, err
		}
		newTableInfo, err := rc.GetTableSchema(table.Db.Name, table.Schema.Name)
		if err != nil {
			return nil, nil, err
		}
		rules := GetRewriteRules(newTableInfo, table.Schema)
		rewriteRules.Table = append(rewriteRules.Table, rules.Table...)
		rewriteRules.Data = append(rewriteRules.Data, rules.Data...)
		newTables = append(newTables, newTableInfo)
	}
	return rewriteRules, newTables, nil
}

// RestoreTable tries to restore the data of a table.
func (rc *Client) RestoreTable(
	table *utils.Table,
	rewriteRules *restore_util.RewriteRules,
	updateCh chan<- struct{},
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("RestoreTable", zap.Stringer("table", table.Schema.Name), zap.Duration("take", elapsed))
	}()
	log.Debug("start to restore table",
		zap.Stringer("table", table.Schema.Name),
		zap.Stringer("db", table.Db.Name),
		zap.Array("files", files(table.Files)),
		zap.Reflect("rewriteRules", rewriteRules),
	)
	errCh := make(chan error, len(table.Files))
	var wg sync.WaitGroup
	defer close(errCh)
	// We should encode the rewrite rewriteRules before using it to import files
	encodedRules := encodeRewriteRules(rewriteRules)
	for _, file := range table.Files {
		wg.Add(1)
		fileReplica := file
		rc.workerPool.Apply(
			func() {
				defer wg.Done()
				select {
				case <-rc.ctx.Done():
					errCh <- nil
				case errCh <- rc.fileImporter.Import(fileReplica, encodedRules):
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
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("RestoreDatabase", zap.Stringer("db", db.Schema.Name), zap.Duration("take", elapsed))
	}()
	errCh := make(chan error, len(db.Tables))
	var wg sync.WaitGroup
	defer close(errCh)
	for _, table := range db.Tables {
		wg.Add(1)
		tblReplica := table
		rc.workerPool.Apply(func() {
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
		err := <-errCh
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
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("RestoreAll", zap.Duration("take", elapsed))
	}()
	errCh := make(chan error, len(rc.databases))
	var wg sync.WaitGroup
	defer close(errCh)
	for _, db := range rc.databases {
		wg.Add(1)
		dbReplica := db
		rc.workerPool.Apply(func() {
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
		err := <-errCh
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
	stores, err := rc.pdClient.GetAllStores(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for _, store := range stores {
		opt := grpc.WithInsecure()
		gctx, cancel := context.WithTimeout(ctx, time.Second*5)
		keepAlive := 10
		keepAliveTimeout := 3
		conn, err := grpc.DialContext(
			gctx,
			store.GetAddress(),
			opt,
			grpc.WithBackoffMaxDelay(time.Second*3),
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
func (rc *Client) ValidateChecksum(tables []*utils.Table, newTables []*model.TableInfo) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("Restore Checksum", zap.Duration("take", elapsed))
	}()

	for i, t := range tables {
		table := t
		newTable := newTables[i]

		checksumResp := &tipb.ChecksumResponse{}
		startTS, err := rc.GetTS()
		if err != nil {
			return errors.Trace(err)
		}
		reqs, err := buildChecksumRequest(newTable, table, startTS)
		if err != nil {
			return errors.Trace(err)
		}
		for _, req := range reqs {
			resp, err := sendChecksumRequest(rc.ctx, rc.tikvCli.GetClient(), req)
			if err != nil {
				return errors.Trace(err)
			}
			updateChecksumResponse(checksumResp, resp)
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
			return errors.New("failed to validate checksum")
		}
	}
	log.Info("validate checksum passed!!")
	return nil
}

func buildChecksumRequest(
	newTable *model.TableInfo,
	oldTable *utils.Table,
	startTs uint64) ([]*kv.Request, error) {
	var partDefs []model.PartitionDefinition
	if part := newTable.Partition; part != nil {
		partDefs = part.Definitions
	}

	reqs := make([]*kv.Request, 0, (len(newTable.Indices)+1)*(len(partDefs)+1))
	if err := appendRequest(newTable, newTable.ID, &reqs, oldTable, startTs); err != nil {
		return nil, err
	}

	for _, partDef := range partDefs {
		if err := appendRequest(newTable, partDef.ID, &reqs, oldTable, startTs); err != nil {
			return nil, err
		}
	}
	return reqs, nil
}

func appendRequest(
	tableInfo *model.TableInfo,
	tableID int64,
	reqs *[]*kv.Request,
	oldTable *utils.Table,
	startTs uint64) error {
	req, err := buildTableRequest(tableID, oldTable, startTs)
	if err != nil {
		return err
	}

	*reqs = append(*reqs, req)
	for _, indexInfo := range tableInfo.Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		for _, oldIndexInfo := range oldTable.Schema.Indices {
			if oldIndexInfo.Name == indexInfo.Name {
				req, err = buildIndexRequest(tableID, indexInfo, oldTable.Schema.ID, oldIndexInfo, startTs)
				if err != nil {
					return err
				}
				*reqs = append(*reqs, req)
			}
		}
	}

	return nil
}

func buildTableRequest(
	tableID int64,
	oldTable *utils.Table,
	startTs uint64) (*kv.Request, error) {
	rule := &tipb.ChecksumRewriteRule{
		OldPrefix: tablecodec.GenTableRecordPrefix(oldTable.Schema.ID),
		NewPrefix: tablecodec.GenTableRecordPrefix(tableID),
	}

	checksum := &tipb.ChecksumRequest{
		StartTs:   startTs,
		ScanOn:    tipb.ChecksumScanOn_Table,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
		Rule:      rule,
	}

	ranges := ranger.FullIntRange(false)

	var builder distsql.RequestBuilder
	return builder.SetTableRanges(tableID, ranges, nil).
		SetChecksumRequest(checksum).
		SetConcurrency(variable.DefDistSQLScanConcurrency).
		Build()
}

func buildIndexRequest(
	tableID int64,
	indexInfo *model.IndexInfo,
	oldTableID int64,
	oldIndexInfo *model.IndexInfo,
	startTs uint64) (*kv.Request, error) {
	rule := &tipb.ChecksumRewriteRule{
		OldPrefix: tablecodec.EncodeTableIndexPrefix(oldTableID, oldIndexInfo.ID),
		NewPrefix: tablecodec.EncodeTableIndexPrefix(tableID, indexInfo.ID),
	}
	checksum := &tipb.ChecksumRequest{
		StartTs:   startTs,
		ScanOn:    tipb.ChecksumScanOn_Index,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
		Rule:      rule,
	}

	ranges := ranger.FullRange()

	var builder distsql.RequestBuilder
	return builder.SetIndexRanges(nil, tableID, indexInfo.ID, ranges).
		SetChecksumRequest(checksum).
		SetConcurrency(variable.DefDistSQLScanConcurrency).
		Build()
}

func sendChecksumRequest(
	ctx context.Context,
	client kv.Client,
	req *kv.Request) (resp *tipb.ChecksumResponse, err error) {
	res, err := distsql.Checksum(ctx, client, req, nil)
	if err != nil {
		return nil, err
	}
	res.Fetch(ctx)
	defer func() {
		if err1 := res.Close(); err1 != nil {
			err = err1
		}
	}()

	resp = &tipb.ChecksumResponse{}

	for {
		data, err := res.NextRaw(ctx)
		if err != nil {
			return nil, err
		}
		if data == nil {
			break
		}
		checksum := &tipb.ChecksumResponse{}
		if err = checksum.Unmarshal(data); err != nil {
			return nil, err
		}
		updateChecksumResponse(resp, checksum)
	}

	return resp, nil
}

func updateChecksumResponse(resp, update *tipb.ChecksumResponse) {
	resp.Checksum ^= update.Checksum
	resp.TotalKvs += update.TotalKvs
	resp.TotalBytes += update.TotalBytes
}
