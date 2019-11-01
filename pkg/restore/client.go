package restore

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/client"
	restore_util "github.com/pingcap/tidb-tools/pkg/restore-util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/meta"
	"github.com/pingcap/br/pkg/utils"
)

// Client sends requests to importer to restore files
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	pdClient     pd.Client
	pdAddrs      []string
	tikvCli      tikv.Storage
	fileImporter FileImporter

	databases  map[string]*utils.Database
	dbDSN      string
	backupMeta *backup.BackupMeta
	backer     *meta.Backer
}

// NewRestoreClient returns a new RestoreClient
func NewRestoreClient(ctx context.Context, pdAddrs string) (*Client, error) {
	_ctx, cancel := context.WithCancel(ctx)
	addrs := strings.Split(pdAddrs, ",")
	backer, err := meta.NewBacker(_ctx, addrs[0])
	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	tikvCli, err := tikv.Driver{}.Open(
		// Disable GC because TiDB enables GC already.
		fmt.Sprintf("tikv://%s?disableGC=true", pdAddrs))
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Client{
		ctx:      _ctx,
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

// SetDbDSN sets the DNS to connect the database to a new value
func (rc *Client) SetDbDSN(dns string) {
	rc.dbDSN = dns
}

// GetDbDSN returns a DNS to connect the database
func (rc *Client) GetDbDSN() string {
	return rc.dbDSN
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
func (rc *Client) CreateTables(tables []*utils.Table) (*restore_util.RewriteRules, error) {
	rewriteRules := &restore_util.RewriteRules{
		Table: make([]*import_sstpb.RewriteRule, 0),
		Data:  make([]*import_sstpb.RewriteRule, 0),
	}
	for _, table := range tables {
		rules, err := rc.CreateTable(table)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rewriteRules.Table = append(rewriteRules.Table, rules.Table...)
		rewriteRules.Data = append(rewriteRules.Data, rules.Data...)
	}
	return rewriteRules, nil
}

// CreateTable creates a table, and returns its rewrite rules.
func (rc *Client) CreateTable(table *utils.Table) (*restore_util.RewriteRules, error) {
	db, err := OpenDatabase(table.Db.Name.String(), rc.dbDSN)
	if err != nil {
		return nil, err
	}
	err = CreateTable(db, table)
	if err != nil {
		return nil, err
	}
	err = AlterAutoIncID(db, table)
	if err != nil {
		return nil, err
	}
	newTableInfo, err := rc.GetTableSchema(table.Db.Name, table.Schema.Name)
	if err != nil {
		return nil, err
	}
	rewriteRules := GetRewriteRules(newTableInfo, table.Schema)
	return rewriteRules, nil
}

// RestoreTable tries to restore the data of a table.
func (rc *Client) RestoreTable(table *utils.Table, rewriteRules *restore_util.RewriteRules, restoreTS uint64) error {
	log.Info("start to restore table",
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
		go func(file *backup.File) {
			defer wg.Done()
			select {
			case <-rc.ctx.Done():
				errCh <- nil
			case errCh <- rc.fileImporter.Import(file, encodedRules):
			}
		}(file)
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
func (rc *Client) RestoreDatabase(db *utils.Database, rewriteRules *restore_util.RewriteRules, restoreTS uint64) error {
	errCh := make(chan error, len(db.Tables))
	var wg sync.WaitGroup
	defer close(errCh)
	for _, table := range db.Tables {
		wg.Add(1)
		go func(table *utils.Table) {
			defer wg.Done()
			select {
			case <-rc.ctx.Done():
				errCh <- nil
			case errCh <- rc.RestoreTable(table, rewriteRules, restoreTS):
			}
		}(table)
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
func (rc *Client) RestoreAll(rewriteRules *restore_util.RewriteRules, restoreTS uint64) error {
	errCh := make(chan error, len(rc.databases))
	var wg sync.WaitGroup
	defer close(errCh)
	for _, db := range rc.databases {
		wg.Add(1)
		go func(db *utils.Database) {
			defer wg.Done()
			select {
			case <-rc.ctx.Done():
				errCh <- nil
			case errCh <- rc.RestoreDatabase(db, rewriteRules, restoreTS):
			}
		}(db)
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
		storeID := store.GetId()
		err := withRetry(func() error {
			conn, err := rc.backer.GetGrpcConn(storeID)
			client := import_sstpb.NewImportSSTClient(conn)
			_, err = client.SwitchMode(ctx, &import_sstpb.SwitchModeRequest{
				Mode: mode,
			})
			return err
		}, func(e error) bool {
			if err := rc.backer.ResetGrpcClient(storeID); err != nil {
				return false
			}
			return true
		}, 2, 10*time.Millisecond)
		if err != nil {
			log.Error("failed to switch mode", zap.Error(err))
			continue
		}
	}
	return nil
}

func getTableRewriteRule(tid int64, rules []*import_sstpb.RewriteRule) *tipb.ChecksumRewriteRule {
	for _, r := range rules {
		tableID := tablecodec.DecodeTableID(r.GetOldKeyPrefix())
		if tableID != 0 && tableID == tid {
			return &tipb.ChecksumRewriteRule{
				OldPrefix: r.GetOldKeyPrefix(),
				NewPrefix: r.GetNewKeyPrefix(),
			}
		}
	}
	return nil
}

func (rc *Client) ValidateChecksum(rewriteRules restore_util.RewriteRules) (bool, error) {
	var tables []*utils.Table
	for _, db := range rc.databases {
		for _, t := range db.Tables {
			tables = append(tables, t)
		}
	}
	for _, table := range tables {
		rule := getTableRewriteRule(table.Schema.ID, rewriteRules.Table)
		newTableID := tablecodec.DecodeTableID(rule.GetNewPrefix())
		if newTableID == 0 || rule == nil {
			// get rewrite rule failed
			return false, nil
		}
		checksumReq := tipb.ChecksumRequest{
			StartTs:   rc.backupMeta.GetEndVersion(),
			ScanOn:    tipb.ChecksumScanOn_Table,
			Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
			Rule:      rule,
		}
		data, err := checksumReq.Marshal()
		if err != nil {
			return false, errors.Trace(err)
		}
		resp, err := rc.checksumTable(newTableID, table.Schema, data)
		if err != nil {
			return false, errors.Trace(err)
		}
		if resp.Checksum != table.Crc64Xor ||
			resp.TotalKvs != table.TotalKvs ||
			resp.TotalBytes != table.TotalBytes {
			return false, nil
		}
	}
	return true, nil
}

func (rc *Client) checksumTable(tableID int64, tableInfo *model.TableInfo, reqData []byte) (*tipb.ChecksumResponse, error) {
	checksumResp := &tipb.ChecksumResponse{}
	start := tablecodec.EncodeTablePrefix(tableID)
	end := tablecodec.EncodeTablePrefix(tableID + 1)
	var nextStart []byte
	for bytes.Compare(start, end) < 0 {
		region, peer, err := rc.pdClient.GetRegion(rc.ctx, codec.EncodeBytes([]byte{}, start))
		if len(region.GetEndKey()) < 9 {
			nextStart = end
		} else {
			if _, regionEnd, err := codec.DecodeBytes(region.GetEndKey(), nil); err != nil {
				return nil, errors.Trace(err)
			} else if bytes.Compare(regionEnd, end) < 0 {
				nextStart = regionEnd
			} else {
				nextStart = end
			}
		}
		storeID := peer.GetStoreId()
		rc.backer.ResetGrpcClient(storeID)
		kvClient, err := rc.backer.GetTikvClient(storeID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reqCtx := &kvrpcpb.Context{
			RegionId:    region.GetId(),
			RegionEpoch: region.GetRegionEpoch(),
			Peer:        peer,
		}
		ranges := []*coprocessor.KeyRange{&coprocessor.KeyRange{Start: start, End: nextStart}}
		req := &coprocessor.Request{
			Context: reqCtx,
			Tp:      105, // REQ_TYPE_CHECKSUM flag
			Data:    reqData,
			Ranges:  ranges,
		}
		resp, err := kvClient.Coprocessor(rc.ctx, req)
		if err != nil || resp.GetRegionError() != nil || resp.GetOtherError() != "" || resp.GetLocked() != nil {
			log.Error("Coprocessor request error",
				zap.Any("RegionError", resp.GetRegionError()),
				zap.String("OtherError", resp.GetOtherError()),
				zap.Any("Locked", resp.GetLocked()))
			return nil, errors.Trace(err)
		}
		checksum := &tipb.ChecksumResponse{}
		if err = checksum.Unmarshal(resp.Data); err != nil {
			return nil, errors.Trace(err)
		}
		if checksum == nil {
			continue
		}
		checksumResp.Checksum ^= checksum.Checksum
		checksumResp.TotalKvs += checksum.TotalKvs
		checksumResp.TotalBytes += checksum.TotalBytes
		start = nextStart
	}
	return checksumResp, nil
}
