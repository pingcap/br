package restore

import (
	"context"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"strings"

	"github.com/pingcap/br/pkg/meta"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Client sends requests to importer to restore files
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	pdClient     pd.Client
	pdAddr       string
	importerAddr string

	databases  map[string]*Database
	dbDNS      string
	statusAddr string
	backupMeta *backup.BackupMeta
}

// NewRestoreClient returns a new RestoreClient
func NewRestoreClient(ctx context.Context, pdAddrs string) (*Client, error) {
	_ctx, cancel := context.WithCancel(ctx)
	addrs := strings.Split(pdAddrs, ",")
	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("new region client", zap.String("pdAddrs", pdAddrs))
	return &Client{
		ctx:      _ctx,
		cancel:   cancel,
		pdClient: pdClient,
		pdAddr:   addrs[0],
	}, nil
}

// InitBackupMeta loads schemas from BackupMeta to initialize RestoreClient
func (rc *Client) InitBackupMeta(backupMeta *backup.BackupMeta, partitionCount int) error {
	databases, err := LoadBackupTables(backupMeta, partitionCount)
	if err != nil {
		return errors.Trace(err)
	}
	rc.databases = databases
	rc.backupMeta = backupMeta
	return nil
}

// SetDbDNS sets the DNS to connect the database to a new value
func (rc *Client) SetDbDNS(dbDNS string) {
	rc.dbDNS = dbDNS
}

// GetDbDNS returns a DNS to connect the database
func (rc *Client) GetDbDNS() string {
	return rc.dbDNS
}

// SetImportAddr sets the address to connect the importer
func (rc *Client) SetImportAddr(addr string) {
	rc.importerAddr = addr
}

// GetImportKVClient returns a new ImportKVClient
func (rc *Client) GetImportKVClient() (import_kvpb.ImportKVClient, error) {
	conn, err := grpc.DialContext(rc.ctx, rc.importerAddr, grpc.WithInsecure())
	if err != nil {
		log.Error("connect to importer server failed", zap.Error(err))
		return nil, errors.Trace(err)
	}
	client := import_kvpb.NewImportKVClient(conn)
	return client, nil
}

// SetStatusAddr sets the address to check status of TiDB to a new value
func (rc *Client) SetStatusAddr(statusAddr string) {
	rc.statusAddr = statusAddr
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
	log.Info("restore timestamp", zap.Uint64("RestoreTS", restoreTS))
	return restoreTS, nil
}

// GetDatabase returns a database instance by name
func (rc *Client) GetDatabase(name string) *Database {
	return rc.databases[name]
}

// RestoreTable executes the job to restore a table
func (rc *Client) RestoreTable(table *Table, restoreTS uint64) error {
	log.Info("start to restore table",
		zap.String("table", table.Schema.Name.O),
		zap.String("db", table.Db.Name.O),
	)

	dns := rc.dbDNS + table.Db.Name.O
	returnErr := CreateTable(table, dns)
	if returnErr != nil {
		return errors.Trace(returnErr)
	}
	tableInfo, returnErr := FetchTableInfo(rc.statusAddr, table.Db.Name.O, table.Schema.Name.O)
	if returnErr != nil {
		return errors.Trace(returnErr)
	}
	tableIDs, indexIDs := GroupIDPairs(table.Schema, tableInfo)

	returnErr = rc.OpenEngine(table.UUID.Bytes())
	if returnErr != nil {
		log.Error("open engine failed",
			zap.Uint64("restore_ts", restoreTS),
			zap.String("table", table.Schema.Name.O),
			zap.String("uuid", table.UUID.String()),
			zap.String("db", table.Db.Name.O),
		)
		return errors.Trace(returnErr)
	}

	errCh := make(chan error)
	defer close(errCh)
	var clients []import_kvpb.ImportKVClient
	for i := 0; i < 4; i++ {
		c, err := rc.GetImportKVClient()
		if err != nil {
			return errors.Trace(err)
		}
		clients = append(clients, c)
	}
	for i, file := range table.Files {
		client := clients[i%len(clients)]
		select {
		case <-rc.ctx.Done():
			return nil
		default:
			go func(file *FilePair) {
				req := &import_kvpb.RestoreFileRequest{
					Default:   file.Default,
					Write:     file.Write,
					Path:      rc.backupMeta.Path,
					PdAddr:    rc.pdAddr,
					TableIds:  tableIDs,
					IndexIds:  indexIDs,
					RestoreTs: restoreTS,
					Uuid:      table.UUID.Bytes(),
				}
				sendErr := func(err error) {
					log.Error("restore file failed",
						zap.Reflect("file", file),
						zap.Uint64("restore_ts", restoreTS),
						zap.String("table", table.Schema.Name.O),
						zap.String("db", table.Db.Name.O),
						zap.Error(errors.Trace(err)),
					)
					errCh <- errors.Trace(err)
				}
				_, err := client.RestoreFile(rc.ctx, req)
				if err != nil {
					sendErr(err)
					return
				}
				log.Debug("restore file success",
					zap.Reflect("file", file),
					zap.Uint64("restore_ts", restoreTS),
					zap.String("table", table.Schema.Name.O),
					zap.String("db", table.Db.Name.O),
				)
				errCh <- nil
			}(file)
		}
	}

	for i := 0; i < len(table.Files); i++ {
		err := <-errCh
		if err != nil {
			returnErr = err
		}
	}
	if returnErr != nil {
		return errors.Trace(returnErr)
	}

	returnErr = rc.CloseEngine(table.UUID.Bytes())
	if returnErr != nil {
		log.Error("close engine failed",
			zap.Uint64("restore_ts", restoreTS),
			zap.String("table", table.Schema.Name.O),
			zap.String("uuid", table.UUID.String()),
			zap.String("db", table.Db.Name.O),
		)
		return errors.Trace(returnErr)
	}
	returnErr = rc.ImportEngine(table.UUID.Bytes())
	if returnErr != nil {
		log.Error("import engine failed",
			zap.Uint64("restore_ts", restoreTS),
			zap.String("table", table.Schema.Name.O),
			zap.String("uuid", table.UUID.String()),
			zap.String("db", table.Db.Name.O),
		)
		return errors.Trace(returnErr)
	}

	returnErr = rc.CleanupEngine(table.UUID.Bytes())
	if returnErr != nil {
		log.Error("cleanup engine failed",
			zap.Uint64("restore_ts", restoreTS),
			zap.String("table", table.Schema.Name.O),
			zap.String("uuid", table.UUID.String()),
			zap.String("db", table.Db.Name.O),
		)
		return errors.Trace(returnErr)
	}

	log.Info("restore table finished",
		zap.Uint64("restore_ts", restoreTS),
		zap.String("table", table.Schema.Name.O),
		zap.String("uuid", table.UUID.String()),
		zap.String("db", table.Db.Name.O),
	)

	return errors.Trace(returnErr)
}

// RestoreMultipleTables executes the job to restore multiple tables
func (rc *Client) RestoreMultipleTables(tables []*Table, restoreTS uint64) error {
	log.Info("start to restore multiple table",
		zap.Int("len", len(tables)),
	)
	var returnErr error
	errCh := make(chan error)
	defer close(errCh)
	for _, table := range tables {
		select {
		case <-rc.ctx.Done():
			return nil
		default:
			go func(t *Table) {
				err := rc.RestoreTable(t, restoreTS)
				if err != nil {
					errCh <- errors.Trace(err)
				}
				errCh <- nil
			}(table)
		}
	}

	for i := 0; i < len(tables); i++ {
		err := <-errCh
		if err != nil {
			returnErr = err
		}
	}
	return errors.Trace(returnErr)
}

// RestoreDatabase executes the job to restore a database
func (rc *Client) RestoreDatabase(db *Database, restoreTS uint64) error {
	returnErr := CreateDatabase(db.Schema, rc.dbDNS)
	if returnErr != nil {
		return returnErr
	}
	returnErr = rc.RestoreMultipleTables(db.Tables, restoreTS)
	if returnErr == nil {
		log.Info("restore database finished",
			zap.Uint64("restore_ts", restoreTS),
			zap.String("db", db.Schema.Name.O),
		)
	}
	return errors.Trace(returnErr)
}

// RestoreAll executes the job to restore all files
func (rc *Client) RestoreAll(restoreTS uint64) error {
	errCh := make(chan error)
	defer close(errCh)
	for _, db := range rc.databases {
		select {
		case <-rc.ctx.Done():
			return nil
		default:
			go func(d *Database) {
				err := rc.RestoreDatabase(d, restoreTS)
				if err != nil {
					errCh <- errors.Trace(err)
				}
				errCh <- nil
			}(db)
		}
	}

	var returnErr error
	for i := 0; i < len(rc.databases); i++ {
		err := <-errCh
		if err != nil {
			returnErr = err
		}
	}
	if returnErr == nil {
		log.Info("restore all finished", zap.Uint64("restore_ts", restoreTS))
	}
	return returnErr
}

// OpenEngine sends a OpenEngine request to importer
func (rc *Client) OpenEngine(uuid []byte) error {
	req := &import_kvpb.OpenEngineRequest{
		Uuid: uuid,
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = client.OpenEngine(rc.ctx, req)
	return errors.Trace(err)
}

// ImportEngine sends a ImportEngine request to importer
func (rc *Client) ImportEngine(uuid []byte) error {
	req := &import_kvpb.ImportEngineRequest{
		Uuid:   uuid,
		PdAddr: rc.pdAddr,
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = client.ImportEngine(rc.ctx, req)
	return errors.Trace(err)
}

// CloseEngine sends a CloseEngine request to importer
func (rc *Client) CloseEngine(uuid []byte) error {
	req := &import_kvpb.CloseEngineRequest{
		Uuid: uuid,
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = client.CloseEngine(rc.ctx, req)
	return errors.Trace(err)
}

// CleanupEngine sends a CleanupEngine request to importer
func (rc *Client) CleanupEngine(uuid []byte) error {
	req := &import_kvpb.CleanupEngineRequest{
		Uuid: uuid,
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = client.CleanupEngine(rc.ctx, req)
	return errors.Trace(err)
}

// SwitchClusterMode sends a SwitchClusterMode request to importer
func (rc *Client) SwitchClusterMode(mode import_sstpb.SwitchMode) error {
	req := &import_kvpb.SwitchModeRequest{
		PdAddr: rc.pdAddr,
		Request: &import_sstpb.SwitchModeRequest{
			Mode: mode,
		},
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		log.Error("switch cluster mode failed", zap.Reflect("mode", mode))
		return errors.Trace(err)
	}
	_, err = client.SwitchMode(rc.ctx, req)
	if err != nil {
		log.Error("switch cluster mode failed", zap.Reflect("mode", mode))
	}
	return errors.Trace(err)
}

// CompactCluster sends a CompactCluster request to importer
func (rc *Client) CompactCluster() error {
	req := &import_kvpb.CompactClusterRequest{
		PdAddr: rc.pdAddr,
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = client.CompactCluster(rc.ctx, req)
	return errors.Trace(err)
}
