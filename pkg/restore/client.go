package restore

import (
	"context"
	"strings"

	"github.com/pingcap/br/pkg/meta"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Client sends requests to importer to restore files
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	pdClient        pd.Client
	pdAddr          string
	importerClients []import_kvpb.ImportKVClient

	databases      map[string]*Database
	dbDSN          string
	statusAddr     string
	maxOpenEngines int
	maxImportJobs  int
	backupMeta     *backup.BackupMeta
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
func (rc *Client) InitBackupMeta(backupMeta *backup.BackupMeta, partitionSize int) error {
	databases, err := LoadBackupTables(backupMeta, partitionSize)
	if err != nil {
		return errors.Trace(err)
	}
	rc.databases = databases
	rc.backupMeta = backupMeta
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

// SetMaxOpenEngines sets the max number of opened importer engine files
func (rc *Client) SetMaxOpenEngines(v int) {
	rc.maxOpenEngines = v
}

// SetMaxImportJobs sets the max number of concurrent import engine jobs
func (rc *Client) SetMaxImportJobs(v int) {
	rc.maxImportJobs = v
}

// SetImportAddr sets the address to connect the importer
func (rc *Client) SetImportAddr(importerAddr string) error {
	addrs := strings.Split(importerAddr, ",")
	clients := make([]import_kvpb.ImportKVClient, 0)
	for _, addr := range addrs {
		conn, err := grpc.DialContext(rc.ctx, addr, grpc.WithInsecure())
		if err != nil {
			log.Error("connect to importer server failed", zap.Error(err))
			return errors.Trace(err)
		}
		cli := import_kvpb.NewImportKVClient(conn)
		clients = append(clients, cli)
	}
	rc.importerClients = clients
	return nil
}

// GetImportKVClients returns a new ImportKVClient
func (rc *Client) GetImportKVClients() []import_kvpb.ImportKVClient {
	return rc.importerClients
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

// UploadTableFiles executes the job to restore files of a table
func (rc *Client) UploadTableFiles(client import_kvpb.ImportKVClient, group *FileGroup, restoreTS uint64) error {
	log.Info("start to upload files of group",
		zap.String("group", group.Schema.Name.String()),
		zap.String("uuid", group.UUID.String()),
		zap.String("db", group.Db.Name.String()),
	)

	tableInfo, returnErr := FetchTableInfo(rc.statusAddr, group.Db.Name.String(), group.Schema.Name.String())
	if returnErr != nil {
		return errors.Trace(returnErr)
	}
	tableIDs, indexIDs := GroupIDPairs(group.Schema, tableInfo)

	returnErr = rc.OpenEngine(client, group.UUID.Bytes())
	if returnErr != nil {
		log.Error("open engine failed",
			zap.Uint64("restore_ts", restoreTS),
			zap.String("group", group.Schema.Name.String()),
			zap.String("uuid", group.UUID.String()),
			zap.String("db", group.Db.Name.String()),
		)
		return errors.Trace(returnErr)
	}

	errCh := make(chan error, len(group.Files))
	defer close(errCh)
	for _, file := range group.Files {
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
					Uuid:      group.UUID.Bytes(),
				}
				sendErr := func(err error) {
					log.Error("restore file failed",
						zap.Reflect("file", file),
						zap.Uint64("restore_ts", restoreTS),
						zap.String("group", group.Schema.Name.String()),
						zap.String("db", group.Db.Name.String()),
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
					zap.String("group", group.Schema.Name.String()),
					zap.String("db", group.Db.Name.String()),
				)
				errCh <- nil
			}(file)
		}
	}

	for i := 0; i < len(group.Files); i++ {
		err := <-errCh
		if err != nil {
			returnErr = err
		}
	}
	if returnErr != nil {
		return errors.Trace(returnErr)
	}

	returnErr = rc.CloseEngine(client, group.UUID.Bytes())
	if returnErr != nil {
		log.Error("close engine failed",
			zap.Uint64("restore_ts", restoreTS),
			zap.String("group", group.Schema.Name.String()),
			zap.String("uuid", group.UUID.String()),
			zap.String("db", group.Db.Name.String()),
		)
		return errors.Trace(returnErr)
	}

	log.Info("upload group files finished",
		zap.Uint64("restore_ts", restoreTS),
		zap.String("group", group.Schema.Name.String()),
		zap.String("uuid", group.UUID.String()),
		zap.String("db", group.Db.Name.String()),
	)

	return errors.Trace(returnErr)
}

type uploadJob struct {
	Group *FileGroup
}

type importJob struct {
	Group  *FileGroup
	Client import_kvpb.ImportKVClient
}

// RestoreMultipleTables executes the job to restore multiple tables
func (rc *Client) RestoreMultipleTables(groups []*FileGroup, restoreTS uint64) error {
	log.Info("start to restore multiple table",
		zap.Int("len", len(groups)),
	)
	var returnErr error
	errCh := make(chan error, len(groups))
	uploadJobCh := make(chan uploadJob, len(groups))
	importJobCh := make(chan importJob, len(groups))
	defer close(errCh)
	defer close(uploadJobCh)
	defer close(importJobCh)

	clients := rc.GetImportKVClients()
	for i := 0; i < len(clients); i++ {
		n := i % len(clients)
		cli := clients[n]
		for j := 0; j < rc.maxOpenEngines; j++ {
			go func(client import_kvpb.ImportKVClient, n int) {
				for job := range uploadJobCh {
					log.Info("restore table", zap.Int("importer", n), zap.String("uuid", job.Group.UUID.String()))
					err := rc.UploadTableFiles(client, job.Group, restoreTS)
					if err != nil {
						errCh <- errors.Trace(err)
						return
					}
					importJobCh <- importJob{
						Group:  job.Group,
						Client: client,
					}
				}
			}(cli, n)
		}
	}

	for i := 0; i < rc.maxImportJobs; i++ {
		go func() {
			for job := range importJobCh {
				group := job.Group
				log.Info("start to import group",
					zap.Uint64("restore_ts", restoreTS),
					zap.String("group", group.Schema.Name.String()),
					zap.String("uuid", group.UUID.String()),
					zap.String("db", group.Db.Name.String()),
				)
				err := rc.ImportEngine(job.Client, group.UUID.Bytes())
				if err != nil {
					log.Error("import engine failed",
						zap.Uint64("restore_ts", restoreTS),
						zap.String("group", group.Schema.Name.String()),
						zap.String("uuid", group.UUID.String()),
						zap.String("db", group.Db.Name.String()),
						zap.Error(err),
					)
					errCh <- errors.Trace(err)
					return
				}
				err = rc.CleanupEngine(job.Client, group.UUID.Bytes())
				if err != nil {
					log.Error("cleanup engine failed",
						zap.Uint64("restore_ts", restoreTS),
						zap.String("group", group.Schema.Name.String()),
						zap.String("uuid", group.UUID.String()),
						zap.String("db", group.Db.Name.String()),
						zap.Error(err),
					)
					errCh <- errors.Trace(err)
					return
				}
				log.Info("import group finished",
					zap.Uint64("restore_ts", restoreTS),
					zap.String("group", group.Schema.Name.String()),
					zap.String("uuid", group.UUID.String()),
					zap.String("db", group.Db.Name.String()),
				)
				errCh <- nil
			}
		}()
	}

	go func() {
		for _, group := range groups {
			select {
			case <-rc.ctx.Done():
				return
			default:
				uploadJobCh <- uploadJob{
					Group: group,
				}
			}
		}
	}()

	for i := 0; i < len(groups); i++ {
		returnErr = <-errCh
		if returnErr != nil {
			return errors.Trace(returnErr)
		}
	}
	return nil
}

// RestoreDatabase executes the job to restore a database
func (rc *Client) RestoreDatabase(db *Database, restoreTS uint64) error {
	returnErr := CreateDatabase(db.Schema, rc.dbDSN)
	if returnErr != nil {
		return returnErr
	}
	for _, table := range db.Tables {
		returnErr = CreateTable(db.Schema.Name.String(), table, rc.dbDSN)
		if returnErr != nil {
			return errors.Trace(returnErr)
		}
		returnErr = AlterAutoIncID(db.Schema.Name.String(), table, rc.dbDSN)
		if returnErr != nil {
			return errors.Trace(returnErr)
		}
	}
	returnErr = rc.RestoreMultipleTables(db.FileGroups, restoreTS)
	if returnErr == nil {
		log.Info("restore database finished",
			zap.Uint64("restore_ts", restoreTS),
			zap.String("db", db.Schema.Name.String()),
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
func (rc *Client) OpenEngine(client import_kvpb.ImportKVClient, uuid []byte) error {
	req := &import_kvpb.OpenEngineRequest{
		Uuid: uuid,
	}
	_, err := client.OpenEngine(rc.ctx, req)
	return errors.Trace(err)
}

// ImportEngine sends a ImportEngine request to importer
func (rc *Client) ImportEngine(client import_kvpb.ImportKVClient, uuid []byte) error {
	req := &import_kvpb.ImportEngineRequest{
		Uuid:   uuid,
		PdAddr: rc.pdAddr,
	}
	_, err := client.ImportEngine(rc.ctx, req)
	return errors.Trace(err)
}

// CloseEngine sends a CloseEngine request to importer
func (rc *Client) CloseEngine(client import_kvpb.ImportKVClient, uuid []byte) error {
	req := &import_kvpb.CloseEngineRequest{
		Uuid: uuid,
	}
	_, err := client.CloseEngine(rc.ctx, req)
	return errors.Trace(err)
}

// CleanupEngine sends a CleanupEngine request to importer
func (rc *Client) CleanupEngine(client import_kvpb.ImportKVClient, uuid []byte) error {
	req := &import_kvpb.CleanupEngineRequest{
		Uuid: uuid,
	}
	_, err := client.CleanupEngine(rc.ctx, req)
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
	clients := rc.GetImportKVClients()
	for _, cli := range clients {
		_, err := cli.SwitchMode(rc.ctx, req)
		if err != nil {
			log.Error("switch cluster mode failed", zap.Reflect("mode", mode))
			return errors.Trace(err)
		}
	}
	return nil
}

// CompactCluster sends a CompactCluster request to importer
func (rc *Client) CompactCluster() error {
	req := &import_kvpb.CompactClusterRequest{
		PdAddr: rc.pdAddr,
	}
	clients := rc.GetImportKVClients()
	for _, cli := range clients {
		_, err := cli.CompactCluster(rc.ctx, req)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
