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

var MaxFileNumPerEngine int = 100

type RestoreClient struct {
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

func NewRestoreClient(ctx context.Context, pdAddrs string) (*RestoreClient, error) {
	_ctx, cancel := context.WithCancel(ctx)
	addrs := strings.Split(pdAddrs, ",")
	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("new region client", zap.String("pdAddrs", pdAddrs))
	return &RestoreClient{
		ctx:      _ctx,
		cancel:   cancel,
		pdClient: pdClient,
		pdAddr:   addrs[0],
	}, nil
}

func (rc *RestoreClient) InitBackupMeta(backupMeta *backup.BackupMeta) error {
	databases, err := LoadBackupTables(backupMeta)
	if err != nil {
		return errors.Trace(err)
	}
	rc.databases = databases
	rc.backupMeta = backupMeta
	return nil
}

func (rc *RestoreClient) SetDbDNS(dbDns string) {
	rc.dbDNS = dbDns
}

func (rc *RestoreClient) GetDbDNS() string {
	return rc.dbDNS
}

func (rc *RestoreClient) SetImportAddr(addr string) {
	rc.importerAddr = addr
}

func (rc *RestoreClient) GetImportKVClient() (import_kvpb.ImportKVClient, error) {
	conn, err := grpc.DialContext(rc.ctx, rc.importerAddr, grpc.WithInsecure())
	if err != nil {
		log.Error("connect to importer server failed", zap.Error(err))
		return nil, errors.Trace(err)
	}
	client := import_kvpb.NewImportKVClient(conn)
	return client, nil
}

func (rc *RestoreClient) SetStatusAddr(statusAddr string) {
	rc.statusAddr = statusAddr
}

func (rc *RestoreClient) GetTS() (uint64, error) {
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

func (rc *RestoreClient) GetDatabase(name string) *Database {
	return rc.databases[name]
}

func (rc *RestoreClient) RestoreTable(table *Table, restoreTS uint64) error {
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
	tableIDs, indexIDs := GroupIDPairs(table.Schema, tableInfo)

	returnErr = rc.OpenEngine(table.Uuid)
	if returnErr != nil {
		return errors.Trace(returnErr)
	}

	errCh := make(chan error)
	defer close(errCh)
	var clients []import_kvpb.ImportKVClient
	for i := 0; i < 4; i++ {
		c, returnErr := rc.GetImportKVClient()
		if returnErr != nil {
			return errors.Trace(returnErr)
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
					Uuid:      table.Uuid,
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

	returnErr = rc.CloseEngine(table.Uuid)
	if returnErr != nil {
		return errors.Trace(returnErr)
	}
	log.Info("start to import table",
		zap.Uint64("restore_ts", restoreTS),
		zap.String("table", table.Schema.Name.O),
		zap.String("db", table.Db.Name.O),
	)
	returnErr = rc.ImportEngine(table.Uuid)
	if returnErr != nil {
		return errors.Trace(returnErr)
	}
	log.Info("import table success",
		zap.Uint64("restore_ts", restoreTS),
		zap.String("table", table.Schema.Name.O),
		zap.String("db", table.Db.Name.O),
	)

	log.Info("restore table finished",
		zap.Uint64("restore_ts", restoreTS),
		zap.String("table", table.Schema.Name.O),
		zap.String("db", table.Db.Name.O),
	)

	returnErr = rc.CleanupEngine(table.Uuid)
	if returnErr != nil {
		return errors.Trace(returnErr)
	}
	returnErr = AnalyzeTable(table, dns)

	return errors.Trace(returnErr)
}

func (rc *RestoreClient) RestoreDatabase(db *Database, restoreTS uint64) error {
	returnErr := CreateDatabase(db.Schema, rc.dbDNS)
	if returnErr != nil {
		return returnErr
	}

	errCh := make(chan error)
	defer close(errCh)
	for _, table := range db.Tables {
		select {
		case <-rc.ctx.Done():
			return nil
		default:
			go func() {
				err := rc.RestoreTable(table, restoreTS)
				if err != nil {
					errCh <- errors.Trace(err)
				}
				errCh <- nil
			}()
		}
	}

	for i := 0; i < len(db.Tables); i++ {
		err := <-errCh
		if err != nil {
			returnErr = err
		}
	}
	if returnErr == nil {
		log.Info("restore database finished",
			zap.Uint64("restore_ts", restoreTS),
			zap.String("db", db.Schema.Name.O),
		)
	}
	return returnErr
}

func (rc *RestoreClient) RestoreAll(restoreTS uint64) error {
	errCh := make(chan error)
	defer close(errCh)
	for _, db := range rc.databases {
		select {
		case <-rc.ctx.Done():
			return nil
		default:
			go func() {
				err := rc.RestoreDatabase(db, restoreTS)
				if err != nil {
					errCh <- errors.Trace(err)
				}
				errCh <- nil
			}()
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

func (rc *RestoreClient) OpenEngine(uuid []byte) error {
	req := &import_kvpb.OpenEngineRequest{
		Uuid: uuid,
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		return err
	}
	_, err = client.OpenEngine(rc.ctx, req)
	return err
}

func (rc *RestoreClient) ImportEngine(uuid []byte) error {
	req := &import_kvpb.ImportEngineRequest{
		Uuid:   uuid,
		PdAddr: rc.pdAddr,
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		return err
	}
	_, err = client.ImportEngine(rc.ctx, req)
	return err
}

func (rc *RestoreClient) CloseEngine(uuid []byte) error {
	req := &import_kvpb.CloseEngineRequest{
		Uuid: uuid,
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		return err
	}
	_, err = client.CloseEngine(rc.ctx, req)
	return err
}

func (rc *RestoreClient) CleanupEngine(uuid []byte) error {
	req := &import_kvpb.CleanupEngineRequest{
		Uuid: uuid,
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		return err
	}
	_, err = client.CleanupEngine(rc.ctx, req)
	return err
}

func (rc *RestoreClient) SwitchClusterMode(mode import_sstpb.SwitchMode) error {
	req := &import_kvpb.SwitchModeRequest{
		PdAddr: rc.pdAddr,
		Request: &import_sstpb.SwitchModeRequest{
			Mode: mode,
		},
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		return err
	}
	_, err = client.SwitchMode(rc.ctx, req)
	return err
}

func (rc *RestoreClient) CompactCluster() error {
	req := &import_kvpb.CompactClusterRequest{
		PdAddr: rc.pdAddr,
	}
	client, err := rc.GetImportKVClient()
	if err != nil {
		return err
	}
	_, err = client.CompactCluster(rc.ctx, req)
	return err
}
