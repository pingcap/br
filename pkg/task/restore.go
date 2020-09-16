// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/spf13/pflag"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/conn"
	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

const (
	flagOnline   = "online"
	flagNoSchema = "no-schema"

	defaultRestoreConcurrency = 128
	maxRestoreBatchSizeLimit  = 256
	defaultDDLConcurrency     = 16
)

// RestoreConfig is the configuration specific for restore tasks.
type RestoreConfig struct {
	Config

	Online   bool `json:"online" toml:"online"`
	NoSchema bool `json:"no-schema" toml:"no-schema"`
}

// DefineRestoreFlags defines common flags for the restore command.
func DefineRestoreFlags(flags *pflag.FlagSet) {
	// TODO remove experimental tag if it's stable
	flags.Bool(flagOnline, false, "(experimental) Whether online when restore")
	flags.Bool(flagNoSchema, false, "skip creating schemas and tables, reuse existing empty ones")

	// Do not expose this flag
	_ = flags.MarkHidden(flagNoSchema)
}

// ParseFromFlags parses the restore-related flags from the flag set.
func (cfg *RestoreConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.Online, err = flags.GetBool(flagOnline)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.NoSchema, err = flags.GetBool(flagNoSchema)
	if err != nil {
		return errors.Trace(err)
	}
	err = cfg.Config.ParseFromFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultRestoreConcurrency
	}
	return nil
}

// adjustRestoreConfig is use for BR(binary) and BR in TiDB.
// When new config was add and not included in parser.
// we should set proper value in this function.
// so that both binary and TiDB will use same default value.
func (cfg *RestoreConfig) adjustRestoreConfig() {
	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultRestoreConcurrency
	}
	if cfg.Config.SwitchModeInterval == 0 {
		cfg.Config.SwitchModeInterval = defaultSwitchInterval
	}
}

// RunRestore starts a restore task inside the current goroutine.
func RunRestore(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	cfg.adjustRestoreConfig()

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	mgr, err := newMgr(ctx, g, cfg.PD, cfg.TLS, cfg.CheckRequirements)
	if err != nil {
		return err
	}
	defer mgr.Close()

	client, err := restore.NewRestoreClient(g, mgr.GetPDClient(), mgr.GetTiKV(), mgr.GetTLSConfig())
	if err != nil {
		return err
	}
	defer client.Close()

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return err
	}
	if err = client.SetStorage(ctx, u, cfg.SendCreds); err != nil {
		return err
	}
	client.SetRateLimit(cfg.RateLimit)
	client.SetConcurrency(uint(cfg.Concurrency))
	if cfg.Online {
		client.EnableOnline()
	}
	if cfg.NoSchema {
		client.EnableSkipCreateSQL()
	}
	client.SetSwitchModeInterval(cfg.SwitchModeInterval)
	err = client.LoadRestoreStores(ctx)
	if err != nil {
		return err
	}

	u, _, backupMeta, err := ReadBackupMeta(ctx, utils.MetaFile, &cfg.Config)
	if err != nil {
		return err
	}
	g.Record("Size", utils.ArchiveSize(backupMeta))
	if err = client.InitBackupMeta(backupMeta, u); err != nil {
		return err
	}

	if client.IsRawKvMode() {
		return errors.Annotate(berrors.ErrRestoreModeMismatch, "cannot do transactional restore from raw kv data")
	}

	files, tables, dbs := filterRestoreFiles(client, cfg)
	if len(dbs) == 0 && len(tables) != 0 {
		return errors.Annotate(berrors.ErrRestoreInvalidBackup, "contain tables but no databases")
	}

	var newTS uint64
	if client.IsIncremental() {
		newTS, err = client.GetTS(ctx)
		if err != nil {
			return err
		}
	}
	ddlJobs := restore.FilterDDLJobs(client.GetDDLJobs(), tables)

	// pre-set TiDB config for restore
	enableTiDBConfig()

	// execute DDL first
	err = client.ExecDDLs(ctx, ddlJobs)
	if err != nil {
		return errors.Trace(err)
	}

	// nothing to restore, maybe only ddl changes in incremental restore
	if len(dbs) == 0 && len(tables) == 0 {
		log.Info("nothing to restore, all databases and tables are filtered out")
		// even nothing to restore, we show a success message since there is no failure.
		summary.SetSuccessStatus(true)
		return nil
	}

	for _, db := range dbs {
		err = client.CreateDatabase(ctx, db.Info)
		if err != nil {
			return err
		}
	}

	// We make bigger errCh so we won't block on multi-part failed.
	errCh := make(chan error, 32)
	// Maybe allow user modify the DDL concurrency isn't necessary,
	// because executing DDL is really I/O bound (or, algorithm bound?),
	// and we cost most of time at waiting DDL jobs be enqueued.
	// So these jobs won't be faster or slower when machine become faster or slower,
	// hence make it a fixed value would be fine.
	var dbPool []*restore.DB
	if g.OwnsStorage() {
		// Only in binary we can use multi-thread sessions to create tables.
		// so use OwnStorage() to tell whether we are use binary or SQL.
		dbPool, err = restore.MakeDBPool(defaultDDLConcurrency, func() (*restore.DB, error) {
			return restore.NewDB(g, mgr.GetTiKV())
		})
	}
	if err != nil {
		log.Warn("create session pool failed, we will send DDLs only by created sessions",
			zap.Error(err),
			zap.Int("sessionCount", len(dbPool)),
		)
	}
	tableStream := client.GoCreateTables(ctx, mgr.GetDomain(), tables, newTS, dbPool, errCh)
	if len(files) == 0 {
		log.Info("no files, empty databases and tables are restored")
		summary.SetSuccessStatus(true)
		// don't return immediately, wait all pipeline done.
	}

	tableFileMap := restore.MapTableToFiles(files)
	log.Debug("mapped table to files", zap.Any("result map", tableFileMap))

	rangeStream := restore.GoValidateFileRanges(ctx, tableStream, tableFileMap, errCh)

	rangeSize := restore.EstimateRangeSize(files)
	summary.CollectInt("restore ranges", rangeSize)
	log.Info("range and file prepared", zap.Int("file count", len(files)), zap.Int("range count", rangeSize))

	restoreSchedulers, err := restorePreWork(ctx, client, mgr)
	if err != nil {
		return err
	}
	// Always run the post-work even on error, so we don't stuck in the import
	// mode or emptied schedulers
	defer restorePostWork(ctx, client, restoreSchedulers)

	// Do not reset timestamp if we are doing incremental restore, because
	// we are not allowed to decrease timestamp.
	if !client.IsIncremental() {
		if err = client.ResetTS(ctx, cfg.PD); err != nil {
			log.Error("reset pd TS failed", zap.Error(err))
			return err
		}
	}

	// Restore sst files in batch.
	batchSize := utils.ClampInt(int(cfg.Concurrency), defaultRestoreConcurrency, maxRestoreBatchSizeLimit)
	failpoint.Inject("small-batch-size", func(v failpoint.Value) {
		log.Info("failpoint small batch size is on", zap.Int("size", v.(int)))
		batchSize = v.(int)
	})

	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := g.StartProgress(
		ctx,
		cmdName,
		// Split/Scatter + Download/Ingest + Checksum
		int64(rangeSize+len(files)+len(tables)),
		!cfg.LogProgress)
	defer updateCh.Close()
	sender, err := restore.NewTiKVSender(ctx, client, updateCh)
	if err != nil {
		return err
	}
	manager := restore.NewBRContextManager(client)
	batcher, afterRestoreStream := restore.NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(batchSize)
	batcher.EnableAutoCommit(ctx, time.Second)
	go restoreTableStream(ctx, rangeStream, batcher, errCh)

	var finish <-chan struct{}
	// Checksum
	if cfg.Checksum {
		finish = client.GoValidateChecksum(
			ctx, afterRestoreStream, mgr.GetTiKV().GetClient(), errCh, updateCh)
	} else {
		// when user skip checksum, just collect tables, and drop them.
		finish = dropToBlackhole(ctx, afterRestoreStream, errCh, updateCh)
	}

	select {
	case err = <-errCh:
		err = multierr.Append(err, multierr.Combine(restore.Exhaust(errCh)...))
	case <-finish:
	}

	// If any error happened, return now.
	if err != nil {
		return err
	}

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}

// dropToBlackhole drop all incoming tables into black hole,
// i.e. don't execute checksum, just increase the process anyhow.
func dropToBlackhole(
	ctx context.Context,
	tableStream <-chan restore.CreatedTable,
	errCh chan<- error,
	updateCh glue.Progress,
) <-chan struct{} {
	outCh := make(chan struct{}, 1)
	go func() {
		defer func() {
			outCh <- struct{}{}
		}()
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case _, ok := <-tableStream:
				if !ok {
					return
				}
				updateCh.Inc()
			}
		}
	}()
	return outCh
}

func filterRestoreFiles(
	client *restore.Client,
	cfg *RestoreConfig,
) (files []*backup.File, tables []*utils.Table, dbs []*utils.Database) {
	for _, db := range client.GetDatabases() {
		createdDatabase := false
		for _, table := range db.Tables {
			if !cfg.TableFilter.MatchTable(db.Info.Name.O, table.Info.Name.O) {
				continue
			}

			if !createdDatabase {
				dbs = append(dbs, db)
				createdDatabase = true
			}
			files = append(files, table.Files...)
			tables = append(tables, table)
		}
	}
	return
}

// restorePreWork executes some prepare work before restore.
// TODO make this function returns a restore post work.
func restorePreWork(ctx context.Context, client *restore.Client, mgr *conn.Mgr) (utils.UndoFunc, error) {
	if client.IsOnline() {
		return utils.Nop, nil
	}

	// Switch TiKV cluster to import mode (adjust rocksdb configuration).
	client.SwitchToImportMode(ctx)

	return mgr.RemoveSchedulers(ctx)
}

// restorePostWork executes some post work after restore.
// TODO: aggregate all lifetime manage methods into batcher's context manager field.
func restorePostWork(
	ctx context.Context, client *restore.Client, restoreSchedulers utils.UndoFunc,
) {
	if client.IsOnline() {
		return
	}
	if err := client.SwitchToNormalMode(ctx); err != nil {
		log.Warn("fail to switch to normal mode", zap.Error(err))
	}
	if err := restoreSchedulers(ctx); err != nil {
		log.Warn("failed to restore PD schedulers", zap.Error(err))
	}
}

// RunRestoreTiflashReplica restores the replica of tiflash saved in the last restore.
func RunRestoreTiflashReplica(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	mgr, err := newMgr(ctx, g, cfg.PD, cfg.TLS, cfg.CheckRequirements)
	if err != nil {
		return err
	}
	defer mgr.Close()

	// Load saved backupmeta
	_, _, backupMeta, err := ReadBackupMeta(ctx, utils.SavedMetaFile, &cfg.Config)
	if err != nil {
		return err
	}
	dbs, err := utils.LoadBackupTables(backupMeta)
	if err != nil {
		return err
	}
	se, err := restore.NewDB(g, mgr.GetTiKV())
	if err != nil {
		return err
	}

	tables := make([]*utils.Table, 0)
	for _, db := range dbs {
		tables = append(tables, db.Tables...)
	}
	updateCh := g.StartProgress(
		ctx, "RecoverTiflashReplica", int64(len(tables)), !cfg.LogProgress)
	for _, t := range tables {
		log.Info("get table", zap.Stringer("name", t.Info.Name),
			zap.Int("replica", t.TiFlashReplicas))
		if t.TiFlashReplicas > 0 {
			err := se.AlterTiflashReplica(ctx, t, t.TiFlashReplicas)
			if err != nil {
				return err
			}
			updateCh.Inc()
		}
	}
	updateCh.Close()
	summary.CollectInt("recover tables", len(tables))

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}

func enableTiDBConfig() {
	// set max-index-length before execute DDLs and create tables
	// we set this value to max(3072*4), otherwise we might not restore table
	// when upstream and downstream both set this value greater than default(3072)
	conf := config.GetGlobalConfig()
	conf.MaxIndexLength = config.DefMaxOfMaxIndexLength
	log.Warn("set max-index-length to max(3072*4) to skip check index length in DDL")

	// we need set this to true, since all create table DDLs will create with tableInfo
	// and we can handle alter drop pk/add pk DDLs with no impact
	conf.AlterPrimaryKey = true

	conf.Experimental.AllowsExpressionIndex = true

	config.StoreGlobalConfig(conf)
}

// restoreTableStream blocks current goroutine and restore a stream of tables,
// by send tables to batcher.
func restoreTableStream(
	ctx context.Context,
	inputCh <-chan restore.TableWithRange,
	batcher *restore.Batcher,
	errCh chan<- error,
) {
	// We cache old tables so that we can 'batch' recover TiFlash and tables.
	oldTables := []*utils.Table{}
	defer func() {
		// when things done, we must clean pending requests.
		batcher.Close()
		log.Info("doing postwork",
			zap.Int("table count", len(oldTables)),
		)
	}()

	for {
		select {
		case <-ctx.Done():
			errCh <- ctx.Err()
			return
		case t, ok := <-inputCh:
			if !ok {
				return
			}
			oldTables = append(oldTables, t.OldTable)

			batcher.Add(t)
		}
	}
}
