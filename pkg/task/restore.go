// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"math"
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

var (
	schedulers = map[string]struct{}{
		"balance-leader-scheduler":     {},
		"balance-hot-region-scheduler": {},
		"balance-region-scheduler":     {},

		"shuffle-leader-scheduler":     {},
		"shuffle-region-scheduler":     {},
		"shuffle-hot-region-scheduler": {},
	}
	pdRegionMergeCfg = []string{
		"max-merge-region-keys",
		"max-merge-region-size",
	}
	pdScheduleLimitCfg = []string{
		"leader-schedule-limit",
		"region-schedule-limit",
		"max-snapshot-count",
	}
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
	cfg.RemoveTiFlash, err = flags.GetBool(flagRemoveTiFlash)
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

// RunRestore starts a restore task inside the current goroutine.
func RunRestore(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	mgr, err := newMgr(ctx, g, cfg.PD, cfg.TLS, conn.SkipTiFlash, cfg.CheckRequirements)
	if err != nil {
		return err
	}
	defer mgr.Close()

	client, err := restore.NewRestoreClient(ctx, g, mgr.GetPDClient(), mgr.GetTiKV(), mgr.GetTLSConfig())
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
		return errors.New("cannot do transactional restore from raw kv data")
	}

	files, tables, dbs := filterRestoreFiles(client, cfg)
	if len(dbs) == 0 && len(tables) != 0 {
		return errors.New("invalid backup, contain tables but no databases")
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
	err = client.ExecDDLs(ddlJobs)
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
		err = client.CreateDatabase(db.Info)
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
	dbPool, err := restore.MakeDBPool(defaultDDLConcurrency, func() (*restore.DB, error) {
		return restore.NewDB(g, mgr.GetTiKV())
	})
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

	clusterCfg, err := restorePreWork(ctx, client, mgr)
	if err != nil {
		return err
	}
	// Always run the post-work even on error, so we don't stuck in the import
	// mode or emptied schedulers
	defer restorePostWork(ctx, client, mgr, clusterCfg)

	// Do not reset timestamp if we are doing incremental restore, because
	// we are not allowed to decrease timestamp.
	if !client.IsIncremental() {
		if err = client.ResetTS(cfg.PD); err != nil {
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
	sender, err := restore.NewTiKVSender(ctx, client, updateCh, cfg.RemoveTiFlash)
	if err != nil {
		return err
	}
	manager := restore.NewBRContextManager(client)
	batcher, afterRestoreStream := restore.NewBatcher(ctx, sender, manager, errCh)
	batcher.SetThreshold(batchSize)
	batcher.EnableAutoCommit(ctx, time.Second)
	go restoreTableStream(ctx, rangeStream, cfg.RemoveTiFlash, cfg.PD, client, batcher, errCh)

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

type clusterConfig struct {
	// Enable PD schedulers before restore
	scheduler []string
	// Original scheudle configuration
	scheduleCfg map[string]interface{}
}

// restorePreWork executes some prepare work before restore.
func restorePreWork(ctx context.Context, client *restore.Client, mgr *conn.Mgr) (clusterConfig, error) {
	if client.IsOnline() {
		return clusterConfig{}, nil
	}

	// Switch TiKV cluster to import mode (adjust rocksdb configuration).
	client.SwitchToImportMode(ctx)

	// Remove default PD scheduler that may affect restore process.
	existSchedulers, err := mgr.ListSchedulers(ctx)
	if err != nil {
		return clusterConfig{}, nil
	}
	needRemoveSchedulers := make([]string, 0, len(existSchedulers))
	for _, s := range existSchedulers {
		if _, ok := schedulers[s]; ok {
			needRemoveSchedulers = append(needRemoveSchedulers, s)
		}
	}
	scheduler, err := removePDLeaderScheduler(ctx, mgr, needRemoveSchedulers)
	if err != nil {
		return clusterConfig{}, nil
	}

	stores, err := mgr.GetPDClient().GetAllStores(ctx)
	if err != nil {
		return clusterConfig{}, err
	}

	scheduleCfg, err := mgr.GetPDScheduleConfig(ctx)
	if err != nil {
		return clusterConfig{}, err
	}

	disableMergeCfg := make(map[string]interface{})
	for _, cfgKey := range pdRegionMergeCfg {
		value := scheduleCfg[cfgKey]
		if value == nil {
			// Ignore non-exist config.
			continue
		}
		// Disable region merge by setting config to 0.
		disableMergeCfg[cfgKey] = 0
	}
	err = mgr.UpdatePDScheduleConfig(ctx, disableMergeCfg)
	if err != nil {
		return clusterConfig{}, err
	}

	scheduleLimitCfg := make(map[string]interface{})
	for _, cfgKey := range pdScheduleLimitCfg {
		value := scheduleCfg[cfgKey]
		if value == nil {
			// Ignore non-exist config.
			continue
		}

		// Speed update PD scheduler by enlarging scheduling limits.
		// Multiply limits by store count but no more than 40.
		// Larger limit may make cluster unstable.
		limit := int(value.(float64))
		scheduleLimitCfg[cfgKey] = math.Min(40, float64(limit*len(stores)))
	}
	err = mgr.UpdatePDScheduleConfig(ctx, scheduleLimitCfg)
	if err != nil {
		return clusterConfig{}, err
	}

	cluster := clusterConfig{
		scheduler:   scheduler,
		scheduleCfg: scheduleCfg,
	}
	return cluster, nil
}

func removePDLeaderScheduler(ctx context.Context, mgr *conn.Mgr, existSchedulers []string) ([]string, error) {
	removedSchedulers := make([]string, 0, len(existSchedulers))
	for _, scheduler := range existSchedulers {
		err := mgr.RemoveScheduler(ctx, scheduler)
		if err != nil {
			return nil, err
		}
		removedSchedulers = append(removedSchedulers, scheduler)
	}
	return removedSchedulers, nil
}

// restorePostWork executes some post work after restore.
// TODO: aggregate all lifetime manage methods into batcher's context manager field.
func restorePostWork(
	ctx context.Context, client *restore.Client, mgr *conn.Mgr, clusterCfg clusterConfig,
) {
	if client.IsOnline() {
		return
	}
	if err := client.SwitchToNormalMode(ctx); err != nil {
		log.Warn("fail to switch to normal mode")
	}
	if err := addPDLeaderScheduler(ctx, mgr, clusterCfg.scheduler); err != nil {
		log.Warn("fail to add PD schedulers")
	}
	mergeCfg := make(map[string]interface{})
	for _, cfgKey := range pdRegionMergeCfg {
		value := clusterCfg.scheduleCfg[cfgKey]
		if value == nil {
			// Ignore non-exist config.
			continue
		}
		mergeCfg[cfgKey] = value
	}
	if err := mgr.UpdatePDScheduleConfig(ctx, mergeCfg); err != nil {
		log.Warn("fail to update PD region merge config")
	}

	scheduleLimitCfg := make(map[string]interface{})
	for _, cfgKey := range pdScheduleLimitCfg {
		value := clusterCfg.scheduleCfg[cfgKey]
		if value == nil {
			// Ignore non-exist config.
			continue
		}
		scheduleLimitCfg[cfgKey] = value
	}
	if err := mgr.UpdatePDScheduleConfig(ctx, scheduleLimitCfg); err != nil {
		log.Warn("fail to update PD schedule config")
	}
	if err := client.ResetRestoreLabels(ctx); err != nil {
		log.Warn("reset store labels failed", zap.Error(err))
	}
}

func addPDLeaderScheduler(ctx context.Context, mgr *conn.Mgr, removedSchedulers []string) error {
	for _, scheduler := range removedSchedulers {
		err := mgr.AddScheduler(ctx, scheduler)
		if err != nil {
			return err
		}
	}
	return nil
}

// RunRestoreTiflashReplica restores the replica of tiflash saved in the last restore.
func RunRestoreTiflashReplica(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	mgr, err := newMgr(ctx, g, cfg.PD, cfg.TLS, conn.SkipTiFlash, cfg.CheckRequirements)
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
	// TODO: remove this field and rules field after we support TiFlash
	removeTiFlashReplica bool,
	pdAddr []string,
	client *restore.Client,
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
		if err := client.RecoverTiFlashReplica(oldTables); err != nil {
			log.Error("failed on recover TiFlash replicas", zap.Error(err))
			errCh <- err
		}
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
			if removeTiFlashReplica {
				rules, err := client.GetPlacementRules(pdAddr)
				if err != nil {
					errCh <- err
					return
				}
				log.Debug("get rules", zap.Any("rules", rules), zap.Strings("pd", pdAddr))
				log.Debug("try to remove tiflash of table", zap.Stringer("table name", t.Table.Name))
				tiFlashRep, err := client.RemoveTiFlashOfTable(t.CreatedTable, rules)
				if err != nil {
					log.Error("failed on remove TiFlash replicas", zap.Error(err))
					errCh <- err
					return
				}
				t.OldTable.TiFlashReplicas = tiFlashRep
			}
			oldTables = append(oldTables, t.OldTable)

			batcher.Add(t)
		}
	}
}
