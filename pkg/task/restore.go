// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/pingcap/tidb-tools/pkg/filter"
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

	mgr, err := newMgr(ctx, g, cfg.PD, cfg.TLS, conn.SkipTiFlash)
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

	files, tables, dbs, err := filterRestoreFiles(client, cfg)
	if err != nil {
		return err
	}

	var newTS uint64
	if client.IsIncremental() {
		newTS, err = client.GetTS(ctx)
		if err != nil {
			return err
		}
	}
	ddlJobs := restore.FilterDDLJobs(client.GetDDLJobs(), tables)
	if err != nil {
		return err
	}

	// pre-set TiDB config for restore
	enableTiDBConfig()

	// execute DDL first
	err = client.ExecDDLs(ddlJobs)
	if err != nil {
		return errors.Trace(err)
	}

	// nothing to restore, maybe only ddl changes in incremental restore
	if len(files) == 0 {
		log.Info("all files are filtered out from the backup archive, nothing to restore")
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
	tableStream := client.GoCreateTables(ctx, mgr.GetDomain(), tables, newTS, errCh)
	placementRules, err := client.GetPlacementRules(cfg.PD)
	if err != nil {
		return err
	}

	tableFileMap := restore.MapTableToFiles(files)
	log.Debug("mapped table to files", zap.Any("result map", tableFileMap))

	rangeStream := restore.GoValidateFileRanges(ctx, tableStream, tableFileMap, errCh)

	rangeSize := restore.EstimateRangeSize(files)
	summary.CollectInt("restore ranges", rangeSize)
	log.Info("range and file prepared", zap.Int("file count", len(files)), zap.Int("range count", rangeSize))

	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := g.StartProgress(
		ctx,
		cmdName,
		// Split/Scatter + Download/Ingest
		int64(restore.EstimateRangeSize(files)+len(files)),
		!cfg.LogProgress)

	clusterCfg, err := restorePreWork(ctx, client, mgr)
	if err != nil {
		return err
	}
	// Always run the post-work even on error, so we don't stuck in the import
	// mode or emptied schedulers
	shouldRestorePostWork := true
	restorePostWork := func() {
		if shouldRestorePostWork {
			shouldRestorePostWork = false
			restorePostWork(ctx, client, mgr, clusterCfg)
		}
	}
	defer restorePostWork()

	// Do not reset timestamp if we are doing incremental restore, because
	// we are not allowed to decrease timestamp.
	if !client.IsIncremental() {
		if err = client.ResetTS(cfg.PD); err != nil {
			log.Error("reset pd TS failed", zap.Error(err))
			return err
		}
	}

	// Restore sst files in batch.
	batchSize := utils.MinInt(int(cfg.Concurrency), maxRestoreBatchSizeLimit)

	sender, err := restore.NewTiKVSender(ctx, client, updateCh)
	if err != nil {
		return err
	}
	batcher, afterRestoreStream := restore.NewBatcher(sender, errCh)
	batcher.SetThreshold(batchSize)
	batcher.EnableAutoCommit(ctx, time.Second)
	goRestore(ctx, rangeStream, placementRules, client, batcher, errCh)

	var finish <-chan struct{}
	// Checksum
	if cfg.Checksum {
		updateCh = g.StartProgress(
			ctx, "Checksum", int64(len(tables)), true)
		defer updateCh.Close()
		finish = client.GoValidateChecksum(
			ctx, afterRestoreStream, mgr.GetTiKV().GetClient(), errCh, updateCh)
	} else {
		// when user skip checksum, just collect tables, and drop them.
		finish = dropToBlockhole(ctx, afterRestoreStream, errCh)
	}

	select {
	case err = <-errCh:
		err = multierr.Append(err, multierr.Combine(restore.Exhaust(errCh)...))
	case <-finish:
		log.Info("all works end.")
	}

	// If any error happened, return now, don't execute checksum.
	if err != nil {
		return err
	}

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}

// dropToBlockhole drop all incoming tables into black hole.
func dropToBlackhole(
	ctx context.Context,
	tableStream <-chan restore.CreatedTable,
	errCh chan<- error,
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
			case tbl, ok := <-tableStream:
				if !ok {
					return
				}
				log.Info("skipping checksum of table because user config",
					zap.Stringer("database", tbl.OldTable.Db.Name),
					zap.Stringer("table", tbl.Table.Name),
				)
			}
		}
	}()
	return outCh
}

func filterRestoreFiles(
	client *restore.Client,
	cfg *RestoreConfig,
) (files []*backup.File, tables []*utils.Table, dbs []*utils.Database, err error) {
	tableFilter, err := filter.New(cfg.CaseSensitive, &cfg.Filter)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, db := range client.GetDatabases() {
		createdDatabase := false
		for _, table := range db.Tables {
			if !tableFilter.Match(&filter.Table{Schema: db.Info.Name.O, Name: table.Info.Name.O}) {
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
	if err := client.SwitchToImportMode(ctx); err != nil {
		return clusterConfig{}, nil
	}

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

func splitPrepareWork(ctx context.Context, client *restore.Client, tables []*model.TableInfo) error {
	err := client.SetupPlacementRules(ctx, tables)
	if err != nil {
		log.Error("setup placement rules failed", zap.Error(err))
		return errors.Trace(err)
	}

	err = client.WaitPlacementSchedule(ctx, tables)
	if err != nil {
		log.Error("wait placement schedule failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func splitPostWork(ctx context.Context, client *restore.Client, tables []*model.TableInfo) {
	err := client.ResetPlacementRules(ctx, tables)
	if err != nil {
		log.Warn("reset placement rules failed", zap.Error(err))
		return
	}

	err = client.ResetRestoreLabels(ctx)
	if err != nil {
		log.Warn("reset store labels failed", zap.Error(err))
	}
}

// RunRestoreTiflashReplica restores the replica of tiflash saved in the last restore.
func RunRestoreTiflashReplica(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	mgr, err := newMgr(ctx, g, cfg.PD, cfg.TLS, conn.SkipTiFlash)
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

	// set this to true for some auto random DDL execute normally during incremental restore
	conf.Experimental.AllowAutoRandom = true
	conf.Experimental.AllowsExpressionIndex = true

	config.StoreGlobalConfig(conf)
}

// goRestore forks a goroutine to do the restore process.
func goRestore(
	ctx context.Context,
	inputCh <-chan restore.TableWithRange,
	rules []placement.Rule,
	client *restore.Client,
	batcher *restore.Batcher,
	errCh chan<- error,
) {
	go func() {
		// We cache old tables so that we can 'batch' recover TiFlash and tables.
		oldTables := []*utils.Table{}
		newTables := []*model.TableInfo{}
		defer func() {
			// when things done, we must clean pending requests.
			batcher.Close(ctx)
			log.Info("doing postwork",
				zap.Int("new tables", len(newTables)),
				zap.Int("old tables", len(oldTables)),
			)
			splitPostWork(ctx, client, newTables)
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
				// Omit the number of TiFlash have been removed.
				if _, err := client.RemoveTiFlashOfTable(t.CreatedTable, rules); err != nil {
					log.Error("failed on remove TiFlash replicas", zap.Error(err))
					errCh <- err
					return
				}
				oldTables = append(oldTables, t.OldTable)

				// Reusage of splitPrepareWork would be safe.
				// But this operation sometime would be costly.
				if err := splitPrepareWork(ctx, client, []*model.TableInfo{t.Table}); err != nil {
					log.Error("failed on set online restore placement rules", zap.Error(err))
					errCh <- err
					return
				}
				newTables = append(newTables, t.Table)

				batcher.Add(ctx, t)
			}
		}
	}()
}
