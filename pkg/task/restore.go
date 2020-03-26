// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

const (
	flagOnline = "online"

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

	Online bool `json:"online" toml:"online"`
}

// DefineRestoreFlags defines common flags for the restore command.
func DefineRestoreFlags(flags *pflag.FlagSet) {
	// TODO remove experimental tag if it's stable
	flags.Bool("online", false, "(experimental) Whether online when restore")
}

// ParseFromFlags parses the restore-related flags from the flag set.
func (cfg *RestoreConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.Online, err = flags.GetBool(flagOnline)
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
	err = client.LoadRestoreStores(ctx)
	if err != nil {
		return err
	}

	u, _, backupMeta, err := ReadBackupMeta(ctx, utils.MetaFile, &cfg.Config)
	if err != nil {
		return err
	}
	if err = client.InitBackupMeta(backupMeta, u); err != nil {
		return err
	}

	if client.IsRawKvMode() {
		return errors.New("cannot do transactional restore from raw kv data")
	}

	files, tables, err := filterRestoreFiles(client, cfg)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return errors.New("all files are filtered out from the backup archive, nothing to restore")
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
	err = client.ExecDDLs(ddlJobs)
	if err != nil {
		return errors.Trace(err)
	}
	rewriteRules, newTables, err := client.CreateTables(mgr.GetDomain(), tables, newTS)
	if err != nil {
		return err
	}
	placementRules, err := client.GetPlacementRules(cfg.PD)
	if err != nil {
		return err
	}
	err = client.RemoveTiFlashReplica(tables, placementRules)
	if err != nil {
		return err
	}

	defer func() {
		_ = client.RecoverTiFlashReplica(tables)
	}()

	ranges, err := restore.ValidateFileRanges(files, rewriteRules)
	if err != nil {
		return err
	}
	summary.CollectInt("restore ranges", len(ranges))

	if err = splitPrepareWork(ctx, client, newTables); err != nil {
		return err
	}

	ranges = restore.AttachFilesToRanges(files, ranges)

	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := g.StartProgress(
		ctx,
		cmdName,
		// Split/Scatter + Download/Ingest
		int64(len(ranges)+len(files)),
		!cfg.LogProgress)

	clusterCfg, err := restorePreWork(ctx, client, mgr)
	if err != nil {
		return err
	}

	// Do not reset timestamp if we are doing incremental restore, because
	// we are not allowed to decrease timestamp.
	if !client.IsIncremental() {
		if err = client.ResetTS(cfg.PD); err != nil {
			log.Error("reset pd TS failed", zap.Error(err))
			return err
		}
	}

	// Restore sst files in batch.
	batchSize := int(cfg.Concurrency)
	if batchSize > maxRestoreBatchSizeLimit {
		batchSize = maxRestoreBatchSizeLimit // 256
	}
	for {
		if len(ranges) == 0 {
			break
		}
		if batchSize > len(ranges) {
			batchSize = len(ranges)
		}
		var rangeBatch []rtree.Range
		ranges, rangeBatch = ranges[batchSize:], ranges[0:batchSize:batchSize]

		// Split regions by the given rangeBatch.
		err = restore.SplitRanges(ctx, client, rangeBatch, rewriteRules, updateCh)
		if err != nil {
			log.Error("split regions failed", zap.Error(err))
			// If any error happened, return now, don't execute checksum.
			return err
		}

		// Collect related files in the given rangeBatch.
		fileBatch := make([]*backup.File, 0, 2*len(rangeBatch))
		for _, rg := range rangeBatch {
			fileBatch = append(fileBatch, rg.Files...)
		}

		// After split, we can restore backup files.
		err = client.RestoreFiles(fileBatch, rewriteRules, updateCh)
		if err != nil {
			// If any error happened, return now, don't execute checksum.
			return err
		}
	}
	// Restore files success.

	// Always run the post-work even on error, so we don't stuck in the import
	// mode or emptied schedulers
	restorePostWork(ctx, client, mgr, clusterCfg)
	splitPostWork(ctx, client, newTables)

	// Restore has finished.
	updateCh.Close()

	// Checksum
	updateCh = g.StartProgress(
		ctx, "Checksum", int64(len(newTables)), !cfg.LogProgress)
	err = client.ValidateChecksum(
		ctx, mgr.GetTiKV().GetClient(), tables, newTables, updateCh)
	if err != nil {
		return err
	}
	updateCh.Close()

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}

func filterRestoreFiles(
	client *restore.Client,
	cfg *RestoreConfig,
) (files []*backup.File, tables []*utils.Table, err error) {
	tableFilter, err := filter.New(cfg.CaseSensitive, &cfg.Filter)
	if err != nil {
		return nil, nil, err
	}

	for _, db := range client.GetDatabases() {
		createdDatabase := false
		for _, table := range db.Tables {
			if !tableFilter.Match(&filter.Table{Schema: db.Info.Name.O, Name: table.Info.Name.O}) {
				continue
			}

			if !createdDatabase {
				if err = client.CreateDatabase(db.Info); err != nil {
					return nil, nil, err
				}
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

// restorePreWork executes some prepare work before restore
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
		scheduleLimitCfg[cfgKey] = math.Max(40, float64(limit*len(stores)))
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

// restorePostWork executes some post work after restore
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
