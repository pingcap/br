// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"

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
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

const (
	flagOnline = "online"
)

var schedulers = map[string]struct{}{
	"balance-leader-scheduler":     {},
	"balance-hot-region-scheduler": {},
	"balance-region-scheduler":     {},

	"shuffle-leader-scheduler":     {},
	"shuffle-region-scheduler":     {},
	"shuffle-hot-region-scheduler": {},
}

const (
	defaultRestoreConcurrency = 128
	maxRestoreBatchSizeLimit  = 256
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
func RunRestore(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) (err error) {
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	mgr, err := newMgr(ctx, g, cfg.PD, cfg.TLS, conn.ErrorOnTiFlash)
	if err != nil {
		return err
	}
	defer mgr.Close()

	client, err := restore.NewRestoreClient(ctx, g, mgr.GetPDClient(), mgr.GetTiKV(), mgr.GetTLSConfig())
	if err != nil {
		return err
	}
	defer client.Close()

	client.SetRateLimit(cfg.RateLimit)
	client.SetConcurrency(uint(cfg.Concurrency))
	if cfg.Online {
		client.EnableOnline()
	}
	err = client.LoadRestoreStores(ctx)
	if err != nil {
		return err
	}

	defer summary.Summary(cmdName)

	u, _, backupMeta, err := ReadBackupMeta(ctx, &cfg.Config)
	if err != nil {
		return err
	}
	if err = client.InitBackupMeta(backupMeta, u); err != nil {
		return err
	}

	files, tables, err := filterRestoreFiles(client, cfg)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return errors.New("all files are filtered out from the backup archive, nothing to restore")
	}
	summary.CollectInt("restore files", len(files))

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
	updateCh := utils.StartProgress(
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
			break
		}
	}

	// Always run the post-work even on error, so we don't stuck in the import
	// mode or emptied schedulers
	err = restorePostWork(ctx, client, mgr, clusterCfg)
	if err != nil {
		return err
	}

	if err = splitPostWork(ctx, client, newTables); err != nil {
		return err
	}

	// Restore has finished.
	close(updateCh)

	// Checksum
	updateCh = utils.StartProgress(
		ctx, "Checksum", int64(len(newTables)), !cfg.LogProgress)
	err = client.ValidateChecksum(
		ctx, mgr.GetTiKV().GetClient(), tables, newTables, updateCh)
	if err != nil {
		return err
	}
	close(updateCh)

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

// restorePreWork executes some prepare work before restore
func restorePreWork(ctx context.Context, client *restore.Client, mgr *conn.Mgr) ([]string, error) {
	if client.IsOnline() {
		return nil, nil
	}

	if err := client.SwitchToImportMode(ctx); err != nil {
		return nil, err
	}

	existSchedulers, err := mgr.ListSchedulers(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	needRemoveSchedulers := make([]string, 0, len(existSchedulers))
	for _, s := range existSchedulers {
		if _, ok := schedulers[s]; ok {
			needRemoveSchedulers = append(needRemoveSchedulers, s)
		}
	}
	return removePDLeaderScheduler(ctx, mgr, needRemoveSchedulers)
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
func restorePostWork(ctx context.Context, client *restore.Client, mgr *conn.Mgr, removedSchedulers []string) error {
	if client.IsOnline() {
		return nil
	}
	if err := client.SwitchToNormalMode(ctx); err != nil {
		return err
	}
	return addPDLeaderScheduler(ctx, mgr, removedSchedulers)
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

func splitPostWork(ctx context.Context, client *restore.Client, tables []*model.TableInfo) error {
	err := client.ResetPlacementRules(ctx, tables)
	if err != nil {
		return errors.Trace(err)
	}

	err = client.ResetRestoreLabels(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
