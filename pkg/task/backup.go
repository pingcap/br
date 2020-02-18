package task

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/spf13/pflag"

	"github.com/pingcap/br/pkg/backup"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

const (
	flagBackupTimeago = "timeago"
	flagLastBackupTS  = "lastbackupts"
)

// BackupConfig is the configuration specific for backup tasks.
type BackupConfig struct {
	Config

	TimeAgo      time.Duration `json:"time-ago" toml:"time-ago"`
	LastBackupTS uint64        `json:"last-backup-ts" toml:"last-backup-ts"`
}

// DefineBackupFlags defines common flags for the backup command.
func DefineBackupFlags(flags *pflag.FlagSet) {
	flags.Duration(
		flagBackupTimeago, 0,
		"The history version of the backup task, e.g. 1m, 1h. Do not exceed GCSafePoint")

	flags.Uint64(flagLastBackupTS, 0, "the last time backup ts")
	_ = flags.MarkHidden(flagLastBackupTS)
}

// ParseFromFlags parses the backup-related flags from the flag set.
func (cfg *BackupConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	timeAgo, err := flags.GetDuration(flagBackupTimeago)
	if err != nil {
		return errors.Trace(err)
	}
	if timeAgo < 0 {
		return errors.New("negative timeago is not allowed")
	}
	cfg.TimeAgo = timeAgo
	cfg.LastBackupTS, err = flags.GetUint64(flagLastBackupTS)
	if err != nil {
		return errors.Trace(err)
	}
	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunBackup starts a backup task inside the current goroutine.
func RunBackup(c context.Context, cmdName string, cfg *BackupConfig) error {
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return err
	}
	tableFilter, err := filter.New(cfg.CaseSensitive, &cfg.Filter)
	if err != nil {
		return err
	}
	mgr, err := newMgr(ctx, cfg.PD)
	if err != nil {
		return err
	}
	defer mgr.Close()

	client, err := backup.NewBackupClient(ctx, mgr)
	if err != nil {
		return err
	}
	if err = client.SetStorage(ctx, u, cfg.SendCreds); err != nil {
		return err
	}

	backupTS, err := client.GetTS(ctx, cfg.TimeAgo)
	if err != nil {
		return err
	}

	defer summary.Summary(cmdName)

	ranges, backupSchemas, err := backup.BuildBackupRangeAndSchema(
		mgr.GetDomain(), mgr.GetTiKV(), tableFilter, backupTS)
	if err != nil {
		return err
	}

	ddlJobs := make([]*model.Job, 0)
	if cfg.LastBackupTS > 0 {
		ddlJobs, err = backup.GetBackupDDLJobs(mgr.GetDomain(), cfg.LastBackupTS, backupTS)
		if err != nil {
			return err
		}
	}

	// The number of regions need to backup
	approximateRegions := 0
	for _, r := range ranges {
		var regionCount int
		regionCount, err = mgr.GetRegionCount(ctx, r.StartKey, r.EndKey)
		if err != nil {
			return err
		}
		approximateRegions += regionCount
	}

	summary.CollectInt("backup total regions", approximateRegions)

	// Backup
	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := utils.StartProgress(
		ctx, cmdName, int64(approximateRegions), !cfg.LogProgress)
	err = client.BackupRanges(
		ctx, ranges, cfg.LastBackupTS, backupTS, cfg.RateLimit, cfg.Concurrency, updateCh)
	if err != nil {
		return err
	}
	// Backup has finished
	close(updateCh)

	// Checksum
	backupSchemasConcurrency := backup.DefaultSchemaConcurrency
	if backupSchemas.Len() < backupSchemasConcurrency {
		backupSchemasConcurrency = backupSchemas.Len()
	}
	updateCh = utils.StartProgress(
		ctx, "Checksum", int64(backupSchemas.Len()), !cfg.LogProgress)
	backupSchemas.SetSkipChecksum(!cfg.Checksum)
	backupSchemas.Start(
		ctx, mgr.GetTiKV(), backupTS, uint(backupSchemasConcurrency), updateCh)

	err = client.CompleteMeta(backupSchemas)
	if err != nil {
		return err
	}

	if cfg.LastBackupTS == 0 {
		var valid bool
		valid, err = client.FastChecksum()
		if err != nil {
			return err
		}
		if !valid {
			log.Error("backup FastChecksum mismatch!")
			return errors.Errorf("mismatched checksum")
		}

	} else {
		log.Warn("Skip fast checksum in incremental backup")
	}
	// Checksum has finished
	close(updateCh)

	err = client.SaveBackupMeta(ctx, ddlJobs)
	if err != nil {
		return err
	}
	return nil
}
