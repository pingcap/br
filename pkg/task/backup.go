// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	kvproto "github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/backup"
	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

const (
	flagBackupTimeago = "timeago"
	flagBackupTS      = "backupts"
	flagLastBackupTS  = "lastbackupts"

	defaultBackupConcurrency = 4
)

// BackupConfig is the configuration specific for backup tasks.
type BackupConfig struct {
	Config

	TimeAgo      time.Duration `json:"time-ago" toml:"time-ago"`
	BackupTS     uint64        `json:"backup-ts" toml:"backup-ts"`
	LastBackupTS uint64        `json:"last-backup-ts" toml:"last-backup-ts"`
}

// DefineBackupFlags defines common flags for the backup command.
func DefineBackupFlags(flags *pflag.FlagSet) {
	flags.Duration(
		flagBackupTimeago, 0,
		"The history version of the backup task, e.g. 1m, 1h. Do not exceed GCSafePoint")

	// TODO: remove experimental tag if it's stable
	flags.Uint64(flagLastBackupTS, 0, "(experimental) the last time backup ts,"+
		" use for incremental backup, support TSO only")
	flags.String(flagBackupTS, "", "the backup ts support TSO or datetime,"+
		" e.g. '400036290571534337', '2018-05-11 01:42:23'")
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
	backupTS, err := flags.GetString(flagBackupTS)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.BackupTS, err = parseTSString(backupTS)
	if err != nil {
		return errors.Trace(err)
	}

	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultBackupConcurrency
	}
	return nil
}

// RunBackup starts a backup task inside the current goroutine.
func RunBackup(c context.Context, g glue.Glue, cmdName string, cfg *BackupConfig) error {
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
	mgr, err := newMgr(ctx, g, cfg.PD, cfg.TLS, conn.SkipTiFlash)
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

	backupTS, err := client.GetTS(ctx, cfg.TimeAgo, cfg.BackupTS)
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
		if backupTS < cfg.LastBackupTS {
			log.Error("LastBackupTS is larger than current TS")
			return errors.New("LastBackupTS is larger than current TS")
		}
		err = backup.CheckGCSafepoint(ctx, mgr.GetPDClient(), cfg.LastBackupTS)
		if err != nil {
			log.Error("Check gc safepoint for last backup ts failed", zap.Error(err))
			return err
		}
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

	req := kvproto.BackupRequest{
		StartVersion: cfg.LastBackupTS,
		EndVersion:   backupTS,
		RateLimit:    cfg.RateLimit,
		Concurrency:  cfg.Concurrency,
	}
	err = client.BackupRanges(
		ctx, ranges, req, updateCh)
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
		// Since we don't support checksum for incremental data, fast checksum should be skipped.
		log.Info("Skip fast checksum in incremental backup")
	}
	// Checksum has finished
	close(updateCh)

	err = client.SaveBackupMeta(ctx, ddlJobs)
	if err != nil {
		log.Error("save backup meta failed", zap.Reflect("meta", ddlJobs))
		return err
	}
	return nil
}

// parseTSString port from tidb setSnapshotTS
func parseTSString(ts string) (uint64, error) {
	if len(ts) == 0 {
		return 0, nil
	}
	if tso, err := strconv.ParseUint(ts, 10, 64); err == nil {
		return tso, nil
	}

	loc := time.Local
	sc := &stmtctx.StatementContext{
		TimeZone: loc,
	}
	t, err := types.ParseTime(sc, ts, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return 0, errors.Trace(err)
	}
	t1, err := t.GoTime(loc)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return variable.GoTimeToTS(t1), nil
}
