// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/br/pkg/utils"

	"github.com/pingcap/errors"
	kvproto "github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
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
)

const (
	flagBackupTimeago   = "timeago"
	flagBackupTS        = "backupts"
	flagLastBackupTS    = "lastbackupts"
	flagCompressionType = "compression-type"

	flagGCTTL = "gcttl"

	defaultBackupConcurrency = 4
	maxBackupConcurrency     = 256
)

// BackupConfig is the configuration specific for backup tasks.
type BackupConfig struct {
	Config

	TimeAgo         time.Duration           `json:"time-ago" toml:"time-ago"`
	BackupTS        uint64                  `json:"backup-ts" toml:"backup-ts"`
	LastBackupTS    uint64                  `json:"last-backup-ts" toml:"last-backup-ts"`
	GCTTL           int64                   `json:"gc-ttl" toml:"gc-ttl"`
	CompressionType kvproto.CompressionType `json:"compression-type" toml:"compression-type"`
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
	flags.Int64(flagGCTTL, backup.DefaultBRGCSafePointTTL, "the TTL (in seconds) that PD holds for BR's GC safepoint")
	flags.String(flagCompressionType, "", "backup sst file compression algorithm, value can be one of 'lz4|zstd|snappy'")
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
	gcTTL, err := flags.GetInt64(flagGCTTL)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.GCTTL = gcTTL

	compressionStr, err := flags.GetString(flagCompressionType)
	if err != nil {
		return errors.Trace(err)
	}
	compressionType, err := parseCompressionType(compressionStr)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionType = compressionType

	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultBackupConcurrency
	}
	if cfg.Config.Concurrency > maxBackupConcurrency {
		cfg.Config.Concurrency = maxBackupConcurrency
	}
	return nil
}

// RunBackup starts a backup task inside the current goroutine.
func RunBackup(c context.Context, g glue.Glue, cmdName string, cfg *BackupConfig) error {
	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return err
	}
	mgr, err := newMgr(ctx, g, cfg.PD, cfg.TLS, conn.SkipTiFlash, cfg.CheckRequirements)
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
	err = client.SetLockFile(ctx)
	if err != nil {
		return err
	}
	client.SetGCTTL(cfg.GCTTL)

	backupTS, err := client.GetTS(ctx, cfg.TimeAgo, cfg.BackupTS)
	if err != nil {
		return err
	}
	g.Record("BackupTS", backupTS)

	isIncrementalBackup := cfg.LastBackupTS > 0

	req := kvproto.BackupRequest{
		StartVersion:    cfg.LastBackupTS,
		EndVersion:      backupTS,
		RateLimit:       cfg.RateLimit,
		Concurrency:     defaultBackupConcurrency,
		CompressionType: cfg.CompressionType,
	}

	ranges, backupSchemas, err := backup.BuildBackupRangeAndSchema(
		mgr.GetDomain(), mgr.GetTiKV(), cfg.TableFilter, backupTS)
	if err != nil {
		return err
	}
	// nothing to backup
	if ranges == nil {
		backupMeta, err2 := backup.BuildBackupMeta(&req, nil, nil, nil)
		if err2 != nil {
			return err2
		}
		return client.SaveBackupMeta(ctx, &backupMeta)
	}

	ddlJobs := make([]*model.Job, 0)
	if isIncrementalBackup {
		if backupTS <= cfg.LastBackupTS {
			log.Error("LastBackupTS is larger or equal to current TS")
			return errors.New("LastBackupTS is larger or equal to current TS")
		}
		err = backup.CheckGCSafePoint(ctx, mgr.GetPDClient(), cfg.LastBackupTS)
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
	updateCh := g.StartProgress(
		ctx, cmdName, int64(approximateRegions), !cfg.LogProgress)

	files, err := client.BackupRanges(ctx, ranges, req, uint(cfg.Concurrency), updateCh)
	if err != nil {
		return err
	}
	// Backup has finished
	updateCh.Close()

	backupMeta, err := backup.BuildBackupMeta(&req, files, nil, ddlJobs)
	if err != nil {
		return err
	}

	// Checksum from server, and then fulfill the backup metadata.
	if cfg.Checksum && !isIncrementalBackup {
		backupSchemasConcurrency := utils.MinInt(backup.DefaultSchemaConcurrency, backupSchemas.Len())
		updateCh = g.StartProgress(
			ctx, "Checksum", int64(backupSchemas.Len()), !cfg.LogProgress)
		backupSchemas.Start(
			ctx, mgr.GetTiKV(), backupTS, uint(backupSchemasConcurrency), updateCh)
		backupMeta.Schemas, err = backupSchemas.FinishTableChecksum()
		if err != nil {
			return err
		}
		// Checksum has finished
		updateCh.Close()
		// collect file information.
		err = checkChecksums(&backupMeta)
		if err != nil {
			return err
		}
	} else {
		// Just... copy schemas from origin.
		backupMeta.Schemas = backupSchemas.CopyMeta()
		if isIncrementalBackup {
			// Since we don't support checksum for incremental data, fast checksum should be skipped.
			log.Info("Skip fast checksum in incremental backup")
			err = backup.FilterSchema(&backupMeta)
			if err != nil {
				return err
			}
		} else {
			// When user specified not to calculate checksum, don't calculate checksum.
			log.Info("Skip fast checksum because user requirement.")
		}
	}

	err = client.SaveBackupMeta(ctx, &backupMeta)
	if err != nil {
		return err
	}

	g.Record("Size", utils.ArchiveSize(&backupMeta))

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}

// checkChecksums checks the checksum of the client, once failed,
// returning a error with message: "mismatched checksum".
func checkChecksums(backupMeta *kvproto.BackupMeta) error {
	checksums, err := backup.CollectChecksums(backupMeta)
	if err != nil {
		return err
	}
	var matches bool
	matches, err = backup.ChecksumMatches(backupMeta, checksums)
	if err != nil {
		return err
	}
	if !matches {
		log.Error("backup FastChecksum mismatch!")
		return errors.New("mismatched checksum")
	}
	return nil
}

// parseTSString port from tidb setSnapshotTS.
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

func parseCompressionType(s string) (kvproto.CompressionType, error) {
	var ct kvproto.CompressionType
	switch s {
	case "lz4":
		ct = kvproto.CompressionType_LZ4
	case "snappy":
		ct = kvproto.CompressionType_SNAPPY
	case "zstd":
		ct = kvproto.CompressionType_ZSTD
	case "":
		ct = kvproto.CompressionType_UNKNOWN
	default:
		return kvproto.CompressionType_UNKNOWN, errors.Errorf("invalid compression type '%s'", s)
	}
	return ct, nil
}
