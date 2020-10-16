// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/errors"
	kvproto "github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/backup"
	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

const (
	flagKeyFormat        = "format"
	flagTiKVColumnFamily = "cf"
	flagStartKey         = "start"
	flagEndKey           = "end"
)

// RawKvConfig is the common config for rawkv backup and restore.
type RawKvConfig struct {
	Config

	StartKey []byte `json:"start-key" toml:"start-key"`
	EndKey   []byte `json:"end-key" toml:"end-key"`
	CF       string `json:"cf" toml:"cf"`
	CompressionConfig
	RemoveSchedulers bool `json:"remove-schedulers" toml:"remove-schedulers"`
}

// DefineRawBackupFlags defines common flags for the backup command.
func DefineRawBackupFlags(command *cobra.Command) {
	command.Flags().StringP(flagKeyFormat, "", "hex", "start/end key format, support raw|escaped|hex")
	command.Flags().StringP(flagTiKVColumnFamily, "", "default", "backup specify cf, correspond to tikv cf")
	command.Flags().StringP(flagStartKey, "", "", "backup raw kv start key, key is inclusive")
	command.Flags().StringP(flagEndKey, "", "", "backup raw kv end key, key is exclusive")
	command.Flags().String(flagCompressionType, "zstd",
		"backup sst file compression algorithm, value can be one of 'lz4|zstd|snappy'")
	command.Flags().Bool(flagRemoveSchedulers, false,
		"disable the balance, shuffle and region-merge schedulers in PD to speed up backup")
	// This flag can impact the online cluster, so hide it in case of abuse.
	_ = command.Flags().MarkHidden(flagRemoveSchedulers)
}

// ParseFromFlags parses the raw kv backup&restore common flags from the flag set.
func (cfg *RawKvConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	format, err := flags.GetString(flagKeyFormat)
	if err != nil {
		return err
	}
	start, err := flags.GetString(flagStartKey)
	if err != nil {
		return err
	}
	cfg.StartKey, err = utils.ParseKey(format, start)
	if err != nil {
		return err
	}
	end, err := flags.GetString(flagEndKey)
	if err != nil {
		return err
	}
	cfg.EndKey, err = utils.ParseKey(format, end)
	if err != nil {
		return err
	}

	if bytes.Compare(cfg.StartKey, cfg.EndKey) >= 0 {
		return errors.Annotate(berrors.ErrBackupInvalidRange, "endKey must be greater than startKey")
	}
	cfg.CF, err = flags.GetString(flagTiKVColumnFamily)
	if err != nil {
		return err
	}
	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ParseBackupConfigFromFlags parses the backup-related flags from the flag set.
func (cfg *RawKvConfig) ParseBackupConfigFromFlags(flags *pflag.FlagSet) error {
	err := cfg.ParseFromFlags(flags)
	if err != nil {
		return err
	}

	compressionCfg, err := parseCompressionFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionConfig = *compressionCfg

	cfg.RemoveSchedulers, err = flags.GetBool(flagRemoveSchedulers)
	if err != nil {
		return errors.Trace(err)
	}
	level, err := flags.GetInt32(flagCompressionLevel)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionLevel = level

	return nil
}

// RunBackupRaw starts a backup task inside the current goroutine.
func RunBackupRaw(c context.Context, g glue.Glue, cmdName string, cfg *RawKvConfig) error {
	cfg.adjust()

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return err
	}
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements)
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

	backupRange := rtree.Range{StartKey: cfg.StartKey, EndKey: cfg.EndKey}

	if cfg.RemoveSchedulers {
		restore, e := mgr.RemoveSchedulers(ctx)
		defer func() {
			if ctx.Err() != nil {
				var cancel context.CancelFunc
				log.Warn("context canceled, doing clean work with another context with timeout")
				ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
			}
			if restoreE := restore(ctx); restoreE != nil {
				log.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
			}
		}()
		if e != nil {
			return err
		}
	}

	// The number of regions need to backup
	approximateRegions, err := mgr.GetRegionCount(ctx, backupRange.StartKey, backupRange.EndKey)
	if err != nil {
		return err
	}

	summary.CollectInt("backup total regions", approximateRegions)

	// Backup
	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := g.StartProgress(
		ctx, cmdName, int64(approximateRegions), !cfg.LogProgress)

	req := kvproto.BackupRequest{
		StartVersion:     0,
		EndVersion:       0,
		RateLimit:        cfg.RateLimit,
		Concurrency:      cfg.Concurrency,
		IsRawKv:          true,
		Cf:               cfg.CF,
		CompressionType:  cfg.CompressionType,
		CompressionLevel: cfg.CompressionLevel,
	}
	files, err := client.BackupRange(ctx, backupRange.StartKey, backupRange.EndKey, req, updateCh)
	if err != nil {
		return err
	}
	// Backup has finished
	updateCh.Close()

	// Checksum
	rawRanges := []*kvproto.RawRange{{StartKey: backupRange.StartKey, EndKey: backupRange.EndKey, Cf: cfg.CF}}
	backupMeta, err := backup.BuildBackupMeta(&req, files, rawRanges, nil)
	if err != nil {
		return err
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
