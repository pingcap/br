// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"bytes"
	"context"

	"github.com/pingcap/errors"
	kvproto "github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/pingcap/br/pkg/backup"
	"github.com/pingcap/br/pkg/conn"
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
}

// DefineRawBackupFlags defines common flags for the backup command.
func DefineRawBackupFlags(command *cobra.Command) {
	command.Flags().StringP(flagKeyFormat, "", "hex", "start/end key format, support raw|escaped|hex")
	command.Flags().StringP(flagTiKVColumnFamily, "", "default", "backup specify cf, correspond to tikv cf")
	command.Flags().StringP(flagStartKey, "", "", "backup raw kv start key, key is inclusive")
	command.Flags().StringP(flagEndKey, "", "", "backup raw kv end key, key is exclusive")
}

// ParseFromFlags parses the backup-related flags from the flag set.
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
		return errors.New("endKey must be greater than startKey")
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

// RunBackupRaw starts a backup task inside the current goroutine.
func RunBackupRaw(c context.Context, g glue.Glue, cmdName string, cfg *RawKvConfig) error {
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
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

	defer summary.Summary(cmdName)

	backupRange := rtree.Range{StartKey: cfg.StartKey, EndKey: cfg.EndKey}

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
		StartVersion: 0,
		EndVersion:   0,
		RateLimit:    cfg.RateLimit,
		Concurrency:  cfg.Concurrency,
		IsRawKv:      true,
		Cf:           cfg.CF,
	}

	err = client.BackupRange(ctx, backupRange.StartKey, backupRange.EndKey, req, updateCh)
	if err != nil {
		return err
	}
	// Backup has finished
	updateCh.Close()

	// Checksum
	err = client.SaveBackupMeta(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}
