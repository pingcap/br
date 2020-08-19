// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/spf13/pflag"

	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/storage"
)

const (
	flagStartTS = "start-ts"
	flagEndTS   = "end-ts"
)

// LogRestoreConfig is the configuration specific for restore tasks.
type LogRestoreConfig struct {
	Config

	StartTS uint64
	EndTS   uint64
}

// ParseFromFlags parses the restore-related flags from the flag set.
func (cfg *LogRestoreConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.RemoveTiFlash, err = flags.GetBool(flagRemoveTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.StartTS, err = flags.GetUint64(flagStartTS)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.EndTS, err = flags.GetUint64(flagEndTS)
	if err != nil {
		return errors.Trace(err)
	}
	err = cfg.Config.ParseFromFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// adjustRestoreConfig is use for BR(binary) and BR in TiDB.
// When new config was add and not included in parser.
// we should set proper value in this function.
// so that both binary and TiDB will use same default value.
func (cfg *LogRestoreConfig) adjustRestoreConfig() {
	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultRestoreConcurrency
	}
}

// RunLogRestore starts a restore task inside the current goroutine.
func RunLogRestore(c context.Context, g glue.Glue, cfg *LogRestoreConfig) error {
	cfg.adjustRestoreConfig()

	ctx, cancel := context.WithCancel(c)
	defer cancel()

	mgr, err := newMgr(ctx, g, cfg.PD, cfg.TLS, conn.SkipTiFlash, cfg.CheckRequirements)
	if err != nil {
		return err
	}
	defer mgr.Close()

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return err
	}
	client, err := restore.NewRestoreClient(ctx, g, mgr.GetPDClient(), mgr.GetTiKV(), mgr.GetTLSConfig())
	if err != nil {
		return err
	}
	defer client.Close()

	if err = client.SetStorage(ctx, u, false); err != nil {
		return err
	}

	err = client.LoadRestoreStores(ctx)
	if err != nil {
		return err
	}

	logClient, err := restore.NewLogRestoreClient(ctx, client, cfg.StartTS, cfg.EndTS, cfg.TableFilter)
	if err != nil {
		return err
	}

	return logClient.RestoreLogData(ctx)
}
