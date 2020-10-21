// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/storage"
)

const (
	flagStartTS         = "start-ts"
	flagEndTS           = "end-ts"
	flagBatchWriteCount = "write-kvs"
	flagBatchFlushCount = "flush-kvs"

	// represents kv flush to storage for each table.
	defaultFlushKV = 5120
	// represents kv size flush to storage for each table.
	defaultFlushKVSize = 5 << 20
	// represents kv that write to TiKV once at at time.
	defaultWriteKV = 1280
)

// LogRestoreConfig is the configuration specific for restore tasks.
type LogRestoreConfig struct {
	Config

	StartTS uint64
	EndTS   uint64

	BatchFlushKVPairs int
	BatchFlushKVSize  int64
	BatchWriteKVPairs int
}

// DefineLogRestoreFlags defines common flags for the backup command.
func DefineLogRestoreFlags(command *cobra.Command) {
	command.Flags().Uint64P(flagStartTS, "", 0, "restore log start ts")
	command.Flags().Uint64P(flagEndTS, "", 0, "restore log end ts")

	command.Flags().Uint64P(flagBatchWriteCount, "", 0, "the kv count that write to TiKV once at a time")
	command.Flags().Uint64P(flagBatchFlushCount, "", 0, "the kv count that flush from memory to TiKV")
}

// ParseFromFlags parses the restore-related flags from the flag set.
func (cfg *LogRestoreConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
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
	cfg.adjust()

	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultRestoreConcurrency
	}
	if cfg.BatchFlushKVPairs == 0 {
		cfg.BatchFlushKVPairs = defaultFlushKV
	}
	if cfg.BatchWriteKVPairs == 0 {
		cfg.BatchWriteKVPairs = defaultWriteKV
	}
	if cfg.BatchFlushKVSize == 0 {
		cfg.BatchFlushKVSize = defaultFlushKVSize
	}
	// write kv count doesn't have to excceed flush kv count.
	if cfg.BatchWriteKVPairs > cfg.BatchFlushKVPairs {
		cfg.BatchWriteKVPairs = cfg.BatchFlushKVPairs
	}
}

// RunLogRestore starts a restore task inside the current goroutine.
func RunLogRestore(c context.Context, g glue.Glue, cfg *LogRestoreConfig) error {
	cfg.adjustRestoreConfig()

	ctx, cancel := context.WithCancel(c)
	defer cancel()

	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements)
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

	logClient, err := restore.NewLogRestoreClient(
		ctx, client, cfg.StartTS, cfg.EndTS, cfg.TableFilter, uint(cfg.Concurrency),
		cfg.BatchFlushKVPairs, cfg.BatchFlushKVSize, cfg.BatchWriteKVPairs)
	if err != nil {
		return err
	}

	return logClient.RestoreLogData(ctx, mgr.GetDomain())
}
