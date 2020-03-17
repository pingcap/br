// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package cmd

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/session"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/gluetikv"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/task"
	"github.com/pingcap/br/pkg/utils"
)

func runRestoreCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RestoreConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		return err
	}
	return task.RunRestore(GetDefaultContext(), tidbGlue, cmdName, &cfg)
}

func runRestoreRawCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RestoreRawConfig{
		RawKvConfig: task.RawKvConfig{Config: task.Config{LogProgress: HasLogFile()}},
	}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		return err
	}
	return task.RunRestoreRaw(GetDefaultContext(), gluetikv.Glue{}, cmdName, &cfg)
}

func runRestoreTiflashReplicaCommand(command *cobra.Command) error {
	cfg := task.RestoreConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		return err
	}

	_, _, backupMeta, err := task.ReadBackupMeta(GetDefaultContext(), &cfg.Config)
	if err != nil {
		return err
	}

	dbs, err := utils.LoadBackupTables(backupMeta)
	if err != nil {
		return errors.Trace(err)
	}

	securityOption := pd.SecurityOption{}
	tlsConfig := cfg.TLS
	if tlsConfig.IsEnabled() {
		securityOption.CAPath = tlsConfig.CA
		securityOption.CertPath = tlsConfig.Cert
		securityOption.KeyPath = tlsConfig.Key
	}

	pdAddress := strings.Join(cfg.PD, ",")
	if len(pdAddress) == 0 {
		return errors.New("pd address can not be empty")
	}
	store, err := tidbGlue.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdAddress), securityOption)
	if err != nil {
		return err
	}
	db, err := restore.NewDB(tidbGlue, store)
	if err != nil {
		return err
	}

	for _, d := range dbs {
		for _, t := range d.Tables {
			log.Info("get table", zap.Stringer("name", t.Info.Name), zap.Int("replica", t.TiFlashReplicas))
			if t.TiFlashReplicas > 0 {
				err := db.AlterTiflashReplica(GetDefaultContext(), t, t.TiFlashReplicas)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
	return nil
}

// NewRestoreCommand returns a restore subcommand
func NewRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "restore",
		Short:        "restore a TiDB/TiKV cluster",
		SilenceUsage: false,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return err
			}
			utils.LogBRInfo()
			utils.LogArguments(c)

			// Do not run stat worker in BR.
			session.DisableStats4Test()

			summary.SetUnit(summary.RestoreUnit)
			return nil
		},
	}
	command.AddCommand(
		newFullRestoreCommand(),
		newDbRestoreCommand(),
		newTableRestoreCommand(),
		newRawRestoreCommand(),
		newTiflashReplicaRestoreCommand(),
	)
	task.DefineRestoreFlags(command.PersistentFlags())

	return command
}

func newFullRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "restore all tables",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreCommand(cmd, "Full restore")
		},
	}
	return command
}

func newDbRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "db",
		Short: "restore tables in a database",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreCommand(cmd, "Database restore")
		},
	}
	task.DefineDatabaseFlags(command)
	return command
}

func newTableRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "table",
		Short: "restore a table",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreCommand(cmd, "Table restore")
		},
	}
	task.DefineTableFlags(command)
	return command
}

func newTiflashReplicaRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "tiflash-replica",
		Short: "restore the tiflash replica before the last restore, it must only be used after the last restore failed",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreTiflashReplicaCommand(cmd)
		},
	}
	return command
}

func newRawRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "raw",
		Short: "(experimental) restore a raw kv range to TiKV cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreRawCommand(cmd, "Raw restore")
		},
	}

	task.DefineRawRestoreFlags(command)
	return command
}
