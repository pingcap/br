// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package cmd

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/session"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/gluetikv"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/task"
	"github.com/pingcap/br/pkg/utils"
)

func runRestoreCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RestoreConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return err
	}
	if err := task.RunRestore(GetDefaultContext(), tidbGlue, cmdName, &cfg); err != nil {
		log.Error("failed to restore", zap.Error(err))
		return err
	}
	return nil
}

func runLogRestoreCommand(command *cobra.Command) error {
	cfg := task.LogRestoreConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return err
	}
	if err := task.RunLogRestore(GetDefaultContext(), tidbGlue, &cfg); err != nil {
		log.Error("failed to restore", zap.Error(err))
		return err
	}
	return nil
}

func runRestoreRawCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RestoreRawConfig{
		RawKvConfig: task.RawKvConfig{Config: task.Config{LogProgress: HasLogFile()}},
	}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return err
	}
	if err := task.RunRestoreRaw(GetDefaultContext(), gluetikv.Glue{}, cmdName, &cfg); err != nil {
		log.Error("failed to restore raw kv", zap.Error(err))
		return err
	}
	return nil
}

func runRestoreTiflashReplicaCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RestoreConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return err
	}

	if err := task.RunRestoreTiflashReplica(GetDefaultContext(), tidbGlue, cmdName, &cfg); err != nil {
		log.Error("failed to restore tiflash replica", zap.Error(err))
		return err
	}
	return nil
}

// NewRestoreCommand returns a restore subcommand.
func NewRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "restore",
		Short:        "restore a TiDB/TiKV cluster",
		SilenceUsage: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return err
			}
			utils.LogBRInfo()
			task.LogArguments(c)

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
		newLogRestoreCommand(),
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
	task.DefineFilterFlags(command)
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

func newLogRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "log",
		Short: "restore data from log backup",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runLogRestoreCommand(cmd)
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
			return runRestoreTiflashReplicaCommand(cmd, "Restore TiFlash Replica")
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
