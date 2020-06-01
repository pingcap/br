// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package cmd

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/session"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/gluetikv"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/task"
	"github.com/pingcap/br/pkg/utils"
)

func runBackupCommand(command *cobra.Command, cmdName string) error {
	cfg := task.BackupConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return err
	}
	if err := task.RunBackup(GetDefaultContext(), tidbGlue, cmdName, &cfg); err != nil {
		log.Error("failed to backup", zap.Error(err))
		return err
	}
	return nil
}

func runBackupRawCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RawKvConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return err
	}
	if err := task.RunBackupRaw(GetDefaultContext(), gluetikv.Glue{}, cmdName, &cfg); err != nil {
		log.Error("failed to backup raw kv", zap.Error(err))
		return err
	}
	return nil
}

// NewBackupCommand return a full backup subcommand.
func NewBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "backup",
		Short:        "backup a TiDB/TiKV cluster",
		SilenceUsage: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return err
			}
			utils.LogBRInfo()
			utils.LogArguments(c)

			// Do not run ddl worker in BR.
			ddl.RunWorker = false
			// Do not run stat worker in BR.
			session.DisableStats4Test()

			summary.SetUnit(summary.BackupUnit)
			return nil
		},
	}
	command.AddCommand(
		newFullBackupCommand(),
		newDbBackupCommand(),
		newTableBackupCommand(),
		newRawBackupCommand(),
	)

	task.DefineBackupFlags(command.PersistentFlags())
	return command
}

// newFullBackupCommand return a full backup subcommand.
func newFullBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "backup all database",
		RunE: func(command *cobra.Command, _ []string) error {
			// empty db/table means full backup.
			return runBackupCommand(command, "Full backup")
		},
	}
	return command
}

// newDbBackupCommand return a db backup subcommand.
func newDbBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "db",
		Short: "backup a database",
		RunE: func(command *cobra.Command, _ []string) error {
			return runBackupCommand(command, "Database backup")
		},
	}
	task.DefineDatabaseFlags(command)
	return command
}

// newTableBackupCommand return a table backup subcommand.
func newTableBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "table",
		Short: "backup a table",
		RunE: func(command *cobra.Command, _ []string) error {
			return runBackupCommand(command, "Table backup")
		},
	}
	task.DefineTableFlags(command)
	return command
}

// newRawBackupCommand return a raw kv range backup subcommand.
func newRawBackupCommand() *cobra.Command {
	// TODO: remove experimental tag if it's stable
	command := &cobra.Command{
		Use:   "raw",
		Short: "(experimental) backup a raw kv range from TiKV cluster",
		RunE: func(command *cobra.Command, _ []string) error {
			return runBackupRawCommand(command, "Raw backup")
		},
	}

	task.DefineRawBackupFlags(command)
	return command
}
