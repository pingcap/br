// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package cmd

import (
	"github.com/pingcap/tidb/session"
	"github.com/spf13/cobra"

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

// NewRestoreCommand returns a restore subcommand
func NewRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "restore",
		Short:        "restore a TiKV cluster from a backup",
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
