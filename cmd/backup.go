package cmd

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/session"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/pingcap/br/pkg/backup"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

const (
	flagBackupTimeago       = "timeago"
	flagBackupRateLimit     = "ratelimit"
	flagBackupRateLimitUnit = "ratelimit-unit"
	flagBackupConcurrency   = "concurrency"
	flagBackupChecksum      = "checksum"
	flagLastBackupTS        = "lastbackupts"
)

func defineBackupFlags(flagSet *pflag.FlagSet) {
	flagSet.StringP(
		flagBackupTimeago, "", "",
		"The history version of the backup task, e.g. 1m, 1h. Do not exceed GCSafePoint")
	flagSet.Uint64P(
		flagBackupRateLimit, "", 0, "The rate limit of the backup task, MB/s per node")
	flagSet.Uint32P(
		flagBackupConcurrency, "", 4, "The size of thread pool on each node that execute the backup task")
	flagSet.BoolP(flagBackupChecksum, "", true,
		"Run checksum after backup")
	flagSet.Uint64P(flagLastBackupTS, "", 0, "the last time backup ts")
	_ = flagSet.MarkHidden(flagLastBackupTS)

	// Test only flag.
	flagSet.Uint64P(
		flagBackupRateLimitUnit, "", utils.MB, "The unit of rate limit of the backup task")
	_ = flagSet.MarkHidden(flagBackupRateLimitUnit)
}

func runBackup(flagSet *pflag.FlagSet, cmdName, db, table string) error {
	ctx, cancel := context.WithCancel(defaultContext)
	defer cancel()

	mgr, err := GetDefaultMgr()
	if err != nil {
		return err
	}
	defer mgr.Close()

	timeago, err := flagSet.GetString(flagBackupTimeago)
	if err != nil {
		return err
	}

	ratelimit, err := flagSet.GetUint64(flagBackupRateLimit)
	if err != nil {
		return err
	}
	ratelimitUnit, err := flagSet.GetUint64(flagBackupRateLimitUnit)
	if err != nil {
		return err
	}
	ratelimit *= ratelimitUnit

	concurrency, err := flagSet.GetUint32(flagBackupConcurrency)
	if err != nil {
		return err
	}
	if concurrency == 0 {
		err = errors.New("at least one thread required")
		return err
	}

	checksum, err := flagSet.GetBool(flagBackupChecksum)
	if err != nil {
		return err
	}

	lastBackupTS, err := flagSet.GetUint64(flagLastBackupTS)
	if err != nil {
		return nil
	}

	u, err := storage.ParseBackendFromFlags(flagSet, FlagStorage)
	if err != nil {
		return err
	}

	client, err := backup.NewBackupClient(ctx, mgr)
	if err != nil {
		return nil
	}

	err = client.SetStorage(ctx, u)
	if err != nil {
		return err
	}

	backupTS, err := client.GetTS(ctx, timeago)
	if err != nil {
		return err
	}

	defer summary.Summary(cmdName)

	ranges, backupSchemas, err := backup.BuildBackupRangeAndSchema(
		mgr.GetDomain(), mgr.GetTiKV(), backupTS, db, table)
	if err != nil {
		return err
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
		ctx, cmdName, int64(approximateRegions), !HasLogFile())
	err = client.BackupRanges(
		ctx, ranges, lastBackupTS, backupTS, ratelimit, concurrency, updateCh)
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
		ctx, "Checksum", int64(backupSchemas.Len()), !HasLogFile())
	backupSchemas.SetSkipChecksum(!checksum)
	backupSchemas.Start(
		ctx, mgr.GetTiKV(), backupTS, uint(backupSchemasConcurrency), updateCh)

	err = client.CompleteMeta(backupSchemas)
	if err != nil {
		return err
	}

	valid, err := client.FastChecksum()
	if err != nil {
		return err
	}
	if !valid {
		log.Error("backup FastChecksum failed!")
	}
	// Checksum has finished
	close(updateCh)

	err = client.SaveBackupMeta(ctx)
	if err != nil {
		return err
	}
	return nil
}

// NewBackupCommand return a full backup subcommand.
func NewBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "backup",
		Short: "backup a TiDB cluster",
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
	)

	defineBackupFlags(command.PersistentFlags())
	return command
}

// newFullBackupCommand return a full backup subcommand.
func newFullBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "backup all database",
		RunE: func(command *cobra.Command, _ []string) error {
			// empty db/table means full backup.
			return runBackup(command.Flags(), "Full backup", "", "")
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
			db, err := command.Flags().GetString(flagDatabase)
			if err != nil {
				return err
			}
			if len(db) == 0 {
				return errors.Errorf("empty database name is not allowed")
			}
			return runBackup(command.Flags(), "Database backup", db, "")
		},
	}
	command.Flags().StringP(flagDatabase, "", "", "backup a table in the specific db")
	_ = command.MarkFlagRequired(flagDatabase)

	return command
}

// newTableBackupCommand return a table backup subcommand.
func newTableBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "table",
		Short: "backup a table",
		RunE: func(command *cobra.Command, _ []string) error {
			db, err := command.Flags().GetString(flagDatabase)
			if err != nil {
				return err
			}
			if len(db) == 0 {
				return errors.Errorf("empty database name is not allowed")
			}
			table, err := command.Flags().GetString(flagTable)
			if err != nil {
				return err
			}
			if len(table) == 0 {
				return errors.Errorf("empty table name is not allowed")
			}
			return runBackup(command.Flags(), "Table backup", db, table)
		},
	}
	command.Flags().StringP(flagDatabase, "", "", "backup a table in the specific db")
	command.Flags().StringP(flagTable, "t", "", "backup the specific table")
	_ = command.MarkFlagRequired(flagDatabase)
	_ = command.MarkFlagRequired(flagTable)
	return command
}
