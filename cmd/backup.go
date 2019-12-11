package cmd

import (
	"bytes"
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
	flagBackupTimeago     = "timeago"
	flagBackupRateLimit   = "ratelimit"
	flagBackupConcurrency = "concurrency"
	flagBackupChecksum    = "checksum"
)

type backupContext struct {
	db    string
	table string

	isRawKv  bool
	startKey []byte
	endKey   []byte
	cf       string
}

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
}

func runBackup(flagSet *pflag.FlagSet, cmdName string, bc backupContext) error {
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

	var (
		ranges        []backup.Range
		backupSchemas *backup.Schemas
	)
	if bc.isRawKv {
		ranges = []backup.Range{{StartKey: bc.startKey, EndKey: bc.endKey}}
	} else {
		ranges, backupSchemas, err = backup.BuildBackupRangeAndSchema(
			mgr.GetDomain(), mgr.GetTiKV(), backupTS, bc.db, bc.table)
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
	updateCh := utils.StartProgress(
		ctx, cmdName, int64(approximateRegions), !HasLogFile())
	err = client.BackupRanges(
		ctx, ranges, backupTS, ratelimit, concurrency, updateCh, bc.isRawKv, bc.cf)
	if err != nil {
		return err
	}
	// Backup has finished
	close(updateCh)

	if backupSchemas != nil {
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
	}

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
		newRawBackupCommand(),
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
			bc := backupContext{db: "", table: "", isRawKv: false}
			return runBackup(command.Flags(), "Full backup", bc)
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
			bc := backupContext{db: db, table: "", isRawKv: false}
			return runBackup(command.Flags(), "Database backup", bc)
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
			bc := backupContext{db: db, table: table, isRawKv: false}
			return runBackup(command.Flags(), "Table backup", bc)
		},
	}
	command.Flags().StringP(flagDatabase, "", "", "backup a table in the specific db")
	command.Flags().StringP(flagTable, "t", "", "backup the specific table")
	_ = command.MarkFlagRequired(flagDatabase)
	_ = command.MarkFlagRequired(flagTable)
	return command
}

// newRawBackupCommand return a raw kv range backup subcommand.
func newRawBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "raw",
		Short: "backup a raw kv range from TiKV cluster",
		RunE: func(command *cobra.Command, _ []string) error {
			start, err := command.Flags().GetString("start")
			if err != nil {
				return err
			}
			startKey, err := utils.ParseKey(command.Flags(), start)
			if err != nil {
				return err
			}
			end, err := command.Flags().GetString("end")
			if err != nil {
				return err
			}
			endKey, err := utils.ParseKey(command.Flags(), end)
			if err != nil {
				return err
			}

			cf, err := command.Flags().GetString("cf")
			if err != nil {
				return err
			}

			if bytes.Compare(startKey, endKey) > 0 {
				return errors.New("input endKey must greater or equal than startKey")
			}
			bc := backupContext{startKey: startKey, endKey: endKey, isRawKv: true, cf: cf}
			return runBackup(command.Flags(), "Raw Backup", bc)
		},
	}
	command.Flags().StringP("format", "", "raw", "raw key format")
	command.Flags().StringP("cf", "", "default", "backup raw kv cf")
	command.Flags().StringP("start", "", "", "backup raw kv start key")
	command.Flags().StringP("end", "", "", "backup raw kv end key")
	return command
}
