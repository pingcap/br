package cmd

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/session"
	"github.com/spf13/cobra"

	"github.com/pingcap/br/pkg/backup"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/utils"
)

// NewBackupCommand return a full backup subcommand.
func NewBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "backup",
		Short: "backup a TiKV cluster",
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
			return nil
		},
	}
	command.AddCommand(
		newFullBackupCommand(),
		newTableBackupCommand(),
	)

	command.PersistentFlags().StringP(
		"timeago", "", "",
		"The history version of the backup task, e.g. 1m, 1h. Do not exceed GCSafePoint")

	command.PersistentFlags().Uint64P(
		"ratelimit", "", 0, "The rate limit of the backup task, MB/s per node")
	command.PersistentFlags().Uint32P(
		"concurrency", "", 4, "The size of thread pool on each node that execute the backup task")
	command.PersistentFlags().BoolP("checksum", "", true,
		"Run checksum after backup")

	command.PersistentFlags().BoolP("fastchecksum", "", false,
		"fast checksum backup sst file by calculate all sst file")

	_ = command.PersistentFlags().MarkHidden("fastchecksum")
	return command
}

// newFullBackupCommand return a full backup subcommand.
func newFullBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "backup the whole TiKV cluster",
		RunE: func(command *cobra.Command, _ []string) error {
			ctx, cancel := context.WithCancel(defaultContext)
			defer cancel()

			mgr, err := GetDefaultMgr()
			if err != nil {
				return err
			}
			defer mgr.Close()

			client, err := backup.NewBackupClient(ctx, mgr)
			if err != nil {
				return nil
			}
			u, err := storage.ParseBackendFromFlags(command.Flags(), FlagStorage)
			if err != nil {
				return err
			}

			err = client.SetStorage(u)
			if err != nil {
				return err
			}

			timeAgo, err := command.Flags().GetString("timeago")
			if err != nil {
				return err
			}

			backupTS, err := client.GetTS(ctx, timeAgo)
			if err != nil {
				return err
			}

			rate, err := command.Flags().GetUint64("ratelimit")
			if err != nil {
				return err
			}

			concurrency, err := command.Flags().GetUint32("concurrency")
			if err != nil {
				return err
			}
			if concurrency == 0 {
				return errors.New("at least one thread required")
			}

			checksum, err := command.Flags().GetBool("checksum")
			if err != nil {
				return err
			}
			fastChecksum, err := command.Flags().GetBool("fastchecksum")
			if err != nil {
				return err
			}

			ranges, backupSchemas, err := backup.BuildBackupRangeAndSchema(
				mgr.GetDomain(), mgr.GetTiKV(), backupTS, "", "")
			if err != nil {
				return err
			}

			// the count of regions need to backup
			approximateRegions, err := client.GetRangeRegionCount([]byte{}, []byte{})
			if err != nil {
				return err
			}

			// Backup
			// Redirect to log if there is no log file to avoid unreadable output.
			updateCh := utils.StartProgress(
				ctx, "Full Backup", int64(approximateRegions), !HasLogFile())
			err = client.BackupRanges(
				ctx, ranges, backupTS, rate, concurrency, updateCh)
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
			cksctx, ckscancel := context.WithCancel(defaultContext)
			defer ckscancel()
			updateCh = utils.StartProgress(
				cksctx, "Checksum", int64(backupSchemas.Len()), !HasLogFile())
			backupSchemas.SetSkipChecksum(!checksum)
			backupSchemas.Start(
				cksctx, mgr.GetTiKV(), backupTS, uint(backupSchemasConcurrency), updateCh)

			err = client.CompleteMeta(backupSchemas)
			if err != nil {
				return err
			}

			if fastChecksum {
				valid, err := client.FastChecksum()
				if err != nil {
					return err
				}
				if !valid {
					log.Error("backup FastChecksum not passed!")
				}
			}
			// Checksum has finished
			close(updateCh)

			return client.SaveBackupMeta()
		},
	}
	return command
}

// newTableBackupCommand return a table backup subcommand.
func newTableBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "table",
		Short: "backup a table",
		RunE: func(command *cobra.Command, _ []string) error {
			ctx, cancel := context.WithCancel(defaultContext)
			defer cancel()

			mgr, err := GetDefaultMgr()
			if err != nil {
				return err
			}
			defer mgr.Close()

			client, err := backup.NewBackupClient(ctx, mgr)
			if err != nil {
				return err
			}
			u, err := storage.ParseBackendFromFlags(command.Flags(), FlagStorage)
			if err != nil {
				return err
			}

			err = client.SetStorage(u)
			if err != nil {
				return err
			}

			db, err := command.Flags().GetString("db")
			if err != nil {
				return err
			}
			if len(db) == 0 {
				return errors.Errorf("empty database name is not allowed")
			}
			table, err := command.Flags().GetString("table")
			if err != nil {
				return err
			}
			if len(table) == 0 {
				return errors.Errorf("empty table name is not allowed")
			}

			timeAgo, err := command.Flags().GetString("timeago")
			if err != nil {
				return err
			}

			backupTS, err := client.GetTS(ctx, timeAgo)
			if err != nil {
				return err
			}

			rate, err := command.Flags().GetUint64("ratelimit")
			if err != nil {
				return err
			}
			concurrency, err := command.Flags().GetUint32("concurrency")
			if err != nil {
				return err
			}
			if concurrency == 0 {
				return errors.New("at least one thread required")
			}
			checksum, err := command.Flags().GetBool("checksum")
			if err != nil {
				return err
			}
			fastChecksum, err := command.Flags().GetBool("fastchecksum")
			if err != nil {
				return err
			}

			ranges, backupSchemas, err := backup.BuildBackupRangeAndSchema(
				mgr.GetDomain(), mgr.GetTiKV(), backupTS, db, table)
			if err != nil {
				return err
			}

			// The number of regions need to backup
			approximateRegions := 0
			for _, r := range ranges {
				var regionCount int
				regionCount, err = client.GetRangeRegionCount(r.StartKey, r.EndKey)
				if err != nil {
					return err
				}
				approximateRegions += regionCount
			}

			// Backup
			// Redirect to log if there is no log file to avoid unreadable output.
			updateCh := utils.StartProgress(
				ctx, "Table Backup", int64(approximateRegions), !HasLogFile())
			err = client.BackupRanges(
				ctx, ranges, backupTS, rate, concurrency, updateCh)
			if err != nil {
				return err
			}
			// Backup has finished
			close(updateCh)

			// Checksum
			cksctx, ckscancel := context.WithCancel(defaultContext)
			defer ckscancel()
			updateCh = utils.StartProgress(
				cksctx, "Checksum", int64(backupSchemas.Len()), !HasLogFile())
			backupSchemas.SetSkipChecksum(!checksum)
			backupSchemas.Start(
				cksctx, mgr.GetTiKV(), backupTS, 1, updateCh)

			err = client.CompleteMeta(backupSchemas)
			if err != nil {
				return err
			}

			if fastChecksum {
				valid, err := client.FastChecksum()
				if err != nil {
					return err
				}
				if !valid {
					log.Error("backup FastChecksum not passed!")
				}
			}
			// Checksum has finished
			close(updateCh)

			return client.SaveBackupMeta()
		},
	}
	command.Flags().StringP("db", "", "", "backup a table in the specific db")
	command.Flags().StringP("table", "t", "", "backup the specific table")
	if err := command.MarkFlagRequired("db"); err != nil {
		panic(err)
	}
	if err := command.MarkFlagRequired("table"); err != nil {
		panic(err)
	}
	return command
}
