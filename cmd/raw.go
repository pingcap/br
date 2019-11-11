package cmd

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"

	"github.com/pingcap/br/pkg/raw"
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

	return command
}

// newFullBackupCommand return a full backup subcommand.
func newFullBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "backup the whole TiKV cluster",
		RunE: func(command *cobra.Command, _ []string) error {
			backer, err := GetDefaultBacker()
			if err != nil {
				return err
			}
			client, err := raw.NewBackupClient(backer)
			if err != nil {
				return nil
			}
			u, err := command.Flags().GetString(FlagStorage)
			if err != nil {
				return err
			}
			if u == "" {
				return errors.New("empty backup store is not allowed")
			}

			err = client.SetStorage(u)
			if err != nil {
				return err
			}

			timeAgo, err := command.Flags().GetString("timeago")
			if err != nil {
				return err
			}

			backupTS, err := client.GetTS(timeAgo)
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

			ranges, err := client.PreBackupAllTableRanges(backupTS)
			if err != nil {
				return err
			}

			// the count of regions need to backup
			approximateRegions, err := client.GetRangeRegionCount([]byte{}, []byte{})
			if err != nil {
				return err
			}

			progress := utils.NewProgressPrinter(
				"Full Backup", int64(approximateRegions))
			ctx, cancel := context.WithCancel(defaultBacker.Context())
			defer cancel()
			progress.GoPrintProgress(ctx)

			err = client.BackupRanges(
				ranges, u, backupTS, rate, concurrency, progress.UpdateCh())
			if err != nil {
				return err
			}

			err = client.CompleteMeta()
			if err != nil {
				return err
			}

			valid, err := client.FastChecksum()
			if err != nil {
				return err
			}

			if !valid {
				log.Error("backup FastChecksum not passed!")
			}

			return client.SaveBackupMeta(u)
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
			backer, err := GetDefaultBacker()
			if err != nil {
				return err
			}
			client, err := raw.NewBackupClient(backer)
			if err != nil {
				return err
			}
			u, err := command.Flags().GetString(FlagStorage)
			if err != nil {
				return err
			}
			if u == "" {
				return errors.New("empty backup store is not allowed")
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

			backupTS, err := client.GetTS(timeAgo)
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

			ranges, err := client.PreBackupTableRanges(db, table, u, backupTS, rate, concurrency)
			if err != nil {
				return err
			}
			// the count of regions need to backup
			approximateRegions := 0
			for _, r := range ranges {
				var regionCount int
				regionCount, err = client.GetRangeRegionCount(r.StartKey, r.EndKey)
				if err != nil {
					return err
				}
				approximateRegions += regionCount
			}

			progress := utils.NewProgressPrinter(
				"Table Backup", int64(approximateRegions))
			ctx, cancel := context.WithCancel(defaultBacker.Context())
			defer cancel()
			progress.GoPrintProgress(ctx)

			err = client.BackupRanges(
				ranges, u, backupTS, rate, concurrency, progress.UpdateCh())
			if err != nil {
				return err
			}

			err = client.CompleteMeta()
			if err != nil {
				return err
			}

			valid, err := client.FastChecksum()
			if err != nil {
				return err
			}

			if !valid {
				log.Error("backup FastChecksum not passed!")
			}

			return client.SaveBackupMeta(u)
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
