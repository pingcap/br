package cmd

import (
	"github.com/pingcap/br/pkg/raw"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
)

// NewBackupCommand return a full backup subcommand.
func NewBackupCommand() *cobra.Command {
	bp := &cobra.Command{
		Use:   "backup",
		Short: "backup a TiKV cluster",
		PersistentPreRun: func(c *cobra.Command, args []string) {
			utils.LogBRInfo()
		},
	}
	bp.AddCommand(
		newFullBackupCommand(),
		newTableBackupCommand(),
	)

	bp.PersistentFlags().BoolP("checksum", "", false, "The checksum verification switch")

	bp.PersistentFlags().StringP("timeago", "", "", "The history version of the backup task, e.g. 1m, 1h. Do not exceed GCSafePoint")

	bp.PersistentFlags().Uint64P(
		"ratelimit", "", 0, "The rate limit of the backup task, MB/s per node")
	bp.PersistentFlags().Uint32P(
		"concurrency", "", 4, "The size of thread pool on each node that execute the backup task")
	return bp
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

			checksumSwitch, err := command.Flags().GetBool("checksum")
			if err != nil {
				return err
			}

			ranges, err := client.GetAllBackupTableRanges(backupTS, checksumSwitch)
			if err != nil {
				return err
			}

			done := make(chan struct{}, 1)

			// the count of regions need to backup
			approximateRegions, err := client.GetRangeRegionCount([]byte{}, []byte{})
			if err != nil {
				return err
			}

			go func() {
				client.PrintBackupProgress("Full Backup", int64(approximateRegions), done)
			}()

			for _, r := range ranges {
				err = client.BackupRange(r.StartKey, r.EndKey, u, backupTS, rate, concurrency)
				if err != nil {
					return err
				}
			}
			if checksumSwitch {
				valid, err := client.FastChecksum()
				if err != nil {
					return err
				}

				if !valid {
					log.Error("backup checksumSwitch not passed!")
				}
			}
			done <- struct{}{}
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

			checksumSwitch, err := command.Flags().GetBool("checksum")
			if err != nil {
				return err
			}

			ranges, err := client.GetBackupTableRanges(db, table, u, backupTS, rate, concurrency, checksumSwitch)
			if err != nil {
				return err
			}
			done := make(chan struct{}, 1)
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

			go func() {
				client.PrintBackupProgress("Table Backup", int64(approximateRegions), done)
			}()

			for _, r := range ranges {
				err = client.BackupRange(r.StartKey, r.EndKey, u, backupTS, rate, concurrency)
				if err != nil {
					return err
				}
			}
			if checksumSwitch {
				valid, err := client.FastChecksum()
				if err != nil {
					return err
				}

				if !valid {
					log.Error("backup checksumSwitch not passed!")
				}
			}
			done <- struct{}{}
			return client.SaveBackupMeta(u)
		},
	}
	command.Flags().StringP("db", "", "", "backup a table in the specific db")
	command.Flags().StringP("table", "t", "", "backup the specific table")
	command.MarkFlagRequired("db")
	command.MarkFlagRequired("table")
	return command
}
