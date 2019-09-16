package cmd

import (
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewBackupCommand return a full backup subcommand.
func NewBackupCommand() *cobra.Command {
	bp := &cobra.Command{
		Use:   "backup",
		Short: "backup a TiKV cluster",
	}
	bp.AddCommand(
		newFullBackupCommand(),
		newTableBackupCommand(),
	)
	bp.PersistentFlags().Uint64P(
		"ratelimit", "", 0, "The rate limit of the backup task, MB/s per node")
	return bp
}

// newFullBackupCommand return a full backup subcommand.
func newFullBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "backup the whole TiKV cluster",
		RunE: func(command *cobra.Command, _ []string) error {
			client := GetDefaultRawClient()
			u, err := command.Flags().GetString("storage")
			if err != nil {
				return err
			}
			if u == "" {
				return errors.New("empty backup store is not allowed")
			}
			backupTS, err := client.GetTS()
			if err != nil {
				return err
			}
			rate, err := command.Flags().GetUint64("ratelimit")
			if err != nil {
				return err
			}
			err = client.BackupAllSchemas(backupTS)
			if err != nil {
				return err
			}
			err = client.BackupRange([]byte(""), []byte(""), u, backupTS, rate)
			if err != nil {
				return err
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
			client := GetDefaultRawClient()
			u, err := command.Flags().GetString("storage")
			if err != nil {
				return err
			}
			if u == "" {
				return errors.New("empty backup store is not allowed")
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

			backupTS, err := client.GetTS()
			if err != nil {
				return err
			}
			rate, err := command.Flags().GetUint64("ratelimit")
			if err != nil {
				return err
			}
			err = client.BackupTable(db, table, u, backupTS, rate)
			if err != nil {
				return err
			}
			return client.SaveBackupMeta(u)
		},
	}
	command.Flags().StringP("db", "", "", "backup a table in the specific db")
	command.Flags().StringP("table", "t", "", "backup the specific table")
	command.MarkFlagRequired("db")
	command.MarkFlagRequired("table")
	return command
}
