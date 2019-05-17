package cmd

import (
	"fmt"
	"os"

	bp "github.com/overvenus/br/backup"
	"github.com/spf13/cobra"
)

// NewBackupCommand return a backup subcommand.
func NewBackupCommand() *cobra.Command {
	backup := &cobra.Command{
		Use:   "backup <subcommand>",
		Short: "backup a TiDB/TiKV cluster",
		PersistentPreRunE: func(cmd *cobra.Command, arg []string) error {
			addr, err := cmd.InheritedFlags().GetString(FlagPD)
			if err != nil {
				return err
			}
			if addr == "" {
				return fmt.Errorf("pd address can not be empty")
			}
			InitDefaultBacker(addr)
			return nil
		},
		Run: func(cmd *cobra.Command, _ []string) {
			backer := GetDefaultBacker()
			interval, err := cmd.LocalFlags().GetDuration("interval")
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			err = backer.Backup(interval)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	backup.Flags().Duration("interval",
		bp.DefaultBackupInterval, "backup checkpoint interval")
	return backup
}
