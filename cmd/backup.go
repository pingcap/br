package cmd

import (
	_ "github.com/overvenus/br/backup"
	"github.com/spf13/cobra"
)

// NewBackupCommand return a backup subcommand.
func NewBackupCommand() *cobra.Command {
	backup := &cobra.Command{
		Use:   "backup <subcommand>",
		Short: "backup a TiDB/TiKV cluster",
		// TODO: add function
	}
	return backup
}
