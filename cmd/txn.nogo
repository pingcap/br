package cmd

import (
	"fmt"

	txn "github.com/overvenus/br/pkg/txn"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewTxnCommand return a backup subcommand.
func NewTxnCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "txn <subcommand>",
		Short: "backup a TiDB cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			backer := GetDefaultBacker()
			interval, err := cmd.LocalFlags().GetDuration("interval")
			if err != nil {
				fmt.Println(errors.ErrorStack(err))
				return err
			}
			err = txn.Backup(backer, interval)
			if err != nil {
				fmt.Println(errors.ErrorStack(err))
				return err
			}
			return nil
		},
	}
	command.Flags().Duration("interval",
		txn.DefaultBackupInterval, "backup checkpoint interval")
	return command
}
