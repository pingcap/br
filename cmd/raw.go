package cmd

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewRawCommand return a backup raw subcommand.
func NewRawCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "raw",
		Short: "backup a TiKV cluster",
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			storeID, err := cmd.LocalFlags().GetUint64("store")
			if err != nil {
				fmt.Println(errors.ErrorStack(err))
				return err
			}
			return InitDefaultRawClient(storeID)
		},
		Run: func(cmd *cobra.Command, args []string) {},
	}
	command.PersistentFlags().Uint64P("store", "s", 0, "backup at the specific store")
	command.MarkFlagRequired("store")
	command.AddCommand(NewRegionCommand())
	command.AddCommand(NewFullBackupCommand())
	return command
}

// NewFullBackupCommand return a full backup subcommand.
func NewFullBackupCommand() *cobra.Command {
	raw := &cobra.Command{
		Use:   "full [flags]",
		Short: "backup the whole TiKV cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := GetDefaultRawClient()
			return client.FullBackup()
		},
	}
	return raw
}

// NewRegionCommand return a backup region subcommand.
func NewRegionCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "region [flags]",
		Short: "backup specified regions",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := GetDefaultRawClient()
			regionID, err := cmd.LocalFlags().GetUint64("region")
			if err != nil {
				return err
			}
			backer := GetDefaultBacker()
			region, _, err := backer.GetPDClient().GetRegionByID(backer.Context(), regionID)
			if err != nil {
				return err
			}
			return client.BackupRegion(region)
		},
	}
	command.LocalFlags().Uint64P("region", "r", 0, "backup the specific regions")
	command.MarkFlagRequired("region")
	return command
}
