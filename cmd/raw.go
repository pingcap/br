package cmd

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/codec"
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
		// newRegionCommand(),
	)
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
			return client.BackupRange([]byte(""), []byte(""), u)
		},
	}
	return command
}

// newRegionCommand return a backup region subcommand.
func newRegionCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "region [flags]",
		Short: "backup specified regions",
		RunE: func(command *cobra.Command, _ []string) error {
			client := GetDefaultRawClient()
			regionID, err := command.Flags().GetUint64("region")
			if err != nil {
				return err
			}
			u, err := command.Flags().GetString("storage")
			if err != nil {
				return err
			}
			useRaw, err := command.Flags().GetBool("raw")
			if err != nil {
				return err
			}
			backer := GetDefaultBacker()
			region, _, err := backer.GetPDClient().GetRegionByID(backer.Context(), regionID)
			if err != nil {
				return err
			}
			startKey := region.GetStartKey()
			endKey := region.GetEndKey()
			if !useRaw {
				startKey, _, err = codec.DecodeBytes(startKey, nil)
				if err != nil {
					return err
				}
				endKey, _, err = codec.DecodeBytes(endKey, nil)
				if err != nil {
					return err
				}
			}
			err = client.BackupRange(startKey, endKey, u)
			if err != nil {
				return err
			}
			return nil
		},
	}
	command.Flags().BoolP("raw", "raw", false,
		"backup region with decode region keys")
	command.Flags().Uint64P("region", "r", 0, "backup the specific region")
	command.MarkFlagRequired("region")
	return command
}
