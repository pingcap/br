package cmd

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// NewBackupCommand return a full backup subcommand.
func NewBackupCommand() *cobra.Command {
	bp := &cobra.Command{
		Use:   "backup",
		Short: "backup a TiKV cluster",
	}
	bp.AddCommand(
		newFullBackupCommand(),
		newRegionCommand(),
		newStopBackupCommand(),
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
			concurrency, err := command.Flags().GetInt("concurrency")
			if err != nil {
				return err
			}
			batch, err := command.Flags().GetInt("batch")
			if err != nil {
				return err
			}
			return client.FullBackup(concurrency, batch)
		},
	}
	command.Flags().IntP("concurrency", "c", 20, "number of concurrent backup regions")
	command.Flags().IntP("batch", "b", 4, "number of batched backup regions")
	return command
}

// newStopBackupCommand return a full backup subcommand.
func newStopBackupCommand() *cobra.Command {
	raw := &cobra.Command{
		Use:   "stop",
		Short: "stop backup",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := GetDefaultRawClient()
			resp, err := client.Stop()
			if err != nil {
				return err
			}
			log.Info("rotate backup",
				zap.Uint64("dependence", resp.GetCurrentDependency()))
			return nil
		},
	}
	return raw
}

// newRegionCommand return a backup region subcommand.
func newRegionCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "region [flags]",
		Short: "backup specified regions",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := GetDefaultRawClient()
			regionID, err := cmd.Flags().GetUint64("region")
			if err != nil {
				return err
			}
			backer := GetDefaultBacker()
			retryCount := 5
			retryErr := errors.Errorf("exceed max retry %d", retryCount)
			for i := 0; i < retryCount; i++ {
				region, _, err := backer.GetPDClient().GetRegionByID(backer.Context(), regionID)
				if err != nil {
					return err
				}
				needRrtry, err := client.BackupRegion(region)
				if err != nil {
					return err
				}
				if !needRrtry {
					retryErr = nil
					break
				}
			}
			return retryErr
		},
	}
	command.Flags().Uint64P("region", "r", 0, "backup the specific regions")
	command.MarkFlagRequired("region")
	return command
}
