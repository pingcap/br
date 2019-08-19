package cmd

import (
	"io/ioutil"

	"github.com/gogo/protobuf/proto"
	"github.com/overvenus/br/pkg/restore"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/cobra"
)

// NewRestoreCommand return a restore command
func NewRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "restore",
		Short: "restore a TiKV cluster from a backup",
		RunE: func(cmd *cobra.Command, _ []string) error {
			concurrency, err := cmd.Flags().GetInt("concurrency")
			if err != nil {
				return err
			}
			srcAddr, err := cmd.Flags().GetString("src")
			if err != nil {
				return err
			}
			destAddr, err := cmd.Flags().GetString("dest")
			if err != nil {
				return err
			}
			tableName, err := cmd.Flags().GetString("table")
			if err != nil {
				return err
			}
			importerAddr, err := cmd.Flags().GetString("importer")
			if err != nil {
				return err
			}
			metaLocation, err := cmd.Flags().GetString("meta")
			if err != nil {
				return err
			}
			metaData, err := ioutil.ReadFile(metaLocation)
			if err != nil {
				return err
			}
			meta := &backup.BackupMeta{}
			err = proto.Unmarshal(metaData, meta)
			if err != nil {
				return err
			}
			pdAddrs, err := cmd.Flags().GetString("pd")
			if err != nil {
				return err
			}
			statusPort, err := cmd.Flags().GetInt("status-port")
			if err != nil {
				return err
			}
			table, err := restore.CreateTable(srcAddr, destAddr, tableName, statusPort)
			if err != nil {
				return err
			}

			restore.Restore(concurrency, importerAddr, meta, table, pdAddrs)

			return nil
		},
	}

	command.Flags().StringP("src", "r", "", "source tidb address, format: username:password@protocol(address)/dbname")
	command.Flags().StringP("dest", "d", "", "destination tidb address, format: username:password@protocol(address)/dbname")
	command.Flags().StringP("table", "t", "", "table name")
	command.Flags().StringP("importer", "i", "", "importer address")
	command.Flags().StringP("meta", "m", "", "meta file location")
	command.Flags().IntP("concurrency", "c", 8, "number of concurrent restore files")
	command.Flags().IntP("status-port", "P", 10080, "tidb status port")

	return command
}
