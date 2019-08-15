package cmd

import (
	"context"
	"io/ioutil"

	"github.com/gogo/protobuf/proto"
	"github.com/overvenus/br/pkg/restore"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/cobra"
)

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
			pdAddr, err := cmd.Flags().GetString("pd")
			if err != nil {
				return err
			}
			table, err := restore.CreateTable(srcAddr, destAddr, tableName)
			if err != nil {
				return err
			}
			ctx := context.Background()
			restore.Restore(ctx, concurrency, importerAddr, meta, table, pdAddr)

			return nil
		},
	}

	command.Flags().StringP("src", "s", "", "source tidb address, format: username:password@protocol(address)/dbname")
	command.Flags().StringP("dest", "d", "", "destination tidb address, format: username:password@protocol(address)/dbname")
	command.Flags().StringP("table", "t", "", "table name")
	command.Flags().StringP("importer", "i", "", "importer address")
	command.Flags().StringP("meta", "m", "", "meta file location")
	command.Flags().IntP("concurrency", "c", 20, "number of concurrent restore files")

	return command
}
