package cmd

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"io/ioutil"
)

// NewRestoreCommand returns a restore subcommand
func NewRestoreCommand() *cobra.Command {
	bp := &cobra.Command{
		Use:   "restore",
		Short: "restore a TiKV cluster from a backup",
	}
	bp.AddCommand(
		newFullRestoreCommand(),
		newDbRestoreCommand(),
		newTableRestoreCommand(),
	)
	return bp
}

func newFullRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "restore all tables",
		RunE: func(cmd *cobra.Command, _ []string) error {
			pdAddr, err := cmd.Flags().GetString(FlagPD)
			if err != nil {
				return errors.Trace(err)
			}
			ctx, cancel := context.WithCancel(GetDefaultContext())
			defer cancel()
			client, err := restore.NewRestoreClient(ctx, pdAddr)
			if err != nil {
				return errors.Trace(err)
			}
			err = initRestoreClient(client, cmd.Flags())
			if err != nil {
				return errors.Trace(err)
			}
			restoreTS, err := client.GetTS()
			if err != nil {
				return errors.Trace(err)
			}
			err = client.SwitchClusterMode(import_sstpb.SwitchMode_Import)
			if err != nil {
				return errors.Trace(err)
			}
			defer client.SwitchClusterMode(import_sstpb.SwitchMode_Normal)
			err = client.RestoreAll(restoreTS)
			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)")
	command.Flags().String("importer", "", "the address of tikv importer, ip:port")
	command.Flags().String("meta", "", "meta file location")
	command.Flags().String("status", "", "the address to check tidb status, ip:port")
	command.Flags().Int("partition-size", 50, "max number of sst per importer engine file")
	command.Flags().Int("max-open-engines", 4, "max opened importer engine files")
	command.Flags().Int("max-import-jobs", 4, "max concurrent import jobs, may cause importer OOM if it is too big")

	command.MarkFlagRequired("connect")
	command.MarkFlagRequired("importer")
	command.MarkFlagRequired("meta")
	command.MarkFlagRequired("status")

	return command
}

func newDbRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "db",
		Short: "restore tables in a database",
		RunE: func(cmd *cobra.Command, _ []string) error {
			pdAddr, err := cmd.Flags().GetString(FlagPD)
			if err != nil {
				return errors.Trace(err)
			}
			ctx, cancel := context.WithCancel(GetDefaultContext())
			defer cancel()
			client, err := restore.NewRestoreClient(ctx, pdAddr)
			if err != nil {
				return errors.Trace(err)
			}
			err = initRestoreClient(client, cmd.Flags())
			if err != nil {
				return errors.Trace(err)
			}
			restoreTS, err := client.GetTS()
			if err != nil {
				return errors.Trace(err)
			}
			err = client.SwitchClusterMode(import_sstpb.SwitchMode_Import)
			if err != nil {
				return errors.Trace(err)
			}
			defer client.SwitchClusterMode(import_sstpb.SwitchMode_Normal)
			dbName, err := cmd.Flags().GetString("db")
			if err != nil {
				return errors.Trace(err)
			}
			db := client.GetDatabase(dbName)
			if db == nil {
				return errors.Trace(fmt.Errorf("not exists database"))
			}
			err = restore.CreateDatabase(db.Schema, client.GetDbDSN())
			if err != nil {
				return errors.Trace(err)
			}
			err = client.RestoreDatabase(db, restoreTS)
			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)/")
	command.Flags().String("importer", "", "the address of tikv importer, ip:port")
	command.Flags().String("meta", "", "meta file location")
	command.Flags().String("status", "", "the address to check tidb status, ip:port")
	command.Flags().Int("partition-size", 50, "max number of sst per importer engine file")
	command.Flags().Int("max-open-engines", 4, "max opened importer engine file")
	command.Flags().Int("max-import-jobs", 4, "max concurrent import jobs, may cause importer OOM if it is too big")

	command.Flags().String("db", "", "database name")

	command.MarkFlagRequired("connect")
	command.MarkFlagRequired("importer")
	command.MarkFlagRequired("meta")
	command.MarkFlagRequired("status")
	command.MarkFlagRequired("db")

	return command
}

func newTableRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "table",
		Short: "restore a table",
		RunE: func(cmd *cobra.Command, _ []string) error {
			pdAddr, err := cmd.Flags().GetString(FlagPD)
			if err != nil {
				return errors.Trace(err)
			}
			ctx, cancel := context.WithCancel(GetDefaultContext())
			defer cancel()
			client, err := restore.NewRestoreClient(ctx, pdAddr)
			if err != nil {
				return errors.Trace(err)
			}
			err = initRestoreClient(client, cmd.Flags())
			if err != nil {
				return errors.Trace(err)
			}
			restoreTS, err := client.GetTS()
			if err != nil {
				return errors.Trace(err)
			}
			err = client.SwitchClusterMode(import_sstpb.SwitchMode_Import)
			if err != nil {
				return errors.Trace(err)
			}
			defer client.SwitchClusterMode(import_sstpb.SwitchMode_Normal)
			dbName, err := cmd.Flags().GetString("db")
			if err != nil {
				return errors.Trace(err)
			}
			db := client.GetDatabase(dbName)
			if db == nil {
				return errors.Trace(fmt.Errorf("not exists database"))
			}
			err = restore.CreateDatabase(db.Schema, client.GetDbDSN())
			if err != nil {
				return errors.Trace(err)
			}
			tableName, err := cmd.Flags().GetString("table")
			if err != nil {
				return errors.Trace(err)
			}
			table := db.GetTable(tableName)
			if table == nil {
				return errors.Trace(fmt.Errorf("not exists table"))
			}
			err = restore.CreateTable(db.Schema.Name.String(), table, client.GetDbDSN())
			if err != nil {
				return errors.Trace(err)
			}
			err = restore.AlterAutoIncID(db.Schema.Name.String(), table, client.GetDbDSN())
			if err != nil {
				return errors.Trace(err)
			}
			fileGroups := db.GetFileGroups(tableName)
			if len(fileGroups) <= 0 {
				return errors.Trace(fmt.Errorf("not exists table"))
			}
			err = client.RestoreMultipleTables(fileGroups, restoreTS)
			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)/")
	command.Flags().String("importer", "", "the address of tikv importer, ip:port")
	command.Flags().String("meta", "", "meta file location")
	command.Flags().String("status", "", "the address to check tidb status, ip:port")
	command.Flags().Int("partition-size", 50, "max number of sst per importer engine file")
	command.Flags().Int("max-open-engines", 4, "max opened importer engine file")
	command.Flags().Int("max-import-jobs", 4, "max concurrent import engine jobs, may cause importer OOM if it is too big")

	command.Flags().String("db", "", "database name")
	command.Flags().String("table", "", "table name")

	command.MarkFlagRequired("connect")
	command.MarkFlagRequired("importer")
	command.MarkFlagRequired("meta")
	command.MarkFlagRequired("status")
	command.MarkFlagRequired("db")
	command.MarkFlagRequired("table")

	return command
}

func initRestoreClient(client *restore.Client, flagSet *flag.FlagSet) error {
	importerAddr, err := flagSet.GetString("importer")
	if err != nil {
		return errors.Trace(err)
	}

	err = client.SetImportAddr(importerAddr)
	if err != nil {
		return errors.Trace(err)
	}

	metaPath, err := flagSet.GetString("meta")
	if err != nil {
		return errors.Trace(err)
	}
	metaData, err := ioutil.ReadFile(metaPath)
	if err != nil {
		return errors.Trace(err)
	}
	backupMeta := &backup.BackupMeta{}
	err = proto.Unmarshal(metaData, backupMeta)
	if err != nil {
		return errors.Trace(err)
	}
	partitionSize, err := flagSet.GetInt("partition-size")
	if err != nil {
		return errors.Trace(err)
	}
	err = client.InitBackupMeta(backupMeta, partitionSize)
	if err != nil {
		return errors.Trace(err)
	}

	dsn, err := flagSet.GetString("connect")
	if err != nil {
		return errors.Trace(err)
	}
	client.SetDbDSN(dsn)

	statusAddr, err := flagSet.GetString("status")
	if err != nil {
		return errors.Trace(err)
	}
	client.SetStatusAddr(statusAddr)

	maxOpenEngines, err := flagSet.GetInt("max-open-engines")
	if err != nil {
		return errors.Trace(err)
	}
	client.SetMaxOpenEngines(maxOpenEngines)

	maxImportJobs, err := flagSet.GetInt("max-import-jobs")
	if err != nil {
		return errors.Trace(err)
	}
	client.SetMaxImportJobs(maxImportJobs)

	return nil
}
