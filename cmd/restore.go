package cmd

import (
	"context"
	"fmt"
	"io/ioutil"

	restore_util "github.com/5kbpers/tidb-tools/pkg/restore-util"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
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

			rules := make([]*import_sstpb.RewriteRule, 0)
			ranges := make([]restore_util.Range, 0)
			for _, db := range client.GetDatabases() {
				dbRules, err := client.CreateTables(db.Tables, restoreTS)
				if err != nil {
					return errors.Trace(err)
				}
				rules = append(rules, dbRules...)
				for _, table := range db.Tables {
					ranges = append(ranges, restore.GetRanges(table.Files)...)
				}
			}

			splitter := restore_util.NewRegionSplitter(restore_util.NewClient(client.GetPDClient()))
			err = splitter.Split(ctx, ranges, rules)
			if err != nil {
				return errors.Trace(err)
			}
			err = client.RestoreAll(rules, restoreTS)
			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)")
	command.Flags().String("meta", "", "meta file location")

	command.MarkFlagRequired("connect")
	command.MarkFlagRequired("importer")
	command.MarkFlagRequired("meta")

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

			rules, err := client.CreateTables(db.Tables, restoreTS)
			if err != nil {
				return errors.Trace(err)
			}
			ranges := make([]restore_util.Range, 0)
			for _, table := range db.Tables {
				ranges = append(ranges, restore.GetRanges(table.Files)...)
			}
			splitter := restore_util.NewRegionSplitter(restore_util.NewClient(client.GetPDClient()))
			err = splitter.Split(ctx, ranges, rules)
			if err != nil {
				return errors.Trace(err)
			}
			err = client.RestoreDatabase(db, rules, restoreTS)
			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)/")
	command.Flags().String("meta", "", "meta file location")

	command.Flags().String("db", "", "database name")

	command.MarkFlagRequired("connect")
	command.MarkFlagRequired("meta")
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
				return errors.New("not exists table")
			}
			rules, err := client.CreateTable(table, restoreTS)
			if err != nil {
				return errors.Trace(err)
			}

			splitter := restore_util.NewRegionSplitter(restore_util.NewClient(client.GetPDClient()))
			err = splitter.Split(ctx, restore.GetRanges(table.Files), rules)
			if err != nil {
				return errors.Trace(err)
			}
			err = client.RestoreTable(table, rules, restoreTS)
			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)/")
	command.Flags().String("meta", "", "meta file location")

	command.Flags().String("db", "", "database name")
	command.Flags().String("table", "", "table name")

	command.MarkFlagRequired("connect")
	command.MarkFlagRequired("meta")
	command.MarkFlagRequired("db")
	command.MarkFlagRequired("table")

	return command
}

func initRestoreClient(client *restore.Client, flagSet *flag.FlagSet) error {
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
	err = client.InitBackupMeta(backupMeta)
	if err != nil {
		return errors.Trace(err)
	}

	dsn, err := flagSet.GetString("connect")
	if err != nil {
		return errors.Trace(err)
	}
	client.SetDbDSN(dsn)

	return nil
}
