package cmd

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	restore_util "github.com/pingcap/tidb-tools/pkg/restore-util"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
)

// TODO: better connect, remove fast checksum, clone rewrite rule, error

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

			tableRules := make([]*import_sstpb.RewriteRule, 0)
			dataRules := make([]*import_sstpb.RewriteRule, 0)
			files := make([]*backup.File, 0)
			for _, db := range client.GetDatabases() {
				err = restore.CreateDatabase(db.Schema, client.GetDbDSN())
				if err != nil {
					return errors.Trace(err)
				}
				var rules *restore_util.RewriteRules
				rules, err = client.CreateTables(db.Tables)
				if err != nil {
					return errors.Trace(err)
				}
				tableRules = append(tableRules, rules.Table...)
				dataRules = append(dataRules, rules.Data...)
				for _, table := range db.Tables {
					files = append(files, table.Files...)
				}
			}

			splitter := restore_util.NewRegionSplitter(restore_util.NewClient(client.GetPDClient()))
			rewriteRules := &restore_util.RewriteRules{
				Table: tableRules,
				Data:  dataRules,
			}
			rewriteRulesRaw := cloneRule(rewriteRules)
			err = splitter.Split(ctx, restore.GetRanges(files), rewriteRules)
			if err != nil {
				return errors.Trace(err)
			}
			restoreTS, err := client.GetTS()
			if err != nil {
				return errors.Trace(err)
			}

			err = client.SwitchToImportMode(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			err = client.RestoreAll(rewriteRules, restoreTS)
			if err != nil {
				return errors.Trace(err)
			}

			err = client.SwitchToNormalMode(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			success, err := client.ValidateChecksum(rewriteRulesRaw)
			println("validateChecksum: ", success)
			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)/")

	command.MarkFlagRequired("connect")
	command.MarkFlagRequired("importer")

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

			dbName, err := cmd.Flags().GetString("db")
			if err != nil {
				return errors.Trace(err)
			}
			db := client.GetDatabase(dbName)
			if db == nil {
				return errors.New("not exists database")
			}
			err = restore.CreateDatabase(db.Schema, client.GetDbDSN())
			if err != nil {
				return errors.Trace(err)
			}

			rewriteRules, err := client.CreateTables(db.Tables)
			if err != nil {
				return errors.Trace(err)
			}
			rewriteRulesRaw := cloneRule(rewriteRules)
			files := make([]*backup.File, 0)
			for _, table := range db.Tables {
				files = append(files, table.Files...)
			}
			splitter := restore_util.NewRegionSplitter(restore_util.NewClient(client.GetPDClient()))
			err = splitter.Split(ctx, restore.GetRanges(files), rewriteRules)
			if err != nil {
				return errors.Trace(err)
			}
			restoreTS, err := client.GetTS()
			if err != nil {
				return errors.Trace(err)
			}

			err = client.SwitchToImportMode(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			err = client.RestoreDatabase(db, rewriteRules, restoreTS)
			if err != nil {
				return errors.Trace(err)
			}

			err = client.SwitchToNormalMode(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			success, err := client.ValidateChecksum(rewriteRulesRaw)
			println("validateChecksum: ", success)

			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)/")

	command.Flags().String("db", "", "database name")

	command.MarkFlagRequired("connect")
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

			dbName, err := cmd.Flags().GetString("db")
			if err != nil {
				return errors.Trace(err)
			}
			db := client.GetDatabase(dbName)
			if db == nil {
				return errors.New("not exists database")
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
			// The rules here is raw key.
			rewriteRules, err := client.CreateTable(table)
			if err != nil {
				return errors.Trace(err)
			}
			rewriteRulesRaw := cloneRule(rewriteRules)

			splitter := restore_util.NewRegionSplitter(restore_util.NewClient(client.GetPDClient()))
			err = splitter.Split(ctx, restore.GetRanges(table.Files), rewriteRules)
			if err != nil {
				return errors.Trace(err)
			}
			restoreTS, err := client.GetTS()
			if err != nil {
				return errors.Trace(err)
			}
			err = client.SwitchToImportMode(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			err = client.RestoreTable(table, rewriteRules, restoreTS)
			if err != nil {
				return errors.Trace(err)
			}
			err = client.SwitchToNormalMode(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			success, err := client.ValidateChecksum(rewriteRulesRaw)
			println("validateChecksum: ", success)

			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)/")

	command.Flags().String("db", "", "database name")
	command.Flags().String("table", "", "table name")

	command.MarkFlagRequired("connect")
	command.MarkFlagRequired("db")
	command.MarkFlagRequired("table")

	return command
}

func initRestoreClient(client *restore.Client, flagSet *flag.FlagSet) error {
	u, err := flagSet.GetString(FlagStorage)
	if err != nil {
		return errors.Trace(err)
	}
	s, err := utils.CreateStorage(u)
	if err != nil {
		return errors.Trace(err)
	}
	metaData, err := s.Read(utils.MetaFile)
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

func cloneRule(r *restore_util.RewriteRules) restore_util.RewriteRules {
	var Table []*import_sstpb.RewriteRule
	var Data []*import_sstpb.RewriteRule
	for _, t := range r.Table {
		Table = append(Table, &*t)
	}
	for _, d := range r.Data {
		Data = append(Data, &*d)
	}
	return restore_util.RewriteRules{
		Table: Table,
		Data:  Data,
	}
}
