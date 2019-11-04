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

// NewRestoreCommand returns a restore subcommand
func NewRestoreCommand() *cobra.Command {
	bp := &cobra.Command{
		Use:   "restore",
		Short: "restore a TiKV cluster from a backup",
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return err
			}
			utils.LogBRInfo()
			return nil
		},
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
			batchSize := 64
			progress := utils.NewProgressPrinter(
				"Full Restore",
				// Split/Scatter + Download/Ingest
				int64(len(files)*2),
			)
			progress.GoPrintProgress(ctx)
			updateCh := progress.UpdateCh()

			rewriteRules := &restore_util.RewriteRules{
				Table: tableRules,
				Data:  dataRules,
			}
			err = client.SplitRanges(files, rewriteRules, batchSize, updateCh)
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

			err = client.RestoreAll(
				rewriteRules, restoreTS, progress.UpdateCh())
			if err != nil {
				return errors.Trace(err)
			}

			err = client.SwitchToNormalMode(ctx)

			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)/")
	command.Flags().Uint("concurrency", 128, "The size of thread pool that execute the restore task")

	command.MarkFlagRequired("connect")

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
			files := make([]*backup.File, 0)
			for _, table := range db.Tables {
				files = append(files, table.Files...)
			}
			batchSize := 64
			progress := utils.NewProgressPrinter(
				"Database Restore",
				// Split/Scatter + Download/Ingest
				int64(len(files)*2),
			)
			progress.GoPrintProgress(ctx)
			updateCh := progress.UpdateCh()

			err = client.SplitRanges(files, rewriteRules, batchSize, updateCh)
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

			err = client.RestoreDatabase(
				db, rewriteRules, restoreTS, updateCh)
			if err != nil {
				return errors.Trace(err)
			}

			err = client.SwitchToNormalMode(ctx)

			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)/")
	command.Flags().Uint("concurrency", 128, "The size of thread pool that execute the restore task")

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

			batchSize := 64
			progress := utils.NewProgressPrinter(
				"Table Restore",
				// Split/Scatter + Download/Ingest
				int64(len(table.Files)*2),
			)
			progress.GoPrintProgress(ctx)
			updateCh := progress.UpdateCh()

			err = client.SplitRanges(table.Files, rewriteRules, batchSize, updateCh)
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

			err = client.RestoreTable(
				table, rewriteRules, restoreTS, progress.UpdateCh())
			if err != nil {
				return errors.Trace(err)
			}
			err = client.SwitchToNormalMode(ctx)
			return errors.Trace(err)
		},
	}

	command.Flags().String("connect", "", "the address to connect tidb, format: username:password@protocol(address)/")
	command.Flags().Uint("concurrency", 128, "The size of thread pool that execute the restore task")

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
		return err
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
		return err
	}
	client.SetDbDSN(dsn)

	concurrency, err := flagSet.GetUint("concurrency")
	if err != nil {
		return err
	}
	client.SetConcurrency(concurrency)

	return nil
}
