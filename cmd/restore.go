package cmd

import (
	"context"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/session"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/utils"
)

// NewRestoreCommand returns a restore subcommand
func NewRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "restore",
		Short: "restore a TiKV cluster from a backup",
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return err
			}
			utils.LogBRInfo()
			utils.LogArguments(c)

			// Do not run stat worker in BR.
			session.DisableStats4Test()
			return nil
		},
	}
	command.AddCommand(
		newFullRestoreCommand(),
		newDbRestoreCommand(),
		newTableRestoreCommand(),
	)

	command.PersistentFlags().Uint("concurrency", 128,
		"The size of thread pool that execute the restore task")
	command.PersistentFlags().Uint64("ratelimit", 0,
		"The rate limit of the restore task, MB/s per node. Set to 0 for unlimited speed.")
	command.PersistentFlags().BoolP("checksum", "", true,
		"Run checksum after restore")
	command.PersistentFlags().BoolP("online", "", false,
		"Whether online when restore")

	command.PersistentFlags().Uint64P("lastbackupts", "", 0, "the last time backup ts")

	return command
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

			mgr, err := GetDefaultMgr()
			if err != nil {
				return err
			}
			defer mgr.Close()

			client, err := restore.NewRestoreClient(
				ctx, mgr.GetPDClient(), mgr.GetTiKV())
			if err != nil {
				return errors.Trace(err)
			}
			defer client.Close()
			err = initRestoreClient(client, cmd.Flags())
			if err != nil {
				return errors.Trace(err)
			}

			lastBackupTS, err := cmd.Flags().GetUint64("lastbackupts")
			if err != nil {
				return err
			}

			files := make([]*backup.File, 0)
			tables := make([]*utils.Table, 0)
			for _, db := range client.GetDatabases() {
				err = client.CreateDatabase(db.Schema)
				if err != nil {
					return errors.Trace(err)
				}
				for _, table := range db.Tables {
					files = append(files, table.Files...)
				}
				tables = append(tables, db.Tables...)
			}

			rewriteRules, newTables, err := client.CreateTables(mgr.GetDomain(), tables)
			if err != nil {
				return errors.Trace(err)
			}
			ranges, err := restore.ValidateFileRanges(files, rewriteRules)
			if err != nil {
				return err
			}

			// Redirect to log if there is no log file to avoid unreadable output.
			updateCh := utils.StartProgress(
				ctx,
				"Full Restore",
				// Split/Scatter + Download/Ingest
				int64(len(ranges)+len(files)),
				!HasLogFile())

			err = restore.SplitRanges(ctx, client, ranges, rewriteRules, updateCh)
			if err != nil {
				log.Error("split regions failed", zap.Error(err))
				return errors.Trace(err)
			}
			pdAddrs := strings.Split(pdAddr, ",")
			err = client.ResetTS(pdAddrs)
			if err != nil {
				log.Error("reset pd TS failed", zap.Error(err))
				return errors.Trace(err)
			}

			err = client.SwitchToImportModeIfOffline(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			err = client.RestoreAll(rewriteRules, lastBackupTS, updateCh)
			if err != nil {
				return errors.Trace(err)
			}

			err = client.SwitchToNormalModeIfOffline(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			// Restore has finished.
			close(updateCh)

			// Checksum
			updateCh = utils.StartProgress(
				ctx, "Checksum", int64(len(newTables)), !HasLogFile())
			err = client.ValidateChecksum(
				ctx, mgr.GetTiKV().GetClient(), tables, newTables, updateCh)
			if err != nil {
				return err
			}
			close(updateCh)

			return nil
		},
	}
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

			mgr, err := GetDefaultMgr()
			if err != nil {
				return err
			}
			defer mgr.Close()

			client, err := restore.NewRestoreClient(
				ctx, mgr.GetPDClient(), mgr.GetTiKV())
			if err != nil {
				return errors.Trace(err)
			}
			defer client.Close()
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
			err = client.CreateDatabase(db.Schema)
			if err != nil {
				return errors.Trace(err)
			}

			lastBackupTS, err := cmd.Flags().GetUint64("lastbackupts")
			if err != nil {
				return err
			}

			rewriteRules, newTables, err := client.CreateTables(mgr.GetDomain(), db.Tables)
			if err != nil {
				return errors.Trace(err)
			}
			files := make([]*backup.File, 0)
			for _, table := range db.Tables {
				files = append(files, table.Files...)
			}
			ranges, err := restore.ValidateFileRanges(files, rewriteRules)
			if err != nil {
				return err
			}
			// Redirect to log if there is no log file to avoid unreadable output.
			updateCh := utils.StartProgress(
				ctx,
				"Database Restore",
				// Split/Scatter + Download/Ingest
				int64(len(ranges)+len(files)),
				!HasLogFile())

			err = restore.SplitRanges(ctx, client, ranges, rewriteRules, updateCh)
			if err != nil {
				log.Error("split regions failed", zap.Error(err))
				return errors.Trace(err)
			}
			pdAddrs := strings.Split(pdAddr, ",")
			err = client.ResetTS(pdAddrs)
			if err != nil {
				log.Error("reset pd TS failed", zap.Error(err))
				return errors.Trace(err)
			}

			err = client.SwitchToImportModeIfOffline(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			err = client.RestoreDatabase(
				db, rewriteRules, lastBackupTS, updateCh)
			if err != nil {
				return errors.Trace(err)
			}

			err = client.SwitchToNormalModeIfOffline(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			// Checksum
			updateCh = utils.StartProgress(
				ctx, "Checksum", int64(len(newTables)), !HasLogFile())
			err = client.ValidateChecksum(
				ctx, mgr.GetTiKV().GetClient(), db.Tables, newTables, updateCh)
			if err != nil {
				return err
			}
			close(updateCh)
			return nil
		},
	}
	command.Flags().String("db", "", "database name")

	if err := command.MarkFlagRequired("db"); err != nil {
		panic(err)
	}

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

			mgr, err := GetDefaultMgr()
			if err != nil {
				return err
			}
			defer mgr.Close()

			client, err := restore.NewRestoreClient(
				ctx, mgr.GetPDClient(), mgr.GetTiKV())
			if err != nil {
				return errors.Trace(err)
			}
			defer client.Close()
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
			err = client.CreateDatabase(db.Schema)
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
			rewriteRules, newTables, err := client.CreateTables(mgr.GetDomain(), []*utils.Table{table})
			if err != nil {
				return errors.Trace(err)
			}

			ranges, err := restore.ValidateFileRanges(table.Files, rewriteRules)
			if err != nil {
				return err
			}

			lastBackupTS, err := cmd.Flags().GetUint64("lastbackupts")
			if err != nil {
				return err
			}

			// Redirect to log if there is no log file to avoid unreadable output.
			updateCh := utils.StartProgress(
				ctx,
				"Table Restore",
				// Split/Scatter + Download/Ingest
				int64(len(ranges)+len(table.Files)),
				!HasLogFile())

			err = restore.SplitRanges(ctx, client, ranges, rewriteRules, updateCh)
			if err != nil {
				log.Error("split regions failed", zap.Error(err))
				return errors.Trace(err)
			}
			pdAddrs := strings.Split(pdAddr, ",")
			err = client.ResetTS(pdAddrs)
			if err != nil {
				log.Error("reset pd TS failed", zap.Error(err))
				return errors.Trace(err)
			}
			err = client.SwitchToImportModeIfOffline(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			err = client.RestoreTable(table, rewriteRules, lastBackupTS, updateCh)
			if err != nil {
				return errors.Trace(err)
			}
			err = client.SwitchToNormalModeIfOffline(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			// Restore has finished.
			close(updateCh)

			// Checksum
			updateCh = utils.StartProgress(
				ctx, "Checksum", int64(len(newTables)), !HasLogFile())
			err = client.ValidateChecksum(
				ctx, mgr.GetTiKV().GetClient(), []*utils.Table{table}, newTables, updateCh)
			if err != nil {
				return err
			}
			close(updateCh)

			return nil
		},
	}

	command.Flags().String("db", "", "database name")
	command.Flags().String("table", "", "table name")

	if err := command.MarkFlagRequired("db"); err != nil {
		panic(err)
	}
	if err := command.MarkFlagRequired("table"); err != nil {
		panic(err)
	}

	return command
}

func initRestoreClient(client *restore.Client, flagSet *flag.FlagSet) error {
	u, err := storage.ParseBackendFromFlags(flagSet, FlagStorage)
	if err != nil {
		return err
	}
	rateLimit, err := flagSet.GetUint64("ratelimit")
	if err != nil {
		return err
	}
	client.SetRateLimit(rateLimit * utils.MB)
	s, err := storage.Create(u)
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
	err = client.InitBackupMeta(backupMeta, u)
	if err != nil {
		return errors.Trace(err)
	}

	concurrency, err := flagSet.GetUint("concurrency")
	if err != nil {
		return err
	}
	client.SetConcurrency(concurrency)

	isOnline, err := flagSet.GetBool("online")
	if err != nil {
		return err
	}
	if isOnline {
		client.EnableOnline()
	}

	return nil
}
