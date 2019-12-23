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

	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

var schedulers = map[string]struct{}{
	"balance-leader-scheduler":     {},
	"balance-hot-region-scheduler": {},
	"balance-region-scheduler":     {},

	"shuffle-leader-scheduler":     {},
	"shuffle-region-scheduler":     {},
	"shuffle-hot-region-scheduler": {},
}

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

			summary.SetUnit(summary.RestoreUnit)
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

	return command
}

func runRestore(flagSet *flag.FlagSet, cmdName, dbName, tableName string) error {
	pdAddr, err := flagSet.GetString(FlagPD)
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
	err = initRestoreClient(ctx, client, flagSet)
	if err != nil {
		return errors.Trace(err)
	}

	files := make([]*backup.File, 0)
	tables := make([]*utils.Table, 0)

	defer summary.Summary(cmdName)

	switch {
	case len(dbName) == 0 && len(tableName) == 0:
		// full restore
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
	case len(dbName) != 0 && len(tableName) == 0:
		// database restore
		db := client.GetDatabase(dbName)
		err = client.CreateDatabase(db.Schema)
		if err != nil {
			return errors.Trace(err)
		}
		for _, table := range db.Tables {
			files = append(files, table.Files...)
		}
		tables = db.Tables
	case len(dbName) != 0 && len(tableName) != 0:
		// table restore
		db := client.GetDatabase(dbName)
		err = client.CreateDatabase(db.Schema)
		if err != nil {
			return errors.Trace(err)
		}
		table := db.GetTable(tableName)
		files = table.Files
		tables = utils.Tables{table}
	default:
		return errors.New("must set db when table was set")
	}

	summary.CollectInt("restore files", len(files))
	rewriteRules, newTables, err := client.CreateTables(mgr.GetDomain(), tables)
	if err != nil {
		return errors.Trace(err)
	}
	ranges, err := restore.ValidateFileRanges(files, rewriteRules)
	if err != nil {
		return err
	}
	summary.CollectInt("restore ranges", len(ranges))

	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := utils.StartProgress(
		ctx,
		cmdName,
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

	removedSchedulers, err := RestorePrepareWork(ctx, client, mgr)
	if err != nil {
		return errors.Trace(err)
	}

	err = client.RestoreAll(rewriteRules, updateCh)
	if err != nil {
		return errors.Trace(err)
	}

	err = RestorePostWork(ctx, client, mgr, removedSchedulers)
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
}

func newFullRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "restore all tables",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestore(cmd.Flags(), "Full Restore", "", "")
		},
	}
	return command
}

func newDbRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "db",
		Short: "restore tables in a database",
		RunE: func(cmd *cobra.Command, _ []string) error {
			db, err := cmd.Flags().GetString(flagDatabase)
			if err != nil {
				return err
			}
			if len(db) == 0 {
				return errors.New("empty database name is not allowed")
			}
			return runRestore(cmd.Flags(), "Database Restore", db, "")
		},
	}
	command.Flags().String(flagDatabase, "", "database name")
	_ = command.MarkFlagRequired(flagDatabase)
	return command
}

func newTableRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "table",
		Short: "restore a table",
		RunE: func(cmd *cobra.Command, _ []string) error {
			db, err := cmd.Flags().GetString(flagDatabase)
			if err != nil {
				return err
			}
			if len(db) == 0 {
				return errors.New("empty database name is not allowed")
			}
			table, err := cmd.Flags().GetString(flagTable)
			if err != nil {
				return err
			}
			if len(table) == 0 {
				return errors.New("empty table name is not allowed")
			}
			return runRestore(cmd.Flags(), "Table Restore", db, table)
		},
	}

	command.Flags().String(flagDatabase, "", "database name")
	command.Flags().String(flagTable, "", "table name")

	_ = command.MarkFlagRequired(flagDatabase)
	_ = command.MarkFlagRequired(flagTable)
	return command
}

func initRestoreClient(ctx context.Context, client *restore.Client, flagSet *flag.FlagSet) error {
	u, err := storage.ParseBackendFromFlags(flagSet, FlagStorage)
	if err != nil {
		return err
	}
	rateLimit, err := flagSet.GetUint64("ratelimit")
	if err != nil {
		return err
	}
	client.SetRateLimit(rateLimit * utils.MB)
	s, err := storage.Create(ctx, u)
	if err != nil {
		return errors.Trace(err)
	}
	metaData, err := s.Read(ctx, utils.MetaFile)
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

// RestorePrepareWork execute some prepare work before restore
func RestorePrepareWork(ctx context.Context, client *restore.Client, mgr *conn.Mgr) ([]string, error) {
	if client.IsOnline() {
		return nil, nil
	}
	err := client.SwitchToImportMode(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	existSchedulers, err := mgr.ListSchedulers(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	needRemoveSchedulers := make([]string, 0, len(existSchedulers))
	for _, s := range existSchedulers {
		if _, ok := schedulers[s]; ok {
			needRemoveSchedulers = append(needRemoveSchedulers, s)
		}
	}
	return removePDLeaderScheduler(ctx, mgr, needRemoveSchedulers)
}

func removePDLeaderScheduler(ctx context.Context, mgr *conn.Mgr, existSchedulers []string) ([]string, error) {
	removedSchedulers := make([]string, 0, len(existSchedulers))
	for _, scheduler := range existSchedulers {
		err := mgr.RemoveScheduler(ctx, scheduler)
		if err != nil {
			return nil, err
		}
		removedSchedulers = append(removedSchedulers, scheduler)
	}
	return removedSchedulers, nil
}

// RestorePostWork execute some post work after restore
func RestorePostWork(ctx context.Context, client *restore.Client, mgr *conn.Mgr, removedSchedulers []string) error {
	if client.IsOnline() {
		return nil
	}
	err := client.SwitchToNormalMode(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	return addPDLeaderScheduler(ctx, mgr, removedSchedulers)
}

func addPDLeaderScheduler(ctx context.Context, mgr *conn.Mgr, removedSchedulers []string) error {
	for _, scheduler := range removedSchedulers {
		err := mgr.AddScheduler(ctx, scheduler)
		if err != nil {
			return err
		}
	}
	return nil
}
