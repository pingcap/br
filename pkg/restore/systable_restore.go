// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.
package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/logutil"
	"github.com/pingcap/br/pkg/utils"
)

// RestoreSystemSchemas restores the system schema(i.e. the `mysql` schema).
// Detail see https://github.com/pingcap/br/issues/679#issuecomment-762592254.
func (rc *Client) RestoreSystemSchemas(ctx context.Context, f filter.Filter) {
	sysDB := mysql.SystemDB

	temporaryDB := utils.TemporaryDBName(sysDB)
	defer rc.cleanTemporaryDatabase(ctx, sysDB)

	if !f.MatchSchema(temporaryDB.O) {
		log.Debug("system database filtered out", zap.String("database", sysDB))
		return
	}
	originDatabase, ok := rc.databases[temporaryDB.O]
	if !ok {
		log.Info("system database not backed up, skipping", zap.String("database", sysDB))
		return
	}
	db, ok := rc.getDatabaseByName(sysDB)
	if !ok {
		// Or should we create the database here?
		log.Warn("target database not exist, aborting", zap.String("database", sysDB))
		return
	}

	tablesRestored := make([]string, 0, len(originDatabase.Tables))
	for _, table := range originDatabase.Tables {
		tableName := table.Info.Name
		if f.MatchTable(sysDB, tableName.O) {
			rc.replaceTemporaryTableToSystable(ctx, tableName.L, db)
		}
		tablesRestored = append(tablesRestored, tableName.L)
	}
	if err := rc.afterSystemTablesReplaced(ctx, tablesRestored); err != nil {
		for _, e := range multierr.Errors(err) {
			logutil.WarnTerm("error during winding up the restoration of system table", zap.String("database", sysDB), logutil.ShortError(e))
		}
	}
}

// database is a record of a database.
// For fast querying whether a table exists and the temporary database of it.
type database struct {
	ExistingTables map[string]*model.TableInfo
	Name           model.CIStr
	TemporaryName  model.CIStr
}

// getDatabaseByName make a record of a database from info schema by its name.
func (rc *Client) getDatabaseByName(name string) (*database, bool) {
	infoSchema := rc.dom.InfoSchema()
	schema, ok := infoSchema.SchemaByName(model.NewCIStr(name))
	if !ok {
		return nil, false
	}
	db := &database{
		ExistingTables: map[string]*model.TableInfo{},
		Name:           model.NewCIStr(name),
		TemporaryName:  utils.TemporaryDBName(name),
	}
	for _, t := range schema.Tables {
		db.ExistingTables[t.Name.L] = t
	}
	return db, true
}

// afterSystemTablesReplaced do some extra work for special system tables.
// e.g. after inserting to the table mysql.user, we must execute `FLUSH PRIVILEGES` to allow it take effect.
func (rc *Client) afterSystemTablesReplaced(ctx context.Context, tables []string) error {
	needFlushStat := false
	var err error
	for _, table := range tables {
		switch {
		case table == "user":
			// We cannot execute `rc.dom.NotifyUpdatePrivilege` here, because there isn't
			// sessionctx.Context provided by the glue.
			// TODO: update the glue type and allow we retrive a session context from it.
			err = multierr.Append(err, errors.Annotatef(berrors.ErrUnsupportedSystemTable,
				"restored user info may not take effect, until you should execute `FLUSH PRIVILEGES` manually"))
		case strings.HasPrefix(table, "stats_"):
			needFlushStat = true
		}
	}
	if needFlushStat {
		// The newly created tables have different table IDs with original tables,
		// 	hence the old statistics are invalid.
		//
		// TODO:
		// 	Plan A) rewrite the IDs in the `rc.statsHandler.Update(rc.dom.InfoSchema())`.
		//  Plan B) give the user a SQL to let him update it manually.
		//  If plan A applied, we can deprecate the origin interface for backing up statistics.
		err = multierr.Append(err, errors.Annotatef(berrors.ErrUnsupportedSystemTable,
			"table ID has been changed, old statistics would not be valid anymore"))
	}
	return err
}

// replaceTemporaryTableToSystable replaces the temporary table to real system table.
func (rc *Client) replaceTemporaryTableToSystable(ctx context.Context, tableName string, db *database) {
	execSQL := func(sql string) {
		if err := rc.db.se.Execute(ctx, sql); err != nil {
			logutil.WarnTerm("failed to restore system table",
				logutil.ShortError(err),
				zap.String("table", tableName),
				zap.Stringer("database", db.Name),
				zap.String("sql", sql))
		} else {
			log.Info("successfully restore system database",
				zap.String("table", tableName),
				zap.Stringer("database", db.Name),
				zap.String("sql", sql),
			)
		}
	}
	if db.ExistingTables[tableName] != nil {
		log.Info("table existing, using replace into for restore",
			zap.String("table", tableName),
			zap.Stringer("schema", db.Name))
		replaceIntoSQL := fmt.Sprintf("REPLACE INTO %s SELECT * FROM %s;",
			utils.EncloseDBAndTable(db.Name.L, tableName),
			utils.EncloseDBAndTable(db.TemporaryName.L, tableName))
		execSQL(replaceIntoSQL)
		return
	}
	renameSQL := fmt.Sprintf("RENAME TABLE %s TO %s;",
		utils.EncloseDBAndTable(db.TemporaryName.L, tableName),
		utils.EncloseDBAndTable(db.Name.L, tableName),
	)
	execSQL(renameSQL)
}

func (rc *Client) cleanTemporaryDatabase(ctx context.Context, originDB string) {
	database := utils.TemporaryDBName(originDB)
	log.Debug("dropping temporary database", zap.Stringer("database", database))
	sql := fmt.Sprintf("DROP DATABASE IF EXISTS %s", utils.EncloseName(database.L))
	if err := rc.db.se.Execute(ctx, sql); err != nil {
		logutil.WarnTerm("failed to drop temporary database, it should be dropped manually",
			zap.Stringer("database", database),
			logutil.ShortError(err),
		)
	}
}
