package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/utils"
)

// DB is a TiDB instance, not thread-safe.
type DB struct {
	se glue.Session
}

// NewDB returns a new DB
func NewDB(g glue.Glue, store kv.Storage) (*DB, error) {
	se, err := g.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Set SQL mode to None for avoiding SQL compatibility problem
	err = se.Execute(context.Background(), "set @@sql_mode=''")
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &DB{
		se: se,
	}, nil
}

// CreateDatabase executes a CREATE DATABASE SQL.
func (db *DB) CreateDatabase(ctx context.Context, schema *model.DBInfo) error {
	createSQL, err := db.se.ShowCreateDatabase(schema)
	if err != nil {
		log.Error("build create database SQL failed", zap.Stringer("db", schema.Name), zap.Error(err))
		return errors.Trace(err)
	}
	err = db.se.Execute(ctx, createSQL)
	if err != nil {
		log.Error("create database failed", zap.String("SQL", createSQL), zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// CreateTable executes a CREATE TABLE SQL.
func (db *DB) CreateTable(ctx context.Context, table *utils.Table) error {
	schema := table.Schema
	createSQL, err := db.se.ShowCreateTable(schema, newIDAllocator(schema.AutoIncID))
	if err != nil {
		log.Error(
			"build create table SQL failed",
			zap.Stringer("db", table.Db.Name),
			zap.Stringer("table", schema.Name),
			zap.Error(err))
		return errors.Trace(err)
	}
	switchDbSQL := fmt.Sprintf("use %s;", utils.EncloseName(table.Db.Name.O))
	err = db.se.Execute(ctx, switchDbSQL)
	if err != nil {
		log.Error("switch db failed",
			zap.String("SQL", switchDbSQL),
			zap.Stringer("db", table.Db.Name),
			zap.Error(err))
		return errors.Trace(err)
	}
	// Insert `IF NOT EXISTS` statement to skip the created tables
	words := strings.SplitN(createSQL, " ", 3)
	if len(words) > 2 && strings.ToUpper(words[0]) == "CREATE" && strings.ToUpper(words[1]) == "TABLE" {
		createSQL = "CREATE TABLE IF NOT EXISTS " + words[2]
	}
	err = db.se.Execute(ctx, createSQL)
	if err != nil {
		log.Error("create table failed",
			zap.String("SQL", createSQL),
			zap.Stringer("db", table.Db.Name),
			zap.Stringer("table", table.Schema.Name),
			zap.Error(err))
		return errors.Trace(err)
	}
	alterAutoIncIDSQL := fmt.Sprintf(
		"alter table %s auto_increment = %d",
		utils.EncloseName(schema.Name.O),
		schema.AutoIncID)
	err = db.se.Execute(ctx, alterAutoIncIDSQL)
	if err != nil {
		log.Error("alter AutoIncID failed",
			zap.String("SQL", alterAutoIncIDSQL),
			zap.Stringer("db", table.Db.Name),
			zap.Stringer("table", table.Schema.Name),
			zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// Close closes the connection
func (db *DB) Close() {
	db.se.Close()
}
