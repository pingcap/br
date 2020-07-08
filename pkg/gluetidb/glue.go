// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gluetidb

import (
	"bytes"
	"context"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"

	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/gluetikv"
	"github.com/pingcap/errors"
)

const (
	defaultCapOfCreateTable    = 512
	defaultCapOfCreateDatabase = 64
	brComment                  = "/*from('br')*/"
)

// Glue is an implementation of glue.Glue using a new TiDB session.
type Glue struct {
	tikvGlue gluetikv.Glue
}

type tidbSession struct {
	se           session.Session
	allocs       map[int64]autoid.Allocator
	allocFactory func(dbID int64, allocType uint8, opts ...autoid.AllocOption) autoid.Allocator
}

// GetDomain implements glue.Glue.
func (Glue) GetDomain(store kv.Storage) (*domain.Domain, error) {
	return session.GetDomain(store)
}

// CreateSession implements glue.Glue.
func (Glue) CreateSession(store kv.Storage) (glue.Session, error) {
	se, err := session.CreateSession(store)
	if err != nil {
		return nil, err
	}
	tiSession := &tidbSession{
		se:     se,
		allocs: make(map[int64]autoid.Allocator),
		allocFactory: func(dbID int64, allocType uint8, opts ...autoid.AllocOption) autoid.Allocator {
			return autoid.NewAllocator(store, dbID, false, allocType, opts...)
		},
	}
	return tiSession, nil
}

// Open implements glue.Glue.
func (g Glue) Open(path string, option pd.SecurityOption) (kv.Storage, error) {
	return g.tikvGlue.Open(path, option)
}

// OwnsStorage implements glue.Glue.
func (Glue) OwnsStorage() bool {
	return true
}

// StartProgress implements glue.Glue.
func (g Glue) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	return g.tikvGlue.StartProgress(ctx, cmdName, total, redirectLog)
}

// Record implements glue.Glue.
func (g Glue) Record(name string, value uint64) {
	g.tikvGlue.Record(name, value)
}

// Execute implements glue.Session.
func (gs *tidbSession) Execute(ctx context.Context, sql string) error {
	_, err := gs.se.Execute(ctx, sql)
	return err
}

// CreateDatabase implements glue.Session.
func (gs *tidbSession) CreateDatabase(ctx context.Context, schema *model.DBInfo) error {
	d := domain.GetDomain(gs.se).DDL()
	gs.se.SetValue(sessionctx.QueryString, gs.showCreateDatabase(schema))
	schema = schema.Clone()
	if len(schema.Charset) == 0 {
		schema.Charset = mysql.DefaultCharset
	}
	return d.CreateSchemaWithInfo(gs.se, schema, ddl.OnExistIgnore, true)
}

// CreateTable implements glue.Session.
func (gs *tidbSession) CreateTable(ctx context.Context, dbName model.CIStr, table *model.TableInfo) error {
	d := domain.GetDomain(gs.se).DDL()
	query, err := gs.showCreateTable(table, dbName)
	if err != nil {
		return err
	}
	gs.se.SetValue(sessionctx.QueryString, query)
	// Clone() does not clone partitions yet :(
	table = table.Clone()
	if table.Partition != nil {
		newPartition := *table.Partition
		newPartition.Definitions = append([]model.PartitionDefinition{}, table.Partition.Definitions...)
		table.Partition = &newPartition
	}
	return d.CreateTableWithInfo(gs.se, dbName, table, ddl.OnExistIgnore, true)
}

// Close implements glue.Session.
func (gs *tidbSession) Close() {
	gs.se.Close()
}

func (gs *tidbSession) allocatorFor(tbl *model.TableInfo, dbName model.CIStr) (autoid.Allocator, error) {
	db, ok := domain.GetDomain(gs.se).InfoSchema().SchemaByName(dbName)
	if !ok {
		return nil, errors.Errorf("failed to get schema of table %s", tbl.Name.String())
	}
	if alloc, ok := gs.allocs[db.ID]; ok {
		return alloc, nil
	}
	alloc := gs.allocFactory(db.ID, autoid.AutoIncrementType)
	gs.allocs[db.ID] = alloc
	return alloc, nil
}

// showCreateTable shows the result of SHOW CREATE TABLE from a TableInfo.
func (gs *tidbSession) showCreateTable(tbl *model.TableInfo, dbName model.CIStr) (string, error) {
	table := tbl.Clone()
	table.AutoIncID = 0
	result := bytes.NewBuffer(make([]byte, 0, defaultCapOfCreateTable))
	// this can never fail.
	alloc, err := gs.allocatorFor(tbl, dbName)
	if err != nil {
		return "", err
	}
	_, _ = result.WriteString(brComment)
	executor.ConstructResultOfShowCreateTable(gs.se, tbl, alloc, result)
	return result.String(), nil
}

// showCreateDatabase shows the result of SHOW CREATE DATABASE from a dbInfo.
func (gs *tidbSession) showCreateDatabase(db *model.DBInfo) string {
	result := bytes.NewBuffer(make([]byte, 0, defaultCapOfCreateTable))
	// this can never fail.
	_, _ = result.WriteString(brComment)
	executor.ConstructResultOfShowCreateDatabase(gs.se, db, true, result)
	return result.String()
}
