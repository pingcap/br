// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gluetidb

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/gluetikv"
)

const (
	defaultCapOfCreateTable    = 512
	defaultCapOfCreateDatabase = 64
	brComment                  = `/*from(br)*/`
)

// New makes a new tidb glue.
func New() Glue {
	log.Debug("enabling no register config")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.SkipRegisterToDashboard = true
	})
	return Glue{}
}

// Glue is an implementation of glue.Glue using a new TiDB session.
type Glue struct {
	tikvGlue gluetikv.Glue
}

type tidbSession struct {
	se    session.Session
	store kv.Storage
}

// GetDomain implements glue.Glue.
func (Glue) GetDomain(store kv.Storage) (*domain.Domain, error) {
	se, err := session.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	dom, err := session.GetDomain(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// create stats handler for backup and restore.
	err = dom.UpdateTableStatsLoop(se)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return dom, nil
}

// CreateSession implements glue.Glue.
func (Glue) CreateSession(store kv.Storage) (glue.Session, error) {
	se, err := session.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tiSession := &tidbSession{
		se:    se,
		store: store,
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

// GetVersion implements glue.Glue.
func (g Glue) GetVersion() string {
	return g.tikvGlue.GetVersion()
}

// Execute implements glue.Session.
func (gs *tidbSession) Execute(ctx context.Context, sql string) error {
	_, err := gs.se.ExecuteInternal(ctx, sql)
	return errors.Trace(err)
}

// CreateDatabase implements glue.Session.
func (gs *tidbSession) CreateDatabase(ctx context.Context, schema *model.DBInfo) error {
	d := domain.GetDomain(gs.se).DDL()
	query, err := gs.showCreateDatabase(schema)
	if err != nil {
		return errors.Trace(err)
	}
	gs.se.SetValue(sessionctx.QueryString, query)
	schema = schema.Clone()
	if len(schema.Charset) == 0 {
		schema.Charset = mysql.DefaultCharset
	}
	return d.CreateSchemaWithInfo(gs.se, schema, ddl.OnExistIgnore, true)
}

func (gs *tidbSession) generateTableID() (int64, error) {
	var ret []int64
	err := kv.RunInNewTxn(context.Background(), gs.store, true, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		var err error
		ret, err = m.GenGlobalIDs(1)
		return err
	})
	return ret[0], err
}

func (gs *tidbSession) setInfoSchemaDiff(m *meta.Meta, schemaID int64, tableID int64) (int64, error) {
	schemaVersion, err := m.GenSchemaVersion()
	if err != nil {
		return 0, err
	}
	diff := &model.SchemaDiff{
		Version:  schemaVersion,
		Type:     model.ActionCreateTable,
		SchemaID: schemaID,
		TableID:  tableID,
	}
	if err := m.SetSchemaDiff(diff); err != nil {
		return 0, err
	}
	return diff.Version, nil
}

func (gs *tidbSession) waitSchemaDiff(ctx context.Context, target int64) {
	timeStart := time.Now()
	sync := domain.GetDomain(gs.se).DDL().SchemaSyncer()
	if err := sync.OwnerUpdateGlobalVersion(ctx, target); err != nil {
		if errors.Find(err, func(e error) bool { return e == context.DeadlineExceeded }) != nil {
			log.Info("BR wait latest schema version changed (2 * lease time exceed)",
				zap.Int64("ver", target),
				zap.Duration("take time", time.Since(timeStart)))
			return
		}
	}
	if err := sync.OwnerCheckAllVersions(ctx, target); err != nil {
		if errors.Find(err, func(e error) bool { return e == context.DeadlineExceeded }) != nil {
			log.Info("BR wait latest schema version changed (2 * lease time exceed)",
				zap.Int64("ver", target),
				zap.Duration("take time", time.Since(timeStart)))
			return
		}
		sync.NotifyCleanExpiredPaths()
		<-ctx.Done()
	}
	log.Info("BR wait latest schema version changed",
		zap.Int64("ver", target),
		zap.Duration("take time", time.Since(timeStart)))
}

func (gs *tidbSession) createTableViaMeta(m *meta.Meta, table *model.TableInfo, schemaInfo *model.DBInfo) error {
	// TODO partition and check.
	// runWithNewTableID := func() error {

	// 	log.Warn("table id conflict, allocating new table ID",
	// 		zap.Stringer("table", table.Name),
	// 		zap.Stringer("database", schemaInfo.Name),
	// 		zap.Int64("old-id", table.ID),
	// 		zap.Int64("new-id", table.ID))
	// 	table.ID = newID
	// 	return gs.createTableViaMeta(m, table, schemaInfo)
	// }

	// if table.ID == 0 {
	// 	return runWithNewTableID()
	// }
	// }

	newID, err := gs.generateTableID()
	table.ID = newID
	if err != nil {
		return err
	}
	return m.CreateTableAndSetAutoID(schemaInfo.ID, table, table.AutoIncID, table.AutoRandID)
}

// CreateTable implements glue.Session.
func (gs *tidbSession) CreateTable(ctx context.Context, dbName model.CIStr, table *model.TableInfo) error {
	dom := domain.GetDomain(gs.se)
	is := dom.InfoSchema()
	table = table.Clone()
	// Clone() does not clone partitions yet :(
	if table.Partition != nil {
		newPartition := *table.Partition
		newPartition.Definitions = append([]model.PartitionDefinition{}, table.Partition.Definitions...)
		table.Partition = &newPartition
	}
	if table.State != model.StatePublic {
		log.Warn("table backed up with non-public state", zap.Stringer("table", table.Name), zap.Stringer("database", dbName))
		table.State = model.StatePublic
	}

	var version int64
	err := gs.WithMeta(ctx, func(ctx context.Context, m *meta.Meta) (err error) {
		schemaInfo, ok := is.SchemaByName(dbName)
		if !ok {
			return errors.Annotatef(infoschema.ErrDatabaseNotExists, "database %s not exist", dbName)
		}
		if errCreateTable := gs.createTableViaMeta(m, table, schemaInfo); errCreateTable != nil {
			return errCreateTable
		}
		version, err = gs.setInfoSchemaDiff(m, schemaInfo.ID, table.ID)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	cctx, cancel := context.WithTimeout(ctx, dom.DDL().GetLease()*2)
	gs.waitSchemaDiff(cctx, version)
	cancel()
	// TODO only reload in unit tests.
	return dom.Reload()
}

// Close implements glue.Session.
func (gs *tidbSession) Close() {
	gs.se.Close()
}

// WithMeta implements glue.Session.
func (gs *tidbSession) WithMeta(ctx context.Context, cont func(ctx context.Context, m *meta.Meta) error) error {
	return kv.RunInNewTxn(ctx, gs.se.GetStore(), false, func(ctx context.Context, txn kv.Transaction) error {
		return cont(ctx, meta.NewMeta(txn))
	})
}

// showCreateTable shows the result of SHOW CREATE TABLE from a TableInfo.
func (gs *tidbSession) showCreateTable(tbl *model.TableInfo) (string, error) {
	table := tbl.Clone()
	table.AutoIncID = 0
	result := bytes.NewBuffer(make([]byte, 0, defaultCapOfCreateTable))
	// this can never fail.
	_, _ = result.WriteString(brComment)
	if err := executor.ConstructResultOfShowCreateTable(gs.se, tbl, autoid.Allocators{}, result); err != nil {
		return "", errors.Trace(err)
	}
	return result.String(), nil
}

// showCreateDatabase shows the result of SHOW CREATE DATABASE from a dbInfo.
func (gs *tidbSession) showCreateDatabase(db *model.DBInfo) (string, error) {
	result := bytes.NewBuffer(make([]byte, 0, defaultCapOfCreateDatabase))
	// this can never fail.
	_, _ = result.WriteString(brComment)
	if err := executor.ConstructResultOfShowCreateDatabase(gs.se, db, true, result); err != nil {
		return "", errors.Trace(err)
	}
	return result.String(), nil
}
