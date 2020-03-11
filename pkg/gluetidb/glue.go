package gluetidb

import (
	"bytes"
	"context"

	"github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/tikv"

	"github.com/pingcap/br/pkg/glue"
)

// Glue is an implementation of glue.Glue using a new TiDB session.
type Glue struct{}

type tidbSession struct {
	se session.Session
}

// BootstrapSession implements glue.Glue
func (Glue) BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	return session.BootstrapSession(store)
}

// CreateSession implements glue.Glue
func (Glue) CreateSession(store kv.Storage) (glue.Session, error) {
	se, err := session.CreateSession(store)
	if err != nil {
		return nil, err
	}
	return &tidbSession{se: se}, nil
}

// Open implements glue.Glue
func (Glue) Open(path string, option pd.SecurityOption) (kv.Storage, error) {
	if option.CAPath != "" {
		conf := config.GetGlobalConfig()
		conf.Security.ClusterSSLCA = option.CAPath
		conf.Security.ClusterSSLCert = option.CertPath
		conf.Security.ClusterSSLKey = option.KeyPath
		config.StoreGlobalConfig(conf)
	}
	return tikv.Driver{}.Open(path)
}

// OwnsStorage implements glue.Glue
func (Glue) OwnsStorage() bool {
	return true
}

// Execute implements glue.Session
func (gs *tidbSession) Execute(ctx context.Context, sql string) error {
	_, err := gs.se.Execute(ctx, sql)
	return err
}

// ShowCreateDatabase implements glue.Session
func (gs *tidbSession) ShowCreateDatabase(schema *model.DBInfo) (string, error) {
	var buf bytes.Buffer
	if err := executor.ConstructResultOfShowCreateDatabase(gs.se, schema, true, &buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// ShowCreateTable implements glue.Session
func (gs *tidbSession) ShowCreateTable(table *model.TableInfo, allocator autoid.Allocator) (string, error) {
	var buf bytes.Buffer
	if err := executor.ConstructResultOfShowCreateTable(gs.se, table, allocator, &buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// Close implements glue.Session
func (gs *tidbSession) Close() {
	gs.se.Close()
}
