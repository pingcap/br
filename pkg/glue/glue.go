package glue

import (
	"context"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
)

// Glue is an abstraction of TiDB function calls used in BR.
type Glue interface {
	BootstrapSession(store kv.Storage) (*domain.Domain, error)
	CreateSession(store kv.Storage) (Session, error)
}

// Session is an abstraction of the session.Session interface.
type Session interface {
	Execute(ctx context.Context, sql string) error
	ShowCreateDatabase(schema *model.DBInfo) (string, error)
	ShowCreateTable(table *model.TableInfo, allocator autoid.Allocator) (string, error)
	Close()
}
