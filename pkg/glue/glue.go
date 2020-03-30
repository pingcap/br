// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package glue

import (
	"context"

	"github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/v3/client"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
)

// Glue is an abstraction of TiDB function calls used in BR.
type Glue interface {
	GetDomain(store kv.Storage) (*domain.Domain, error)
	CreateSession(store kv.Storage) (Session, error)
	Open(path string, option pd.SecurityOption) (kv.Storage, error)

	// OwnsStorage returns whether the storage returned by Open() is owned
	// If this method returns false, the connection manager will never close the storage.
	OwnsStorage() bool

	StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) Progress
}

// Session is an abstraction of the session.Session interface.
type Session interface {
	Execute(ctx context.Context, sql string) error
	ShowCreateDatabase(schema *model.DBInfo) (string, error)
	ShowCreateTable(table *model.TableInfo, allocator autoid.Allocator) (string, error)
	Close()
}

// Progress is an interface recording the current execution progress.
type Progress interface {
	// Inc increases the progress. This method must be goroutine-safe, and can
	// be called from any goroutine.
	Inc()
	// Close marks the progress as 100% complete and that Inc() can no longer be
	// called.
	Close()
}
