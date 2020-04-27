// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/parser/model"
)

// CreatedTable is a table is created on restore process,
// but not yet filled by data.
type CreatedTable struct {
	RewriteRule *RewriteRules
	Table       *model.TableInfo
	OldTable    *utils.Table
}

// TableWithRange is a CreatedTable that has been bind to some of key ranges.
type TableWithRange struct {
	CreatedTable

	Range []rtree.Range
}
