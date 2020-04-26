package restore

import (
	"github.com/pingcap/parser/model"
)

// CreatedTable is a table is created on restore process,
// but not yet filled by data.
type CreatedTable struct {
	RewriteRule *RewriteRules
	Table *model.TableInfo
}