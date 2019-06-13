package txn

import (
	"github.com/overvenus/br/pkg/meta"
)

// DoCheckpoint returns a checkpoint.
func DoCheckpoint(backer *meta.Backer) ([]*meta.RangeMeta, error) {
	return []*meta.RangeMeta{}, nil
}
