package raw

import (
	"bytes"

	"github.com/google/btree"
	"github.com/pingcap/kvproto/pkg/backup"
)

// Range represents a backup response.
type Range struct {
	Error    *backup.Error
	StartKey []byte
	EndKey   []byte
	files    []*backup.File
}

// Less impls btree.Item
func (rg *Range) Less(than btree.Item) bool {
	// rg.StartKey < than.StartKey
	ta := than.(*Range)
	return bytes.Compare(rg.StartKey, ta.StartKey) < 0
}

var _ btree.Item = &Range{}

// Result is the result of a backup task
type Result struct {
	ranges *btree.BTree
}

func newResult() Result {
	return Result{
		ranges: btree.New(32),
	}
}

func (result *Result) put(rg *Range) {
	old := result.ranges.ReplaceOrInsert(rg)
	_ = old
}

func (result *Result) putError(
	startKey, endKey []byte, err *backup.Error) {
	rg := &Range{
		StartKey: startKey,
		EndKey:   endKey,
		Error:    err,
	}
	result.put(rg)
}

func (result *Result) putOk(
	startKey, endKey []byte, files []*backup.File) {
	rg := &Range{
		StartKey: startKey,
		EndKey:   endKey,
		files:    files,
	}
	result.put(rg)
}
