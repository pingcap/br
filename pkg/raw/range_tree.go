package raw

import (
	"bytes"

	"github.com/google/btree"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Range represents a backup response.
type Range struct {
	StartKey []byte
	EndKey   []byte
	Files    []*backup.File
	Error    *backup.Error
}

func (rg *Range) intersect(
	start, end []byte,
) (subStart, subEnd []byte, isIntersect bool) {
	// empty mean the max end key
	if len(rg.EndKey) != 0 && bytes.Compare(start, rg.EndKey) >= 0 {
		isIntersect = false
		return
	}
	if len(end) != 0 && bytes.Compare(end, rg.StartKey) <= 0 {
		isIntersect = false
		return
	}
	isIntersect = true
	if bytes.Compare(start, rg.StartKey) >= 0 {
		subStart = start
	} else {
		subStart = rg.StartKey
	}
	if len(end) == 0 {
		subEnd = rg.EndKey
	} else if len(rg.EndKey) == 0 {
		subEnd = end
	} else if bytes.Compare(end, rg.EndKey) < 0 {
		subEnd = end
	} else {
		subEnd = rg.EndKey
	}
	return
}

// contains check if the range contains the given key, [start, end)
func (rg *Range) contains(key []byte) bool {
	start, end := rg.StartKey, rg.EndKey
	return bytes.Compare(key, start) >= 0 &&
		(len(end) == 0 || bytes.Compare(key, end) < 0)
}

// Less impls btree.Item
func (rg *Range) Less(than btree.Item) bool {
	// rg.StartKey < than.StartKey
	ta := than.(*Range)
	return bytes.Compare(rg.StartKey, ta.StartKey) < 0
}

var _ btree.Item = &Range{}

// RangeTree is the result of a backup task
type RangeTree struct {
	tree *btree.BTree
}

func newRangeTree() RangeTree {
	return RangeTree{
		tree: btree.New(32),
	}
}

func (rangeTree *RangeTree) len() int {
	return rangeTree.tree.Len()
}

// find is a helper function to find an item that contains the range start
// key.
func (rangeTree *RangeTree) find(rg *Range) *Range {
	var ret *Range
	rangeTree.tree.DescendLessOrEqual(rg, func(i btree.Item) bool {
		ret = i.(*Range)
		return false
	})

	if ret == nil || !ret.contains(rg.StartKey) {
		return nil
	}

	return ret
}

// getOverlaps gets the ranges which are overlapped with the specified range range.
func (rangeTree *RangeTree) getOverlaps(rg *Range) []*Range {
	// note that find() gets the last item that is less or equal than the range.
	// in the case: |_______a_______|_____b_____|___c___|
	// new range is     |______d______|
	// find() will return Range of range_a
	// and both startKey of range_a and range_b are less than endKey of range_d,
	// thus they are regarded as overlapped ranges.
	found := rangeTree.find(rg)
	if found == nil {
		found = rg
	}

	var overlaps []*Range
	rangeTree.tree.AscendGreaterOrEqual(found, func(i btree.Item) bool {
		over := i.(*Range)
		if len(rg.EndKey) > 0 && bytes.Compare(rg.EndKey, over.StartKey) <= 0 {
			return false
		}
		overlaps = append(overlaps, over)
		return true
	})
	return overlaps
}

func (rangeTree *RangeTree) update(rg *Range) {
	overlaps := rangeTree.getOverlaps(rg)
	// Range has backuped, overwrite overlapping range.
	for _, item := range overlaps {
		log.Info("delete overlapping range",
			zap.Binary("StartKey", item.StartKey),
			zap.Binary("EndKey", item.EndKey),
		)
		rangeTree.tree.Delete(item)
	}
	rangeTree.tree.ReplaceOrInsert(rg)
	return
}

func (rangeTree *RangeTree) putErr(
	startKey, endKey []byte, err *backup.Error,
) {
	rg := &Range{
		StartKey: startKey,
		EndKey:   endKey,
		Error:    err,
	}
	rangeTree.update(rg)
}

func (rangeTree *RangeTree) putOk(
	startKey, endKey []byte, files []*backup.File,
) {
	rg := &Range{
		StartKey: startKey,
		EndKey:   endKey,
		Files:    files,
	}
	rangeTree.update(rg)
}

func (rangeTree *RangeTree) getIncompleteRange(
	startKey, endKey []byte,
) []Range {
	if len(startKey) != 0 && bytes.Compare(startKey, endKey) == 0 {
		return []Range{}
	}
	incomplete := make([]Range, 0, 64)
	requsetRange := Range{StartKey: startKey, EndKey: endKey}
	lastEndKey := startKey
	pviot := &Range{StartKey: startKey}
	if first := rangeTree.find(pviot); first != nil {
		pviot.StartKey = first.StartKey
	}
	rangeTree.tree.AscendGreaterOrEqual(pviot, func(i btree.Item) bool {
		rg := i.(*Range)
		if bytes.Compare(lastEndKey, rg.StartKey) < 0 {
			start, end, isIntersect :=
				requsetRange.intersect(lastEndKey, rg.StartKey)
			if isIntersect {
				// There is a gap between the last item and the current item.
				incomplete =
					append(incomplete, Range{StartKey: start, EndKey: end})
			}
		}
		lastEndKey = rg.EndKey
		return len(endKey) == 0 || bytes.Compare(rg.EndKey, endKey) < 0
	})

	// Check whether we need append the last range
	if !bytes.Equal(lastEndKey, endKey) && len(lastEndKey) != 0 &&
		(len(endKey) == 0 || bytes.Compare(lastEndKey, endKey) < 0) {
		start, end, isIntersect := requsetRange.intersect(lastEndKey, endKey)
		if isIntersect {
			incomplete =
				append(incomplete, Range{StartKey: start, EndKey: end})
		}
	}
	return incomplete
}
