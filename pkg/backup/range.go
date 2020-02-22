package backup

import (
	"bytes"
	"encoding/hex"

	"github.com/google/btree"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/utils/rtree"
)

func getIncompleteRange(
	rangeTree *rtree.RangeTree, startKey, endKey []byte,
) []rtree.Range {
	if len(startKey) != 0 && bytes.Equal(startKey, endKey) {
		return []rtree.Range{}
	}
	incomplete := make([]rtree.Range, 0, 64)
	requsetRange := rtree.Range{StartKey: startKey, EndKey: endKey}
	lastEndKey := startKey
	pviot := &rtree.Range{StartKey: startKey}
	if first := rangeTree.Find(pviot); first != nil {
		pviot.StartKey = first.StartKey
	}
	rangeTree.AscendGreaterOrEqual(pviot, func(i btree.Item) bool {
		rg := i.(*rtree.Range)
		if bytes.Compare(lastEndKey, rg.StartKey) < 0 {
			start, end, isIntersect :=
				requsetRange.Intersect(lastEndKey, rg.StartKey)
			if isIntersect {
				// There is a gap between the last item and the current item.
				incomplete =
					append(incomplete, rtree.Range{StartKey: start, EndKey: end})
			}
		}
		lastEndKey = rg.EndKey
		return len(endKey) == 0 || bytes.Compare(rg.EndKey, endKey) < 0
	})

	// Check whether we need append the last range
	if !bytes.Equal(lastEndKey, endKey) && len(lastEndKey) != 0 &&
		(len(endKey) == 0 || bytes.Compare(lastEndKey, endKey) < 0) {
		start, end, isIntersect := requsetRange.Intersect(lastEndKey, endKey)
		if isIntersect {
			incomplete =
				append(incomplete, rtree.Range{StartKey: start, EndKey: end})
		}
	}
	return incomplete
}

func checkDupFiles(rangeTree *rtree.RangeTree) {
	// Name -> SHA256
	files := make(map[string][]byte)
	rangeTree.Ascend(func(i btree.Item) bool {
		rg := i.(*rtree.Range)
		for _, f := range rg.Files {
			old, ok := files[f.Name]
			if ok {
				log.Error("dup file",
					zap.String("Name", f.Name),
					zap.String("SHA256_1", hex.EncodeToString(old)),
					zap.String("SHA256_2", hex.EncodeToString(f.Sha256)),
				)
			} else {
				files[f.Name] = f.Sha256
			}
		}
		return true
	})
}
