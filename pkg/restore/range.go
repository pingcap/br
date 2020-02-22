package restore

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/tablecodec"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/utils/rtree"
)

// sortRanges checks if the range overlapped and sort them
func sortRanges(ranges []rtree.Range, rewriteRules *RewriteRules) ([]rtree.Range, error) {
	rangeTree := rtree.NewRangeTree()
	for _, rg := range ranges {
		if rewriteRules != nil {
			startID := tablecodec.DecodeTableID(rg.StartKey)
			endID := tablecodec.DecodeTableID(rg.EndKey)
			var rule *import_sstpb.RewriteRule
			if startID == endID {
				rg.StartKey, rule = replacePrefix(rg.StartKey, rewriteRules)
				if rule == nil {
					log.Warn("cannot find rewrite rule", zap.Binary("key", rg.StartKey))
				} else {
					log.Debug(
						"rewrite start key",
						zap.Binary("key", rg.StartKey),
						zap.Stringer("rule", rule))
				}
				rg.EndKey, rule = replacePrefix(rg.EndKey, rewriteRules)
				if rule == nil {
					log.Warn("cannot find rewrite rule", zap.Binary("key", rg.EndKey))
				} else {
					log.Debug(
						"rewrite end key",
						zap.Binary("key", rg.EndKey),
						zap.Stringer("rule", rule))
				}
			} else {
				log.Warn("table id does not match",
					zap.Binary("startKey", rg.StartKey),
					zap.Binary("endKey", rg.EndKey),
					zap.Int64("startID", startID),
					zap.Int64("endID", endID))
				return nil, errors.New("table id does not match")
			}
		}
		if out := rangeTree.InsertRange(rg); out != nil {
			return nil, errors.Errorf("ranges overlapped: %s, %s", out, rg)
		}
	}
	sortedRanges := rangeTree.GetSortedRanges()
	return sortedRanges, nil
}

// RegionInfo includes a region and the leader of the region.
type RegionInfo struct {
	Region *metapb.Region
	Leader *metapb.Peer
}

// RewriteRules contains rules for rewriting keys of tables.
type RewriteRules struct {
	Table []*import_sstpb.RewriteRule
	Data  []*import_sstpb.RewriteRule
}
