// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/tablecodec"
	"go.uber.org/zap"

	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/logutil"
	"github.com/pingcap/br/pkg/rtree"
)

// SortRanges checks if the range overlapped and sort them.
func SortRanges(ranges []rtree.Range, rewriteRules *RewriteRules) ([]rtree.Range, error) {
	rangeTree := rtree.NewRangeTree()
	for _, rg := range ranges {
		if rewriteRules != nil {
			startID := tablecodec.DecodeTableID(rg.StartKey)
			endID := tablecodec.DecodeTableID(rg.EndKey)
			var rule *import_sstpb.RewriteRule
			if startID == endID {
				rg.StartKey, rule = replacePrefix(rg.StartKey, rewriteRules)
				if rule == nil {
					log.Warn("cannot find rewrite rule", zap.Stringer("key", logutil.WrapKey(rg.StartKey)))
				} else {
					log.Debug(
						"rewrite start key",
						zap.Stringer("key", logutil.WrapKey(rg.StartKey)),
						logutil.RewriteRule(rule))
				}
				rg.EndKey, rule = replacePrefix(rg.EndKey, rewriteRules)
				if rule == nil {
					log.Warn("cannot find rewrite rule", zap.Stringer("key", logutil.WrapKey(rg.EndKey)))
				} else {
					log.Debug(
						"rewrite end key",
						zap.Stringer("key", logutil.WrapKey(rg.EndKey)),
						logutil.RewriteRule(rule))
				}
			} else {
				log.Warn("table id does not match",
					zap.Stringer("startKey", logutil.WrapKey(rg.StartKey)),
					zap.Stringer("endKey", logutil.WrapKey(rg.EndKey)),
					zap.Int64("startID", startID),
					zap.Int64("endID", endID))
				return nil, errors.Annotate(berrors.ErrRestoreTableIDMismatch, "table id mismatch")
			}
		}
		if out := rangeTree.InsertRange(rg); out != nil {
			log.Error("insert ranges overlapped",
				logutil.ZapRedactReflect("out", out),
				logutil.ZapRedactReflect("range", rg))
			return nil, errors.Annotatef(berrors.ErrRestoreInvalidRange, "ranges overlapped")
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

// ContainsInterior returns whether the region contains the given key, and also
// that the key does not fall on the boundary (start key) of the region.
func (region *RegionInfo) ContainsInterior(key []byte) bool {
	return bytes.Compare(key, region.Region.GetStartKey()) > 0 &&
		(len(region.Region.GetEndKey()) == 0 ||
			bytes.Compare(key, region.Region.GetEndKey()) < 0)
}

// RewriteRules contains rules for rewriting keys of tables.
type RewriteRules struct {
	Table []*import_sstpb.RewriteRule
	Data  []*import_sstpb.RewriteRule
}

// Append append its argument to this rewrite rules.
func (r *RewriteRules) Append(other RewriteRules) {
	r.Data = append(r.Data, other.Data...)
	r.Table = append(r.Table, other.Table...)
}

// EmptyRewriteRule make a new, empty rewrite rule.
func EmptyRewriteRule() *RewriteRules {
	return &RewriteRules{
		Table: []*import_sstpb.RewriteRule{},
		Data:  []*import_sstpb.RewriteRule{},
	}
}
