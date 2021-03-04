// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"

	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/logutil"
	"github.com/pingcap/br/pkg/rtree"
)

// Constants for split retry machinery.
const (
	SplitRetryTimes       = 32
	SplitRetryInterval    = 50 * time.Millisecond
	SplitMaxRetryInterval = time.Second

	SplitCheckMaxRetryTimes = 64
	SplitCheckInterval      = 8 * time.Millisecond
	SplitMaxCheckInterval   = time.Second

	ScatterWaitMaxRetryTimes = 64
	ScatterWaitInterval      = 50 * time.Millisecond
	ScatterMaxWaitInterval   = time.Second
	ScatterWaitUpperInterval = 180 * time.Second

	RejectStoreCheckRetryTimes  = 64
	RejectStoreCheckInterval    = 100 * time.Millisecond
	RejectStoreMaxCheckInterval = 2 * time.Second
)

// RegionSplitter is a executor of region split by rules.
type RegionSplitter struct {
	client SplitClient
}

// NewRegionSplitter returns a new RegionSplitter.
func NewRegionSplitter(client SplitClient) *RegionSplitter {
	return &RegionSplitter{
		client: client,
	}
}

// OnSplitFunc is called before split a range.
type OnSplitFunc func(key [][]byte)

// Split executes a region split. It will split regions by the rewrite rules,
// then it will split regions by the end key of each range.
// tableRules includes the prefix of a table, since some ranges may have
// a prefix with record sequence or index sequence.
// note: all ranges and rewrite rules must have raw key.
func (rs *RegionSplitter) Split(
	ctx context.Context,
	ranges []rtree.Range,
	rewriteRules *RewriteRules,
	onSplit OnSplitFunc,
) error {
	if len(ranges) == 0 {
		log.Info("skip split regions, no range")
		return nil
	}
	startTime := time.Now()
	// Sort the range for getting the min and max key of the ranges
	sortedRanges, errSplit := SortRanges(ranges, rewriteRules)
	if errSplit != nil {
		return errors.Trace(errSplit)
	}
	minKey := codec.EncodeBytes([]byte{}, sortedRanges[0].StartKey)
	maxKey := codec.EncodeBytes([]byte{}, sortedRanges[len(sortedRanges)-1].EndKey)
	for _, rule := range rewriteRules.Table {
		if bytes.Compare(minKey, rule.GetNewKeyPrefix()) > 0 {
			minKey = rule.GetNewKeyPrefix()
		}
		if bytes.Compare(maxKey, rule.GetNewKeyPrefix()) < 0 {
			maxKey = rule.GetNewKeyPrefix()
		}
	}
	for _, rule := range rewriteRules.Data {
		if bytes.Compare(minKey, rule.GetNewKeyPrefix()) > 0 {
			minKey = rule.GetNewKeyPrefix()
		}
		if bytes.Compare(maxKey, rule.GetNewKeyPrefix()) < 0 {
			maxKey = rule.GetNewKeyPrefix()
		}
	}
	interval := SplitRetryInterval
	scatterRegions := make([]*RegionInfo, 0)
SplitRegions:
	for i := 0; i < SplitRetryTimes; i++ {
		regions, errScan := PaginateScanRegion(ctx, rs.client, minKey, maxKey, scanRegionPaginationLimit)
		if errScan != nil {
			return errors.Trace(errScan)
		}
		if len(regions) == 0 {
			log.Warn("split regions cannot scan any region")
			return nil
		}
		splitKeyMap := GetSplitKeys(rewriteRules, sortedRanges, regions)
		regionMap := make(map[uint64]*RegionInfo)
		for _, region := range regions {
			regionMap[region.Region.GetId()] = region
		}
		for regionID, keys := range splitKeyMap {
			var newRegions []*RegionInfo
			region := regionMap[regionID]
			log.Info("split regions",
				logutil.Region(region.Region), logutil.Keys(keys), rtree.ZapRanges(ranges))
			newRegions, errSplit = rs.splitAndScatterRegions(ctx, region, keys)
			if errSplit != nil {
				if strings.Contains(errSplit.Error(), "no valid key") {
					for _, key := range keys {
						// Region start/end keys are encoded. split_region RPC
						// requires raw keys (without encoding).
						log.Error("split regions no valid key",
							logutil.Key("startKey", region.Region.StartKey),
							logutil.Key("endKey", region.Region.EndKey),
							logutil.Key("key", codec.EncodeBytes([]byte{}, key)),
							rtree.ZapRanges(ranges))
					}
					return errors.Trace(errSplit)
				}
				interval = 2 * interval
				if interval > SplitMaxRetryInterval {
					interval = SplitMaxRetryInterval
				}
				time.Sleep(interval)
				log.Warn("split regions failed, retry",
					zap.Error(errSplit),
					logutil.Region(region.Region),
					logutil.Leader(region.Leader),
					logutil.Keys(keys), rtree.ZapRanges(ranges))
				continue SplitRegions
			}
			if len(newRegions) != len(keys) {
				log.Warn("split key count and new region count mismatch",
					zap.Int("new region count", len(newRegions)),
					zap.Int("split key count", len(keys)))
			}
			scatterRegions = append(scatterRegions, newRegions...)
			onSplit(keys)
		}
		break
	}
	if errSplit != nil {
		return errors.Trace(errSplit)
	}
	log.Info("start to wait for scattering regions",
		zap.Int("regions", len(scatterRegions)), zap.Duration("take", time.Since(startTime)))
	startTime = time.Now()
	scatterCount := 0
	for _, region := range scatterRegions {
		rs.waitForScatterRegion(ctx, region)
		if time.Since(startTime) > ScatterWaitUpperInterval {
			break
		}
		scatterCount++
	}
	if scatterCount == len(scatterRegions) {
		log.Info("waiting for scattering regions done",
			zap.Int("regions", len(scatterRegions)), zap.Duration("take", time.Since(startTime)))
	} else {
		log.Warn("waiting for scattering regions timeout",
			zap.Int("scatterCount", scatterCount),
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
	}
	return nil
}

func (rs *RegionSplitter) hasRegion(ctx context.Context, regionID uint64) (bool, error) {
	regionInfo, err := rs.client.GetRegionByID(ctx, regionID)
	if err != nil {
		return false, errors.Trace(err)
	}
	return regionInfo != nil, nil
}

func (rs *RegionSplitter) isScatterRegionFinished(ctx context.Context, regionID uint64) (bool, error) {
	resp, err := rs.client.GetOperator(ctx, regionID)
	if err != nil {
		return false, errors.Trace(err)
	}
	// Heartbeat may not be sent to PD
	if respErr := resp.GetHeader().GetError(); respErr != nil {
		if respErr.GetType() == pdpb.ErrorType_REGION_NOT_FOUND {
			return true, nil
		}
		return false, errors.Annotatef(berrors.ErrPDInvalidResponse, "get operator error: %s", respErr.GetType())
	}
	retryTimes := ctx.Value(retryTimes).(int)
	if retryTimes > 3 {
		log.Info("get operator", zap.Uint64("regionID", regionID), zap.Stringer("resp", resp))
	}
	// If the current operator of the region is not 'scatter-region', we could assume
	// that 'scatter-operator' has finished or timeout
	ok := string(resp.GetDesc()) != "scatter-region" || resp.GetStatus() != pdpb.OperatorStatus_RUNNING
	return ok, nil
}

func (rs *RegionSplitter) waitForSplit(ctx context.Context, regionID uint64) {
	interval := SplitCheckInterval
	for i := 0; i < SplitCheckMaxRetryTimes; i++ {
		ok, err := rs.hasRegion(ctx, regionID)
		if err != nil {
			log.Warn("wait for split failed", zap.Error(err))
			return
		}
		if ok {
			break
		}
		interval = 2 * interval
		if interval > SplitMaxCheckInterval {
			interval = SplitMaxCheckInterval
		}
		time.Sleep(interval)
	}
}

type retryTimeKey struct{}

var retryTimes = new(retryTimeKey)

func (rs *RegionSplitter) waitForScatterRegion(ctx context.Context, regionInfo *RegionInfo) {
	interval := ScatterWaitInterval
	regionID := regionInfo.Region.GetId()
	for i := 0; i < ScatterWaitMaxRetryTimes; i++ {
		ctx1 := context.WithValue(ctx, retryTimes, i)
		ok, err := rs.isScatterRegionFinished(ctx1, regionID)
		if err != nil {
			log.Warn("scatter region failed: do not have the region",
				logutil.Region(regionInfo.Region))
			return
		}
		if ok {
			break
		}
		interval = 2 * interval
		if interval > ScatterMaxWaitInterval {
			interval = ScatterMaxWaitInterval
		}
		time.Sleep(interval)
	}
}

func (rs *RegionSplitter) splitAndScatterRegions(
	ctx context.Context, regionInfo *RegionInfo, keys [][]byte,
) ([]*RegionInfo, error) {
	newRegions, err := rs.client.BatchSplitRegions(ctx, regionInfo, keys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, region := range newRegions {
		// Wait for a while until the regions successfully split.
		rs.waitForSplit(ctx, region.Region.Id)
		if err = rs.client.ScatterRegion(ctx, region); err != nil {
			log.Warn("scatter region failed", logutil.Region(region.Region), zap.Error(err))
		}
	}
	return newRegions, nil
}

// GetSplitKeys checks if the regions should be split by the new prefix of the rewrites rule and the end key of
// the ranges, groups the split keys by region id.
func GetSplitKeys(rewriteRules *RewriteRules, ranges []rtree.Range, regions []*RegionInfo) map[uint64][][]byte {
	splitKeyMap := make(map[uint64][][]byte)
	checkKeys := make([][]byte, 0)
	for _, rule := range rewriteRules.Table {
		checkKeys = append(checkKeys, rule.GetNewKeyPrefix())
	}
	for _, rule := range rewriteRules.Data {
		checkKeys = append(checkKeys, rule.GetNewKeyPrefix())
	}
	for _, rg := range ranges {
		checkKeys = append(checkKeys, truncateRowKey(rg.EndKey))
	}
	for _, key := range checkKeys {
		if region := NeedSplit(key, regions); region != nil {
			splitKeys, ok := splitKeyMap[region.Region.GetId()]
			if !ok {
				splitKeys = make([][]byte, 0, 1)
			}
			splitKeyMap[region.Region.GetId()] = append(splitKeys, key)
			log.Debug("get key for split region",
				logutil.Key("key", key),
				logutil.Key("startKey", region.Region.StartKey),
				logutil.Key("endKey", region.Region.EndKey))
		}
	}
	return splitKeyMap
}

// NeedSplit checks whether a key is necessary to split, if true returns the split region.
func NeedSplit(splitKey []byte, regions []*RegionInfo) *RegionInfo {
	// If splitKey is the max key.
	if len(splitKey) == 0 {
		return nil
	}
	splitKey = codec.EncodeBytes([]byte{}, splitKey)
	for _, region := range regions {
		// If splitKey is the boundary of the region
		if bytes.Equal(splitKey, region.Region.GetStartKey()) {
			return nil
		}
		// If splitKey is in a region
		if region.ContainsInterior(splitKey) {
			return region
		}
	}
	return nil
}

var (
	tablePrefix  = []byte{'t'}
	idLen        = 8
	recordPrefix = []byte("_r")
)

func truncateRowKey(key []byte) []byte {
	if bytes.HasPrefix(key, tablePrefix) &&
		len(key) > tablecodec.RecordRowKeyLen &&
		bytes.HasPrefix(key[len(tablePrefix)+idLen:], recordPrefix) {
		return key[:tablecodec.RecordRowKeyLen]
	}
	return key
}

func replacePrefix(s []byte, rewriteRules *RewriteRules) ([]byte, *import_sstpb.RewriteRule) {
	// We should search the dataRules firstly.
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(s, rule.GetOldKeyPrefix()) {
			return append(append([]byte{}, rule.GetNewKeyPrefix()...), s[len(rule.GetOldKeyPrefix()):]...), rule
		}
	}
	for _, rule := range rewriteRules.Table {
		if bytes.HasPrefix(s, rule.GetOldKeyPrefix()) {
			return append(append([]byte{}, rule.GetNewKeyPrefix()...), s[len(rule.GetOldKeyPrefix()):]...), rule
		}
	}

	return s, nil
}

func beforeEnd(key []byte, end []byte) bool {
	return bytes.Compare(key, end) < 0 || len(end) == 0
}

func keyInsideRegion(region *metapb.Region, key []byte) bool {
	return bytes.Compare(key, region.GetStartKey()) >= 0 && beforeEnd(key, region.GetEndKey())
}

func nextKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{}
	}
	res := make([]byte, 0, len(key)+1)
	pos := 0
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] != '\xff' {
			pos = i
			break
		}
	}
	s, e := key[:pos], key[pos]+1
	res = append(append(res, s...), e)
	return res
}
