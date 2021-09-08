// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/tikv/pd/pkg/codec"
	"go.uber.org/zap"

	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/logutil"
	"github.com/pingcap/br/pkg/redact"
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/utils"
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
	minKey := codec.EncodeBytes(sortedRanges[0].StartKey)
	maxKey := codec.EncodeBytes(sortedRanges[len(sortedRanges)-1].EndKey)
	interval := SplitRetryInterval
	scatterRegions := make([]*RegionInfo, 0)
SplitRegions:
	for i := 0; i < SplitRetryTimes; i++ {
		regions, errScan := PaginateScanRegion(ctx, rs.client, minKey, maxKey, scanRegionPaginationLimit)
		if errScan != nil {
			if berrors.ErrPDBatchScanRegion.Equal(errScan) {
				log.Warn("inconsistent region info get.", logutil.ShortError(errScan))
				time.Sleep(time.Second)
				continue SplitRegions
			}
			return errors.Trace(errScan)
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
							logutil.Key("key", codec.EncodeBytes(key)),
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
	if len(keys) == 0 {
		return []*RegionInfo{regionInfo}, nil
	}

	newRegions, err := rs.client.BatchSplitRegions(ctx, regionInfo, keys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// There would be some regions be scattered twice, e.g.:
	// |--1-|--2-+----|-3--|
	//      |    +(t1)|
	//      +(t1_r4)  |
	//                +(t2_r42)
	// When spliting at `t1_r4`, we would scatter region 1, 2.
	// When spliting at `t2_r42`, we would scatter region 2, 3.
	// Because we don't split at t1 anymore.
	// The trick here is a pinky promise: never scatter regions you haven't imported any data.
	// In this scenario, it is the last region after spliting (applying to >= 5.0).
	if bytes.Equal(newRegions[len(newRegions)-1].Region.StartKey, keys[len(keys)-1]) {
		newRegions = newRegions[:len(newRegions)-1]
	}
	rs.ScatterRegions(ctx, newRegions)
	return newRegions, nil
}

// ScatterRegions scatter the regions.
func (rs *RegionSplitter) ScatterRegions(ctx context.Context, newRegions []*RegionInfo) {
	for _, region := range newRegions {
		// Wait for a while until the regions successfully split.
		rs.waitForSplit(ctx, region.Region.Id)
		if err := rs.client.ScatterRegion(ctx, region); err != nil {
			log.Warn("scatter region failed", logutil.Region(region.Region), zap.Error(err))
		}
	}
}

func checkRegionConsistency(startKey, endKey []byte, regions []*RegionInfo) error {
	// current pd can't guarantee the consistency of returned regions
	if len(regions) == 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "scan region return empty result, startKey: %s, endkey: %s",
			redact.Key(startKey), redact.Key(endKey))
	}

	if bytes.Compare(regions[0].Region.StartKey, startKey) > 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "first region's startKey > startKey, startKey: %s, regionStartKey: %s",
			redact.Key(startKey), redact.Key(regions[0].Region.StartKey))
	} else if len(regions[len(regions)-1].Region.EndKey) != 0 && bytes.Compare(regions[len(regions)-1].Region.EndKey, endKey) < 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "last region's endKey < startKey, startKey: %s, regionStartKey: %s",
			redact.Key(endKey), redact.Key(regions[len(regions)-1].Region.EndKey))
	}

	cur := regions[0]
	for _, r := range regions[1:] {
		if !bytes.Equal(cur.Region.EndKey, r.Region.StartKey) {
			return errors.Annotatef(berrors.ErrPDBatchScanRegion, "region endKey not equal to next region startKey, endKey: %s, startKey: %s",
				redact.Key(cur.Region.EndKey), redact.Key(r.Region.StartKey))
		}
		cur = r
	}

	return nil
}

// PaginateScanRegion scan regions with a limit pagination and
// return all regions at once.
// It reduces max gRPC message size.
func PaginateScanRegion(
	ctx context.Context, client SplitClient, startKey, endKey []byte, limit int,
) ([]*RegionInfo, error) {
	if len(endKey) != 0 && bytes.Compare(startKey, endKey) >= 0 {
		return nil, errors.Annotatef(berrors.ErrRestoreInvalidRange, "startKey >= endKey, startKey: %s, endkey: %s",
			hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	}

	var regions []*RegionInfo
	err := utils.WithRetry(ctx, func() error {
		regions = []*RegionInfo{}
		scanStartKey := startKey
		for {
			batch, err := client.ScanRegions(ctx, scanStartKey, endKey, limit)
			if err != nil {
				return errors.Trace(err)
			}
			regions = append(regions, batch...)
			if len(batch) < limit {
				// No more region
				break
			}
			scanStartKey = batch[len(batch)-1].Region.GetEndKey()
			if len(scanStartKey) == 0 ||
				(len(endKey) > 0 && bytes.Compare(scanStartKey, endKey) >= 0) {
				// All key space have scanned
				break
			}
		}
		if err := checkRegionConsistency(startKey, endKey, regions); err != nil {
			log.Warn("failed to scan region, retrying", logutil.ShortError(err))
			return err
		}
		return nil
	}, newScanRegionBackoffer())

	return regions, err
}

type scanRegionBackoffer struct {
	attempt int
}

func newScanRegionBackoffer() utils.Backoffer {
	return &scanRegionBackoffer{
		attempt: 3,
	}
}

// NextBackoff returns a duration to wait before retrying again
func (b *scanRegionBackoffer) NextBackoff(err error) time.Duration {
	if berrors.ErrPDBatchScanRegion.Equal(err) {
		// 500ms * 3 could be enough for splitting remain regions in the hole.
		b.attempt--
		return 500 * time.Millisecond
	}
	b.attempt = 0
	return 0
}

// Attempt returns the remain attempt times
func (b *scanRegionBackoffer) Attempt() int {
	return b.attempt
}

// GetSplitKeys checks if the regions should be split by the new prefix of the rewrites rule and the end key of
// the ranges, groups the split keys by region id.
func GetSplitKeys(rewriteRules *RewriteRules, ranges []rtree.Range, regions []*RegionInfo) map[uint64][][]byte {
	splitKeyMap := make(map[uint64][][]byte)
	checkKeys := make([][]byte, 0)
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
	splitKey = codec.EncodeBytes(splitKey)
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
