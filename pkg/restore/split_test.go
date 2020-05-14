// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"bytes"
	"context"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/pingcap/tidb/util/codec"

	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/rtree"
)

type testClient struct {
	mu           sync.RWMutex
	stores       map[uint64]*metapb.Store
	regions      map[uint64]*restore.RegionInfo
	regionsInfo  *core.RegionsInfo // For now it's only used in ScanRegions
	nextRegionID uint64
}

func newTestClient(
	stores map[uint64]*metapb.Store,
	regions map[uint64]*restore.RegionInfo,
	nextRegionID uint64,
) *testClient {
	regionsInfo := core.NewRegionsInfo()
	for _, regionInfo := range regions {
		regionsInfo.AddRegion(core.NewRegionInfo(regionInfo.Region, regionInfo.Leader))
	}
	return &testClient{
		stores:       stores,
		regions:      regions,
		regionsInfo:  regionsInfo,
		nextRegionID: nextRegionID,
	}
}

func (c *testClient) GetAllRegions() map[uint64]*restore.RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.regions
}

func (c *testClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	store, ok := c.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store not found")
	}
	return store, nil
}

func (c *testClient) GetRegion(ctx context.Context, key []byte) (*restore.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, region := range c.regions {
		if bytes.Compare(key, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(key, region.Region.EndKey) < 0) {
			return region, nil
		}
	}
	return nil, errors.Errorf("region not found: key=%s", string(key))
}

func (c *testClient) GetRegionByID(ctx context.Context, regionID uint64) (*restore.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	region, ok := c.regions[regionID]
	if !ok {
		return nil, errors.Errorf("region not found: id=%d", regionID)
	}
	return region, nil
}

func (c *testClient) SplitRegion(
	ctx context.Context,
	regionInfo *restore.RegionInfo,
	key []byte,
) (*restore.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var target *restore.RegionInfo
	splitKey := codec.EncodeBytes([]byte{}, key)
	for _, region := range c.regions {
		if bytes.Compare(splitKey, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(splitKey, region.Region.EndKey) < 0) {
			target = region
		}
	}
	if target == nil {
		return nil, errors.Errorf("region not found: key=%s", string(key))
	}
	newRegion := &restore.RegionInfo{
		Region: &metapb.Region{
			Peers:    target.Region.Peers,
			Id:       c.nextRegionID,
			StartKey: target.Region.StartKey,
			EndKey:   splitKey,
		},
	}
	c.regions[c.nextRegionID] = newRegion
	c.nextRegionID++
	target.Region.StartKey = splitKey
	c.regions[target.Region.Id] = target
	return newRegion, nil
}

func (c *testClient) BatchSplitRegions(
	ctx context.Context, regionInfo *restore.RegionInfo, keys [][]byte,
) ([]*restore.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	newRegions := make([]*restore.RegionInfo, 0)
	for _, key := range keys {
		var target *restore.RegionInfo
		splitKey := codec.EncodeBytes([]byte{}, key)
		for _, region := range c.regions {
			if region.ContainsInterior(splitKey) {
				target = region
			}
		}
		if target == nil {
			continue
		}
		newRegion := &restore.RegionInfo{
			Region: &metapb.Region{
				Peers:    target.Region.Peers,
				Id:       c.nextRegionID,
				StartKey: target.Region.StartKey,
				EndKey:   splitKey,
			},
		}
		c.regions[c.nextRegionID] = newRegion
		c.nextRegionID++
		target.Region.StartKey = splitKey
		c.regions[target.Region.Id] = target
		newRegions = append(newRegions, newRegion)
	}
	return newRegions, nil
}

func (c *testClient) ScatterRegion(ctx context.Context, regionInfo *restore.RegionInfo) error {
	return nil
}

func (c *testClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{
		Header: new(pdpb.ResponseHeader),
	}, nil
}

func (c *testClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*restore.RegionInfo, error) {
	infos := c.regionsInfo.ScanRange(key, endKey, limit)
	regions := make([]*restore.RegionInfo, 0, len(infos))
	for _, info := range infos {
		regions = append(regions, &restore.RegionInfo{
			Region: info.GetMeta(),
			Leader: info.GetLeader(),
		})
	}
	return regions, nil
}

func (c *testClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (r placement.Rule, err error) {
	return
}

func (c *testClient) SetPlacementRule(ctx context.Context, rule placement.Rule) error {
	return nil
}

func (c *testClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	return nil
}

func (c *testClient) SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error {
	return nil
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
// rewrite rules: aa -> xx,  cc -> bb
// expected regions after split:
//   [, aay), [aay, bb), [bb, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//   [bbj, cca), [cca, xx), [xx, xxe), [xxe, xxz), [xxz, )
func (s *testRestoreUtilSuite) TestSplit(c *C) {
	client := initTestClient()
	ranges := initRanges()
	rewriteRules := initRewriteRules()
	regionSplitter := restore.NewRegionSplitter(client)

	ctx := context.Background()
	err := regionSplitter.Split(ctx, ranges, rewriteRules, func(key [][]byte) {})
	if err != nil {
		c.Assert(err, IsNil, Commentf("split regions failed: %v", err))
	}
	regions := client.GetAllRegions()
	if !validateRegions(regions) {
		for _, region := range regions {
			c.Logf("region: %v\n", region.Region)
		}
		c.Log("get wrong result")
		c.Fail()
	}
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
func initTestClient() *testClient {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	keys := [6]string{"", "aay", "bba", "bbh", "cca", ""}
	regions := make(map[uint64]*restore.RegionInfo)
	for i := uint64(1); i < 6; i++ {
		startKey := []byte(keys[i-1])
		if len(startKey) != 0 {
			startKey = codec.EncodeBytes([]byte{}, startKey)
		}
		endKey := []byte(keys[i])
		if len(endKey) != 0 {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		regions[i] = &restore.RegionInfo{
			Region: &metapb.Region{
				Id:       i,
				Peers:    peers,
				StartKey: startKey,
				EndKey:   endKey,
			},
		}
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}
	return newTestClient(stores, regions, 6)
}

// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
func initRanges() []rtree.Range {
	var ranges [4]rtree.Range
	ranges[0] = rtree.Range{
		StartKey: []byte("aaa"),
		EndKey:   []byte("aae"),
	}
	ranges[1] = rtree.Range{
		StartKey: []byte("aae"),
		EndKey:   []byte("aaz"),
	}
	ranges[2] = rtree.Range{
		StartKey: []byte("ccd"),
		EndKey:   []byte("ccf"),
	}
	ranges[3] = rtree.Range{
		StartKey: []byte("ccf"),
		EndKey:   []byte("ccj"),
	}
	return ranges[:]
}

func initRewriteRules() *restore.RewriteRules {
	var rules [2]*import_sstpb.RewriteRule
	rules[0] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("aa"),
		NewKeyPrefix: []byte("xx"),
	}
	rules[1] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("cc"),
		NewKeyPrefix: []byte("bb"),
	}
	return &restore.RewriteRules{
		Table: rules[:],
		Data:  rules[:],
	}
}

// expected regions after split:
//   [, aay), [aay, bb), [bb, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//   [bbj, cca), [cca, xx), [xx, xxe), [xxe, xxz), [xxz, )
func validateRegions(regions map[uint64]*restore.RegionInfo) bool {
	keys := [12]string{"", "aay", "bb", "bba", "bbf", "bbh", "bbj", "cca", "xx", "xxe", "xxz", ""}
	if len(regions) != 11 {
		return false
	}
FindRegion:
	for i := 1; i < len(keys); i++ {
		for _, region := range regions {
			startKey := []byte(keys[i-1])
			if len(startKey) != 0 {
				startKey = codec.EncodeBytes([]byte{}, startKey)
			}
			endKey := []byte(keys[i])
			if len(endKey) != 0 {
				endKey = codec.EncodeBytes([]byte{}, endKey)
			}
			if bytes.Equal(region.Region.GetStartKey(), startKey) &&
				bytes.Equal(region.Region.GetEndKey(), endKey) {
				continue FindRegion
			}
		}
		return false
	}
	return true
}

func (s *testRestoreUtilSuite) TestNeedSplit(c *C) {
	regions := []*restore.RegionInfo{
		{
			Region: &metapb.Region{
				StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
				EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
			},
		},
	}
	// Out of region
	c.Assert(restore.NeedSplit([]byte("a"), regions), IsNil)
	// Region start key
	c.Assert(restore.NeedSplit([]byte("b"), regions), IsNil)
	// In region
	region := restore.NeedSplit([]byte("c"), regions)
	c.Assert(bytes.Compare(region.Region.GetStartKey(), codec.EncodeBytes([]byte{}, []byte("b"))), Equals, 0)
	c.Assert(bytes.Compare(region.Region.GetEndKey(), codec.EncodeBytes([]byte{}, []byte("d"))), Equals, 0)
	// Region end key
	c.Assert(restore.NeedSplit([]byte("d"), regions), IsNil)
	// Out of region
	c.Assert(restore.NeedSplit([]byte("e"), regions), IsNil)
}
