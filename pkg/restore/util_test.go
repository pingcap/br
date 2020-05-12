// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"encoding/binary"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

var _ = Suite(&testRestoreUtilSuite{})

type testRestoreUtilSuite struct {
}

func (s *testRestoreUtilSuite) TestGetSSTMetaFromFile(c *C) {
	file := &backup.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t1ccc"),
	}
	rule := &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("t1"),
		NewKeyPrefix: []byte("t2"),
	}
	region := &metapb.Region{
		StartKey: []byte("t2abc"),
		EndKey:   []byte("t3a"),
	}
	sstMeta := getSSTMetaFromFile([]byte{}, file, region, rule)
	c.Assert(string(sstMeta.GetRange().GetStart()), Equals, "t2abc")
	c.Assert(string(sstMeta.GetRange().GetEnd()), Equals, "t2\xff")
}

func (s *testRestoreUtilSuite) TestMapTableToFiles(c *C) {
	filesOfTable1 := []*backup.File{
		{Name: "table1-1.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(1)},
		{Name: "table1-2.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(1)},
		{Name: "table1-3.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(1)}}
	filesOfTable2 := []*backup.File{
		{Name: "table2-1.sst",
			StartKey: tablecodec.EncodeTablePrefix(2),
			EndKey:   tablecodec.EncodeTablePrefix(2)},
		{Name: "table2-2.sst",
			StartKey: tablecodec.EncodeTablePrefix(2),
			EndKey:   tablecodec.EncodeTablePrefix(2)},
	}

	result := MapTableToFiles(append(filesOfTable2, filesOfTable1...))

	c.Assert(result[1], DeepEquals, filesOfTable1)
	c.Assert(result[2], DeepEquals, filesOfTable2)
}

func (s *testRestoreUtilSuite) TestValidateFileRanges(c *C) {
	rules := &RewriteRules{
		Table: []*import_sstpb.RewriteRule{&import_sstpb.RewriteRule{
			OldKeyPrefix: []byte(tablecodec.EncodeTablePrefix(1)),
			NewKeyPrefix: []byte(tablecodec.EncodeTablePrefix(2)),
		}},
	}

	// Empty start/end key is not allowed.
	_, err := ValidateFileRanges(
		[]*backup.File{&backup.File{
			Name:     "file_write.sst",
			StartKey: []byte(""),
			EndKey:   []byte(""),
		}},
		rules,
	)
	c.Assert(err, ErrorMatches, ".*cannot find rewrite rule.*")

	// Range is not overlap, no rule found.
	_, err = ValidateFileRanges(
		[]*backup.File{{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(0),
			EndKey:   tablecodec.EncodeTablePrefix(1),
		}},
		rules,
	)
	c.Assert(err, ErrorMatches, ".*cannot find rewrite rule.*")

	// No rule for end key.
	_, err = ValidateFileRanges(
		[]*backup.File{{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		}},
		rules,
	)
	c.Assert(err, ErrorMatches, ".*cannot find rewrite rule.*")

	// Add a rule for end key.
	rules.Table = append(rules.Table, &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTablePrefix(2),
		NewKeyPrefix: tablecodec.EncodeTablePrefix(3),
	})
	_, err = ValidateFileRanges(
		[]*backup.File{{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		}},
		rules,
	)
	c.Assert(err, ErrorMatches, "table ids mismatch")

	// Add a bad rule for end key, after rewrite start key > end key.
	rules.Table = append(rules.Table[:1], &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTablePrefix(2),
		NewKeyPrefix: tablecodec.EncodeTablePrefix(1),
	})
	_, err = ValidateFileRanges(
		[]*backup.File{{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		}},
		rules,
	)
	c.Assert(err, ErrorMatches, "unexpected rewrite rules")
}

func (s *testRestoreUtilSuite) TestPaginateScanRegion(c *C) {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}

	makeRegions := func(num uint64) (map[uint64]*RegionInfo, []*RegionInfo) {
		regionsMap := make(map[uint64]*RegionInfo, num)
		regions := make([]*RegionInfo, 0, num)
		endKey := make([]byte, 8)
		for i := uint64(0); i < num-1; i++ {
			ri := &RegionInfo{
				Region: &metapb.Region{
					Id:    i + 1,
					Peers: peers,
				},
			}

			if i != 0 {
				startKey := make([]byte, 8)
				binary.BigEndian.PutUint64(startKey, i)
				ri.Region.StartKey = codec.EncodeBytes([]byte{}, startKey)
			}
			endKey = make([]byte, 8)
			binary.BigEndian.PutUint64(endKey, i+1)
			ri.Region.EndKey = codec.EncodeBytes([]byte{}, endKey)

			regionsMap[i] = ri
			regions = append(regions, ri)
		}

		if num == 1 {
			endKey = []byte{}
		} else {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		ri := &RegionInfo{
			Region: &metapb.Region{
				Id:       num,
				Peers:    peers,
				StartKey: endKey,
				EndKey:   []byte{},
			},
		}
		regionsMap[num] = ri
		regions = append(regions, ri)

		return regionsMap, regions
	}

	ctx := context.Background()
	regionMap := make(map[uint64]*RegionInfo)
	regions := []*RegionInfo{}
	batch, err := paginateScanRegion(ctx, newTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	c.Assert(err, IsNil)
	c.Assert(batch, DeepEquals, regions)

	regionMap, regions = makeRegions(1)
	batch, err = paginateScanRegion(ctx, newTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	c.Assert(err, IsNil)
	c.Assert(batch, DeepEquals, regions)

	regionMap, regions = makeRegions(2)
	batch, err = paginateScanRegion(ctx, newTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	c.Assert(err, IsNil)
	c.Assert(batch, DeepEquals, regions)

	regionMap, regions = makeRegions(3)
	batch, err = paginateScanRegion(ctx, newTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	c.Assert(err, IsNil)
	c.Assert(batch, DeepEquals, regions)

	regionMap, regions = makeRegions(8)
	batch, err = paginateScanRegion(ctx, newTestClient(stores, regionMap, 0), []byte{}, []byte{}, 3)
	c.Assert(err, IsNil)
	c.Assert(batch, DeepEquals, regions)

	regionMap, regions = makeRegions(8)
	batch, err = paginateScanRegion(
		ctx, newTestClient(stores, regionMap, 0), regions[1].Region.StartKey, []byte{}, 3)
	c.Assert(err, IsNil)
	c.Assert(batch, DeepEquals, regions[1:])

	batch, err = paginateScanRegion(
		ctx, newTestClient(stores, regionMap, 0), []byte{}, regions[6].Region.EndKey, 3)
	c.Assert(err, IsNil)
	c.Assert(batch, DeepEquals, regions[:7])

	batch, err = paginateScanRegion(
		ctx, newTestClient(stores, regionMap, 0), regions[1].Region.StartKey, regions[1].Region.EndKey, 3)
	c.Assert(err, IsNil)
	c.Assert(batch, DeepEquals, regions[1:2])

	_, err = paginateScanRegion(ctx, newTestClient(stores, regionMap, 0), []byte{2}, []byte{1}, 3)
	c.Assert(err, ErrorMatches, "startKey >= endKey.*")
}
