// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
	kvproto "github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"

	"github.com/pingcap/br/pkg/restore"
)

var _ = Suite(&testMergeRangesSuite{})

type testMergeRangesSuite struct{}

type fileBulder struct {
	tableID, startKeyOffset int64
}

func (fb *fileBulder) build(tableID, num, bytes, kv int) (files []*kvproto.File) {
	if num != 1 && num != 2 {
		panic("num must be 1 or 2")
	}

	// Rotate table ID
	if fb.tableID != int64(tableID) {
		fb.tableID = int64(tableID)
		fb.startKeyOffset = 0
	}

	low := codec.EncodeInt(nil, fb.startKeyOffset)
	fb.startKeyOffset += 10
	high := codec.EncodeInt(nil, fb.startKeyOffset)
	files = append(files, &kvproto.File{
		Name:       fmt.Sprint(rand.Int63n(math.MaxInt64), "_write.sst"),
		StartKey:   tablecodec.EncodeRowKey(fb.tableID, low),
		EndKey:     tablecodec.EncodeRowKey(fb.tableID, high),
		TotalKvs:   uint64(kv),
		TotalBytes: uint64(bytes),
		Cf:         "write",
	})
	if num == 1 {
		return
	}

	// To match TiKV's behavior.
	files[0].TotalKvs = 0
	files[0].TotalBytes = 0
	files = append(files, &kvproto.File{
		Name:       fmt.Sprint(rand.Int63n(math.MaxInt64), "_default.sst"),
		StartKey:   tablecodec.EncodeRowKey(fb.tableID, low),
		EndKey:     tablecodec.EncodeRowKey(fb.tableID, high),
		TotalKvs:   uint64(kv),
		TotalBytes: uint64(bytes),
		Cf:         "default",
	})
	return files
}

func (s *testMergeRangesSuite) TestMergeRanges(c *C) {
	type Case struct {
		files       [][4]int // tableID, num, bytes, kv
		mergedIndex []int    // start from 0, points to an end of a merged group
		stat        restore.MergeRangesStat
	}
	splitSizeBytes := int(restore.DefaultMergeRegionSizeBytes)
	splitKeyCount := int(restore.DefaultMergeRegionKeyCount)
	cases := []Case{
		// Empty backup.
		{
			files:       [][4]int{},
			mergedIndex: []int{},
			stat:        restore.MergeRangesStat{TotalRegions: 0, MergedRegions: 0},
		},

		// Do not merge big range.
		{
			files:       [][4]int{{1, 1, splitSizeBytes, 1}, {1, 1, 1, 1}},
			mergedIndex: []int{},
			stat:        restore.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		{
			files:       [][4]int{{1, 1, 1, 1}, {1, 1, splitSizeBytes, 1}},
			mergedIndex: []int{},
			stat:        restore.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		{
			files:       [][4]int{{1, 1, 1, splitKeyCount}, {1, 1, 1, 1}},
			mergedIndex: []int{},
			stat:        restore.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		{
			files:       [][4]int{{1, 1, 1, 1}, {1, 1, 1, splitKeyCount}},
			mergedIndex: []int{},
			stat:        restore.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},

		// 3 -> 1
		{
			files:       [][4]int{{1, 1, 1, 1}, {1, 1, 1, 1}, {1, 1, 1, 1}},
			mergedIndex: []int{2},
			stat:        restore.MergeRangesStat{TotalRegions: 3, MergedRegions: 1},
		},
		// 3 -> 2, [split*1/3, split*1/3, split*1/2] -> [split*2/3, split*1/2]
		{
			files:       [][4]int{{1, 1, splitSizeBytes / 3, 1}, {1, 1, splitSizeBytes / 3, 1}, {1, 1, splitSizeBytes / 2, 1}},
			mergedIndex: []int{1},
			stat:        restore.MergeRangesStat{TotalRegions: 3, MergedRegions: 2},
		},
		// 4 -> 2, [split*1/3, split*1/3, split*1/2, 1] -> [split*2/3, split*1/2 +1]
		{
			files:       [][4]int{{1, 1, splitSizeBytes / 3, 1}, {1, 1, splitSizeBytes / 3, 1}, {1, 1, splitSizeBytes / 2, 1}, {1, 1, 1, 1}},
			mergedIndex: []int{1, 3},
			stat:        restore.MergeRangesStat{TotalRegions: 4, MergedRegions: 2},
		},
		// 5 -> 3, [split*1/3, split*1/3, split, split*1/2, 1] -> [split*2/3, split, split*1/2 +1]
		{
			files:       [][4]int{{1, 1, splitSizeBytes / 3, 1}, {1, 1, splitSizeBytes / 3, 1}, {1, 1, splitSizeBytes, 1}, {1, 1, splitSizeBytes / 2, 1}, {1, 1, 1, 1}},
			mergedIndex: []int{1, 2, 4},
			stat:        restore.MergeRangesStat{TotalRegions: 5, MergedRegions: 3},
		},

		// Do not merge ranges from different tables
		// 2 -> 2, [1, 1] -> [1, 1]
		{
			files:       [][4]int{{1, 1, 1, 1}, {2, 1, 1, 1}},
			mergedIndex: []int{},
			stat:        restore.MergeRangesStat{TotalRegions: 2, MergedRegions: 2},
		},
		// 3 -> 2, [1@split*1/3, 2@split*1/3, 2@split*1/2] -> [1@split*1/3, 2@split*5/6]
		{
			files:       [][4]int{{1, 1, splitSizeBytes / 3, 1}, {2, 1, splitSizeBytes / 3, 1}, {2, 1, splitSizeBytes / 2, 1}},
			mergedIndex: []int{0, 2},
			stat:        restore.MergeRangesStat{TotalRegions: 3, MergedRegions: 2},
		},
	}

	for _, cs := range cases {
		backupMeta := &kvproto.BackupMeta{}
		fb := fileBulder{}
		for _, f := range cs.files {
			backupMeta.Files = append(backupMeta.Files, fb.build(f[0], f[1], f[2], f[3])...)
		}
		stat, err := restore.MergeRanges(backupMeta, restore.DefaultMergeRegionSizeBytes, restore.DefaultMergeRegionKeyCount)
		c.Assert(err, IsNil, Commentf("%+v", cs))
		c.Assert(stat.TotalRegions, Equals, cs.stat.TotalRegions, Commentf("%+v", cs))
		c.Assert(stat.MergedRegions, Equals, cs.stat.MergedRegions, Commentf("%+v", cs))

		// Files in the same merge group must have the same start key and end key.
		lastOffset := 0
		for _, offset := range cs.mergedIndex {
			start := lastOffset
			end := offset
			for j := start + 1; j < end; j++ {
				c.Assert(backupMeta.Files[j-1].StartKey, BytesEquals, backupMeta.Files[j].StartKey, Commentf("%+v", cs))
				c.Assert(backupMeta.Files[j-1].EndKey, BytesEquals, backupMeta.Files[j].EndKey, Commentf("%+v", cs))
			}
			lastOffset = offset + 1
		}
	}
}

// Benchmark results on Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz
//
// BenchmarkMergeRanges1k-40          10430            119695 ns/op
// BenchmarkMergeRanges10k-40           168           6326650 ns/op
// BenchmarkMergeRanges50k-40             1       28679301994 ns/op
// BenchmarkMergeRanges100k-40            1      131613090157 ns/op

func benchmarkMergeRanges(b *testing.B, files int) {
	backupMeta := &kvproto.BackupMeta{}
	fb := fileBulder{}
	for i := 0; i < files; i++ {
		backupMeta.Files = append(backupMeta.Files, fb.build(1, 1, 1, 1)...)
	}
	var err error
	for i := 0; i < b.N; i++ {
		_, err = restore.MergeRanges(backupMeta, restore.DefaultMergeRegionSizeBytes, restore.DefaultMergeRegionKeyCount)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkMergeRanges1k(b *testing.B) {
	benchmarkMergeRanges(b, 1000)
}

func BenchmarkMergeRanges10k(b *testing.B) {
	benchmarkMergeRanges(b, 10000)
}

func BenchmarkMergeRanges50k(b *testing.B) {
	benchmarkMergeRanges(b, 50000)
}

func BenchmarkMergeRanges100k(b *testing.B) {
	benchmarkMergeRanges(b, 100000)
}
