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
	"github.com/pingcap/br/pkg/utils"
)

// See https://github.com/tikv/tikv/blob/v4.0.8/components/raftstore/src/coprocessor/config.rs#L35-L38
const (
	// splitSizeMB is the default region split size.
	splitSizeMB uint64 = 96 * utils.MB
	// splitKeys is the default region split key count.
	splitKeys uint64 = 960000
)

var _ = Suite(&testMergeRangesSuite{})

type testMergeRangesSuite struct{}

type fileBulder struct {
	tableID, startKeyOffset int64
}

func (fb *fileBulder) build(c *C, tableID, num, bytes, kv int) (files []*kvproto.File) {
	if c != nil {
		c.Assert(num == 1 || num == 2, IsTrue)
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
	cases := []Case{
		// Do not merge big range.
		{files: [][4]int{{1, 1, int(splitSizeMB), 1}, {1, 1, 1, 1}},
			mergedIndex: []int{},
			stat:        restore.MergeRangesStat{TotalRegions: 2, MergedRegions: 2}},
		{files: [][4]int{{1, 1, 1, 1}, {1, 1, int(splitSizeMB), 1}},
			mergedIndex: []int{},
			stat:        restore.MergeRangesStat{TotalRegions: 2, MergedRegions: 2}},
		{files: [][4]int{{1, 1, 1, int(splitKeys)}, {1, 1, 1, 1}},
			mergedIndex: []int{},
			stat:        restore.MergeRangesStat{TotalRegions: 2, MergedRegions: 2}},
		{files: [][4]int{{1, 1, 1, 1}, {1, 1, 1, int(splitKeys)}},
			mergedIndex: []int{},
			stat:        restore.MergeRangesStat{TotalRegions: 2, MergedRegions: 2}},

		// 3 -> 1
		{files: [][4]int{{1, 1, 1, 1}, {1, 1, 1, 1}, {1, 1, 1, 1}},
			mergedIndex: []int{2},
			stat:        restore.MergeRangesStat{TotalRegions: 3, MergedRegions: 1}},
		// 3 -> 2, [split*1/3, split*1/3, split*1/2] -> [split*2/3, split*1/2]
		{files: [][4]int{{1, 1, int(splitSizeMB) / 3, 1}, {1, 1, int(splitSizeMB) / 3, 1}, {1, 1, int(splitSizeMB) / 2, 1}},
			mergedIndex: []int{1},
			stat:        restore.MergeRangesStat{TotalRegions: 3, MergedRegions: 2}},
		// 4 -> 2, [split*1/3, split*1/3, split*1/2, 1] -> [split*2/3, split*1/2 +1]
		{files: [][4]int{{1, 1, int(splitSizeMB) / 3, 1}, {1, 1, int(splitSizeMB) / 3, 1}, {1, 1, int(splitSizeMB) / 2, 1}, {1, 1, 1, 1}},
			mergedIndex: []int{1, 3},
			stat:        restore.MergeRangesStat{TotalRegions: 4, MergedRegions: 2}},
		// 5 -> 3, [split*1/3, split*1/3, split, split*1/2, 1] -> [split*2/3, split, split*1/2 +1]
		{files: [][4]int{{1, 1, int(splitSizeMB) / 3, 1}, {1, 1, int(splitSizeMB) / 3, 1}, {1, 1, int(splitSizeMB), 1}, {1, 1, int(splitSizeMB) / 2, 1}, {1, 1, 1, 1}},
			mergedIndex: []int{1, 2, 4},
			stat:        restore.MergeRangesStat{TotalRegions: 5, MergedRegions: 3}},

		// Do not merge ranges from different tables
		// 2 -> 2, [1, 1] -> [1, 1]
		{files: [][4]int{{1, 1, 1, 1}, {2, 1, 1, 1}},
			mergedIndex: []int{},
			stat:        restore.MergeRangesStat{TotalRegions: 2, MergedRegions: 2}},
		// 3 -> 2, [1@split*1/3, 2@split*1/3, 2@split*1/2] -> [1@split*1/3, 2@split*5/6]
		{files: [][4]int{{1, 1, int(splitSizeMB) / 3, 1}, {2, 1, int(splitSizeMB) / 3, 1}, {2, 1, int(splitSizeMB) / 2, 1}},
			mergedIndex: []int{0, 2},
			stat:        restore.MergeRangesStat{TotalRegions: 3, MergedRegions: 2}},
	}

	for _, cs := range cases {
		backupMeta := &kvproto.BackupMeta{}
		fb := fileBulder{}
		for _, f := range cs.files {
			backupMeta.Files = append(backupMeta.Files, fb.build(c, f[0], f[1], f[2], f[3])...)
		}
		stat, err := restore.MergeRanges(backupMeta)
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

func benchmarkMregeRanges(b *testing.B, files int) {
	backupMeta := &kvproto.BackupMeta{}
	fb := fileBulder{}
	for i := 0; i < files; i++ {
		backupMeta.Files = append(backupMeta.Files, fb.build(nil, 1, 1, 1, 1)...)
	}
	var err error
	for i := 0; i < b.N; i++ {
		_, err = restore.MergeRanges(backupMeta)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkMregeRanges1k(b *testing.B) {
	benchmarkMregeRanges(b, 1000)
}
func BenchmarkMregeRanges10k(b *testing.B) {
	benchmarkMregeRanges(b, 10000)
}
func BenchmarkMregeRanges50k(b *testing.B) {
	benchmarkMregeRanges(b, 50000)
}

func BenchmarkMregeRanges100k(b *testing.B) {
	benchmarkMregeRanges(b, 100000)
}
