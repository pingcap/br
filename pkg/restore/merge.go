// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"github.com/pingcap/errors"
	kvproto "github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"

	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/utils"
)

const (
	// DefaultgeRegionSizeBytes is the default region split size, 96MB.
	// See https://github.com/tikv/tikv/blob/v4.0.8/components/raftstore/src/coprocessor/config.rs#L35-L38
	DefaultgeRegionSizeBytes = 96 * utils.MB

	// DefaultMergeRegionKeyCount is the default region key count, 960000.
	DefaultMergeRegionKeyCount = 960000
)

// MergeRangesStat holds statistics for the MergeRanges.
type MergeRangesStat struct {
	TotalFiles           int
	TotalWriteCFFile     int
	TotalDefaultCFFile   int
	TotalRegions         int
	RegionKeysAvg        int
	RegionBytesAvg       int
	MergedFiles          int
	MergedRegions        int
	MergedRegionKeysAvg  int
	MergedRegionBytesAvg int
}

// MergeRanges merges small ranges into a bigger one.
// It speeds up restoring a backup that contains many small ranges (regions),
// as it reduces split region and scatter region.
// Note: this function modify backupMeta in place.
func MergeRanges(backupMeta *kvproto.BackupMeta, splitSizeBytes, splitKeyCount uint64) (*MergeRangesStat, error) {
	// Skip if the backup is empty.
	if len(backupMeta.Files) == 0 {
		return &MergeRangesStat{}, nil
	}
	totalBytes := uint64(0)
	totalKvs := uint64(0)
	totalFiles := len(backupMeta.Files)
	writeCFFile := 0
	defaultCFFile := 0
	filesMap := make(map[string][]*kvproto.File)
	for _, file := range backupMeta.Files {
		filesMap[string(file.StartKey)] = append(filesMap[string(file.StartKey)], file)
		if file.Cf == "write" {
			writeCFFile++
		} else {
			defaultCFFile++
		}
		totalBytes += file.TotalBytes
		totalKvs += file.TotalKvs
	}
	rangeTree := rtree.NewRangeTree()
	// Check if files are overlapped
	for key := range filesMap {
		files := filesMap[key]
		if out := rangeTree.InsertRange(rtree.Range{
			StartKey: files[0].GetStartKey(),
			EndKey:   files[0].GetEndKey(),
			Files:    files,
		}); out != nil {
			return nil, errors.Annotatef(berrors.ErrRestoreInvalidRange,
				"duplicate range %s files %+v", out, files)
		}
	}

	needMerge := func(left, right *rtree.Range) bool {
		leftBytes, leftKeys := left.BytesAndKeys()
		rightBytes, rightKeys := right.BytesAndKeys()
		if rightBytes == 0 {
			return true
		}
		if leftBytes+rightBytes > splitSizeBytes {
			return false
		}
		if leftKeys+rightKeys > splitKeyCount {
			return false
		}
		// Do not merge ranges in different tables.
		if tablecodec.DecodeTableID(kv.Key(left.StartKey)) != tablecodec.DecodeTableID(kv.Key(right.StartKey)) {
			return false
		}
		return true
	}
	sortedRanges := rangeTree.GetSortedRanges()
	for i := 1; i < len(sortedRanges); {
		if !needMerge(&sortedRanges[i-1], &sortedRanges[i]) {
			i++
			continue
		}
		sortedRanges[i-1].EndKey = sortedRanges[i].EndKey
		startKey, endKey := sortedRanges[i-1].StartKey, sortedRanges[i-1].EndKey
		sortedRanges[i-1].Files = append(sortedRanges[i-1].Files, sortedRanges[i].Files...)
		for j := range sortedRanges[i-1].Files {
			sortedRanges[i-1].Files[j].StartKey = startKey
			sortedRanges[i-1].Files[j].EndKey = endKey
		}
		// TODO: this is slow when there are lots of ranges need to merge.
		sortedRanges = append(sortedRanges[:i], sortedRanges[i+1:]...)
	}

	backupMeta.Files = backupMeta.Files[:0]
	for i := range sortedRanges {
		backupMeta.Files = append(backupMeta.Files, sortedRanges[i].Files...)
	}

	regionBytesAvg := totalBytes / uint64(writeCFFile)
	regionKeysAvg := totalKvs / uint64(writeCFFile)
	mergedRegionBytesAvg := totalBytes / uint64(len(sortedRanges))
	mergedRegionKeysAvg := totalKvs / uint64(len(sortedRanges))

	return &MergeRangesStat{
		TotalFiles:           totalFiles,
		TotalWriteCFFile:     writeCFFile,
		TotalDefaultCFFile:   defaultCFFile,
		TotalRegions:         writeCFFile,
		RegionKeysAvg:        int(regionKeysAvg),
		RegionBytesAvg:       int(regionBytesAvg),
		MergedFiles:          len(backupMeta.Files),
		MergedRegions:        len(sortedRanges),
		MergedRegionKeysAvg:  int(mergedRegionKeysAvg),
		MergedRegionBytesAvg: int(mergedRegionBytesAvg),
	}, nil
}
