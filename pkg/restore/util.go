package restore

import (
	"bytes"
	"context"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	restore_util "github.com/pingcap/tidb-tools/pkg/restore-util"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var recordPrefixSep = []byte("_r")

type files []*backup.File

func (fs files) MarshalLogArray(arr zapcore.ArrayEncoder) error {
	for i := range fs {
		err := arr.AppendReflected(fs[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// GetRewriteRules returns the rewrite rule of the new table and the old table.
func GetRewriteRules(newTable *model.TableInfo, oldTable *model.TableInfo) (map[int64]int64, *restore_util.RewriteRules) {
	// old table id -> new table id
	tableIDs := make(map[int64]int64)
	tableIDs[oldTable.ID] = newTable.ID
	// Add table partition id to map.
	if oldTable.Partition != nil {
		for _, oldPart := range oldTable.Partition.Definitions {
			for _, newPart := range newTable.Partition.Definitions {
				if oldPart.Name == newPart.Name {
					tableIDs[oldPart.ID] = newPart.ID
				}
			}
		}
	}
	// old index id -> new index id
	indexIDs := make(map[int64]int64)
	for _, oldIndex := range oldTable.Indices {
		for _, newIndex := range newTable.Indices {
			if oldIndex.Name == newIndex.Name {
				indexIDs[oldIndex.ID] = newIndex.ID
			}
		}
	}

	tableRules := make([]*import_sstpb.RewriteRule, 0)
	dataRules := make([]*import_sstpb.RewriteRule, 0)

	for oldTableID, newTableID := range tableIDs {
		tableRules = append(tableRules, &import_sstpb.RewriteRule{
			OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
			NewKeyPrefix: tablecodec.EncodeTablePrefix(newTableID),
		})

		dataRules = append(dataRules, &import_sstpb.RewriteRule{
			OldKeyPrefix: append(tablecodec.EncodeTablePrefix(oldTableID), recordPrefixSep...),
			NewKeyPrefix: append(tablecodec.EncodeTablePrefix(newTableID), recordPrefixSep...),
		})

		for oldIndexID, newIndexID := range indexIDs {
			dataRules = append(dataRules, &import_sstpb.RewriteRule{
				OldKeyPrefix: tablecodec.EncodeTableIndexPrefix(oldTableID, oldIndexID),
				NewKeyPrefix: tablecodec.EncodeTableIndexPrefix(newTableID, newIndexID),
			})
		}
	}

	return tableIDs, &restore_util.RewriteRules{
		Table: tableRules,
		Data:  dataRules,
	}
}

// getSSTMetaFromFile compares the keys in file, region and rewrite rules, then returns a sst meta.
// The range of the returned sst meta is [regionRule.NewKeyPrefix, append(regionRule.NewKeyPrefix, 0xff)]
func getSSTMetaFromFile(
	id []byte,
	file *backup.File,
	region *metapb.Region,
	regionRule *import_sstpb.RewriteRule,
) import_sstpb.SSTMeta {
	// Get the column family of the file by the file name.
	var cfName string
	if strings.Contains(file.GetName(), "default") {
		cfName = "default"
	} else if strings.Contains(file.GetName(), "write") {
		cfName = "write"
	}
	// Find the overlapped part between the file and the region.
	// Here we rewrites the keys to compare with the keys of the region.
	rangeStart := regionRule.GetNewKeyPrefix()
	//  rangeStart = max(rangeStart, region.StartKey)
	if bytes.Compare(rangeStart, region.GetStartKey()) < 0 {
		rangeStart = region.GetStartKey()
	}
	rangeEnd := append(append([]byte{}, regionRule.GetNewKeyPrefix()...), 0xff)
	// rangeEnd = min(rangeEnd, region.EndKey)
	if len(region.GetEndKey()) > 0 && bytes.Compare(rangeEnd, region.GetEndKey()) > 0 {
		rangeEnd = region.GetEndKey()
	}
	return import_sstpb.SSTMeta{
		Uuid:   id,
		CfName: cfName,
		Range: &import_sstpb.Range{
			Start: rangeStart,
			End:   rangeEnd,
		},
	}
}

type retryableFunc func() error
type continueFunc func(error) bool

func withRetry(
	retryableFunc retryableFunc,
	continueFunc continueFunc,
	attempts uint,
	delayTime time.Duration,
	maxDelayTime time.Duration,
) error {
	var lastErr error
	for i := uint(0); i < attempts; i++ {
		err := retryableFunc()
		if err != nil {
			lastErr = err
			// If this is the last attempt, do not wait
			if !continueFunc(err) || i == attempts-1 {
				break
			}
			delayTime = 2 * delayTime
			if delayTime > maxDelayTime {
				delayTime = maxDelayTime
			}
			time.Sleep(delayTime)
		} else {
			return nil
		}
	}
	return lastErr
}

// GetRanges returns the ranges of the files.
func GetRanges(files []*backup.File) []restore_util.Range {
	ranges := make([]restore_util.Range, 0, len(files))
	fileAppended := make(map[string]bool)

	for _, file := range files {
		// We skips all default cf files because we don't range overlap.
		if !fileAppended[file.GetName()] && strings.Contains(file.GetName(), "write") {
			ranges = append(ranges, restore_util.Range{
				StartKey: file.GetStartKey(),
				EndKey:   file.GetEndKey(),
			})
			fileAppended[file.GetName()] = true
		}
	}
	return ranges
}

// rules must be encoded
func findRegionRewriteRule(
	region *metapb.Region,
	rewriteRules *restore_util.RewriteRules,
) *import_sstpb.RewriteRule {
	for _, rule := range rewriteRules.Data {
		// regions may have the new prefix
		if bytes.HasPrefix(region.GetStartKey(), rule.GetNewKeyPrefix()) {
			return rule
		}
	}
	return nil
}

func encodeRewriteRules(rewriteRules *restore_util.RewriteRules) *restore_util.RewriteRules {
	encodedTableRules := make([]*import_sstpb.RewriteRule, 0, len(rewriteRules.Table))
	encodedDataRules := make([]*import_sstpb.RewriteRule, 0, len(rewriteRules.Data))
	for _, rule := range rewriteRules.Table {
		encodedTableRules = append(encodedTableRules, &import_sstpb.RewriteRule{
			OldKeyPrefix: encodeKeyPrefix(rule.GetOldKeyPrefix()),
			NewKeyPrefix: encodeKeyPrefix(rule.GetNewKeyPrefix()),
		})
	}
	for _, rule := range rewriteRules.Data {
		encodedDataRules = append(encodedDataRules, &import_sstpb.RewriteRule{
			OldKeyPrefix: encodeKeyPrefix(rule.GetOldKeyPrefix()),
			NewKeyPrefix: encodeKeyPrefix(rule.GetNewKeyPrefix()),
		})
	}
	return &restore_util.RewriteRules{
		Table: encodedTableRules,
		Data:  encodedDataRules,
	}
}

func encodeKeyPrefix(key []byte) []byte {
	encodedPrefix := make([]byte, 0)
	ungroupedLen := len(key) % 8
	encodedPrefix =
		append(encodedPrefix, codec.EncodeBytes([]byte{}, key[:len(key)-ungroupedLen])...)
	return append(encodedPrefix[:len(encodedPrefix)-9], key[len(key)-ungroupedLen:]...)
}

// Encode a raw key and find a rewrite rule to rewrite it.
// Return false if cannot find a related rewrite rule.
func rewriteRawKeyWithNewPrefix(key []byte, rewriteRules *restore_util.RewriteRules) ([]byte, bool) {
	if len(key) > 0 {
		ret := codec.EncodeBytes([]byte{}, key)
		for _, rule := range rewriteRules.Data {
			// regions may have the new prefix
			if bytes.HasPrefix(ret, rule.GetOldKeyPrefix()) {
				return bytes.Replace(ret, rule.GetOldKeyPrefix(), rule.GetNewKeyPrefix(), 1), true
			}
		}
		for _, rule := range rewriteRules.Table {
			// regions may have the new prefix
			if bytes.HasPrefix(ret, rule.GetOldKeyPrefix()) {
				return bytes.Replace(ret, rule.GetOldKeyPrefix(), rule.GetNewKeyPrefix(), 1), true
			}
		}
	}
	return []byte(""), false
}

func truncateTS(key []byte) []byte {
	return key[:len(key)-8]
}

// SplitRanges splits region by
// 1. data range after rewrite
// 2. rewrite rules
func SplitRanges(
	ctx context.Context,
	client *Client,
	ranges []restore_util.Range,
	rewriteRules *restore_util.RewriteRules,
	updateCh chan<- struct{},
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("SplitRegion", zap.Duration("costs", elapsed))
	}()
	splitter := restore_util.NewRegionSplitter(restore_util.NewClient(client.GetPDClient()))
	return splitter.Split(ctx, ranges, rewriteRules, func(*restore_util.Range) {
		updateCh <- struct{}{}
	})
}
