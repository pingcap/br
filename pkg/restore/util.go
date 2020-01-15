package restore

import (
	"bytes"
	"context"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/errors"
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

	"github.com/pingcap/br/pkg/summary"
)

var recordPrefixSep = []byte("_r")

type files []*backup.File

func (fs files) MarshalLogArray(arr zapcore.ArrayEncoder) error {
	for i := range fs {
		arr.AppendString(fs[i].String())
	}
	return nil
}

type rules []*import_sstpb.RewriteRule

func (rs rules) MarshalLogArray(arr zapcore.ArrayEncoder) error {
	for i := range rs {
		arr.AppendString(rs[i].String())
	}
	return nil
}

// idAllocator always returns a specified ID
type idAllocator struct {
	id int64
}

func newIDAllocator(id int64) *idAllocator {
	return &idAllocator{id: id}
}

func (alloc *idAllocator) Alloc(tableID int64, n uint64) (min int64, max int64, err error) {
	return alloc.id, alloc.id, nil
}

func (alloc *idAllocator) Rebase(tableID, newBase int64, allocIDs bool) error {
	return nil
}

func (alloc *idAllocator) Base() int64 {
	return alloc.id
}

func (alloc *idAllocator) End() int64 {
	return alloc.id
}

func (alloc *idAllocator) NextGlobalAutoID(tableID int64) (int64, error) {
	return alloc.id, nil
}

// GetRewriteRules returns the rewrite rule of the new table and the old table.
func GetRewriteRules(
	newTable *model.TableInfo,
	oldTable *model.TableInfo,
	newTimeStamp uint64,
) *restore_util.RewriteRules {
	tableIDs := make(map[int64]int64)
	tableIDs[oldTable.ID] = newTable.ID
	if oldTable.Partition != nil {
		for _, srcPart := range oldTable.Partition.Definitions {
			for _, destPart := range newTable.Partition.Definitions {
				if srcPart.Name == destPart.Name {
					tableIDs[srcPart.ID] = destPart.ID
				}
			}
		}
	}
	indexIDs := make(map[int64]int64)
	for _, srcIndex := range oldTable.Indices {
		for _, destIndex := range newTable.Indices {
			if srcIndex.Name == destIndex.Name {
				indexIDs[srcIndex.ID] = destIndex.ID
			}
		}
	}

	tableRules := make([]*import_sstpb.RewriteRule, 0)
	dataRules := make([]*import_sstpb.RewriteRule, 0)
	for oldTableID, newTableID := range tableIDs {
		tableRules = append(tableRules, &import_sstpb.RewriteRule{
			OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
			NewKeyPrefix: tablecodec.EncodeTablePrefix(newTableID),
			NewTimestamp: newTimeStamp,
		})
		dataRules = append(dataRules, &import_sstpb.RewriteRule{
			OldKeyPrefix: append(tablecodec.EncodeTablePrefix(oldTableID), recordPrefixSep...),
			NewKeyPrefix: append(tablecodec.EncodeTablePrefix(newTableID), recordPrefixSep...),
			NewTimestamp: newTimeStamp,
		})
		for oldIndexID, newIndexID := range indexIDs {
			dataRules = append(dataRules, &import_sstpb.RewriteRule{
				OldKeyPrefix: tablecodec.EncodeTableIndexPrefix(oldTableID, oldIndexID),
				NewKeyPrefix: tablecodec.EncodeTableIndexPrefix(newTableID, newIndexID),
				NewTimestamp: newTimeStamp,
			})
		}
	}

	return &restore_util.RewriteRules{
		Table: tableRules,
		Data:  dataRules,
	}
}

// getSSTMetaFromFile compares the keys in file, region and rewrite rules, then returns a sst conn.
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

// ValidateFileRanges checks and returns the ranges of the files.
func ValidateFileRanges(
	files []*backup.File,
	rewriteRules *restore_util.RewriteRules,
) ([]restore_util.Range, error) {
	ranges := make([]restore_util.Range, 0, len(files))
	fileAppended := make(map[string]bool)

	for _, file := range files {
		// We skips all default cf files because we don't range overlap.
		if !fileAppended[file.GetName()] && strings.Contains(file.GetName(), "write") {
			err := ValidateFileRewriteRule(file, rewriteRules)
			if err != nil {
				return nil, err
			}
			startID := tablecodec.DecodeTableID(file.GetStartKey())
			endID := tablecodec.DecodeTableID(file.GetEndKey())
			if startID != endID {
				log.Error("table ids dont match",
					zap.Int64("startID", startID),
					zap.Int64("endID", endID),
					zap.Stringer("file", file))
				return nil, errors.New("table ids dont match")
			}
			ranges = append(ranges, restore_util.Range{
				StartKey: file.GetStartKey(),
				EndKey:   file.GetEndKey(),
			})
			fileAppended[file.GetName()] = true
		}
	}
	return ranges, nil
}

// ValidateFileRewriteRule uses rewrite rules to validate the ranges of a file
func ValidateFileRewriteRule(file *backup.File, rewriteRules *restore_util.RewriteRules) error {
	// Check if the start key has a matched rewrite key
	_, startRule := rewriteRawKey(file.GetStartKey(), rewriteRules)
	if rewriteRules != nil && startRule == nil {
		tableID := tablecodec.DecodeTableID(file.GetStartKey())
		log.Error(
			"cannot find rewrite rule for file start key",
			zap.Int64("tableID", tableID),
			zap.Stringer("file", file),
		)
		return errors.Errorf("cannot find rewrite rule")
	}
	// Check if the end key has a matched rewrite key
	_, endRule := rewriteRawKey(file.GetEndKey(), rewriteRules)
	if rewriteRules != nil && endRule == nil {
		tableID := tablecodec.DecodeTableID(file.GetEndKey())
		log.Error(
			"cannot find rewrite rule for file end key",
			zap.Int64("tableID", tableID),
			zap.Stringer("file", file),
		)
		return errors.Errorf("cannot find rewrite rule")
	}
	// the new prefix of the start rule must equal or less than the new prefix of the end rule
	if bytes.Compare(startRule.GetNewKeyPrefix(), endRule.GetNewKeyPrefix()) > 0 {
		startTableID := tablecodec.DecodeTableID(file.GetStartKey())
		endTableID := tablecodec.DecodeTableID(file.GetEndKey())
		log.Error(
			"unexpected rewrite rules",
			zap.Int64("startTableID", startTableID),
			zap.Int64("endTableID", endTableID),
			zap.Stringer("startRule", startRule),
			zap.Stringer("endRule", endRule),
			zap.Stringer("file", file),
		)
		return errors.Errorf("unexpected rewrite rules")
	}
	return nil
}

// Rewrites a raw key and returns a encoded key
func rewriteRawKey(key []byte, rewriteRules *restore_util.RewriteRules) ([]byte, *import_sstpb.RewriteRule) {
	if rewriteRules == nil {
		return codec.EncodeBytes([]byte{}, key), nil
	}
	if len(key) > 0 {
		rule := matchOldPrefix(key, rewriteRules)
		ret := bytes.Replace(key, rule.GetOldKeyPrefix(), rule.GetNewKeyPrefix(), 1)
		return codec.EncodeBytes([]byte{}, ret), rule
	}
	return nil, nil
}

func matchOldPrefix(key []byte, rewriteRules *restore_util.RewriteRules) *import_sstpb.RewriteRule {
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(key, rule.GetOldKeyPrefix()) {
			return rule
		}
	}
	for _, rule := range rewriteRules.Table {
		if bytes.HasPrefix(key, rule.GetOldKeyPrefix()) {
			return rule
		}
	}
	return nil
}

func matchNewPrefix(key []byte, rewriteRules *restore_util.RewriteRules) *import_sstpb.RewriteRule {
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(key, rule.GetNewKeyPrefix()) {
			return rule
		}
	}
	for _, rule := range rewriteRules.Table {
		if bytes.HasPrefix(key, rule.GetNewKeyPrefix()) {
			return rule
		}
	}
	return nil
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
		summary.CollectDuration("split region", elapsed)
	}()
	splitter := restore_util.NewRegionSplitter(restore_util.NewClient(client.GetPDClient()))
	return splitter.Split(ctx, ranges, rewriteRules, func(keys [][]byte) {
		for range keys {
			updateCh <- struct{}{}
		}
	})
}

func rewriteFileKeys(file *backup.File, rewriteRules *restore_util.RewriteRules) (startKey, endKey []byte, err error) {
	startID := tablecodec.DecodeTableID(file.GetStartKey())
	endID := tablecodec.DecodeTableID(file.GetEndKey())
	var rule *import_sstpb.RewriteRule
	if startID == endID {
		startKey, rule = rewriteRawKey(file.GetStartKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			err = errors.New("cannot find rewrite rule for start key")
			return
		}
		endKey, rule = rewriteRawKey(file.GetEndKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			err = errors.New("cannot find rewrite rule for end key")
			return
		}
	} else {
		log.Error("table ids dont matched",
			zap.Int64("startID", startID),
			zap.Int64("endID", endID),
			zap.Binary("startKey", startKey),
			zap.Binary("endKey", endKey))
		err = errors.New("illegal table id")
	}
	return
}

func encodeKeyPrefix(key []byte) []byte {
	encodedPrefix := make([]byte, 0)
	ungroupedLen := len(key) % 8
	encodedPrefix = append(encodedPrefix, codec.EncodeBytes([]byte{}, key[:len(key)-ungroupedLen])...)
	return append(encodedPrefix[:len(encodedPrefix)-9], key[len(key)-ungroupedLen:]...)
}

// escape the identifier for pretty-printing.
// For instance, the identifier "foo `bar`" will become "`foo ``bar```".
// The sqlMode controls whether to escape with backquotes (`) or double quotes
// (`"`) depending on whether mysql.ModeANSIQuotes is enabled.
func escapeTableName(cis model.CIStr) string {
	quote := "`"
	return quote + strings.Replace(cis.O, quote, quote+quote, -1) + quote
}
