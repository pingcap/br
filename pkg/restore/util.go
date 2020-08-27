// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

var recordPrefixSep = []byte("_r")

// GetRewriteRules returns the rewrite rule of the new table and the old table.
func GetRewriteRules(
	newTable *model.TableInfo,
	oldTable *model.TableInfo,
	newTimeStamp uint64,
) *RewriteRules {
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

	return &RewriteRules{
		Table: tableRules,
		Data:  dataRules,
	}
}

// GetSSTMetaFromFile compares the keys in file, region and rewrite rules, then returns a sst conn.
// The range of the returned sst meta is [regionRule.NewKeyPrefix, append(regionRule.NewKeyPrefix, 0xff)].
func GetSSTMetaFromFile(
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

	// Append 10 * 0xff to make sure rangeEnd cover all file key
	// If choose to regionRule.NewKeyPrefix + 1, it may cause WrongPrefix here
	// https://github.com/tikv/tikv/blob/970a9bf2a9ea782a455ae579ad237aaf6cb1daec/
	// components/sst_importer/src/sst_importer.rs#L221
	suffix := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	rangeEnd := append(append([]byte{}, regionRule.GetNewKeyPrefix()...), suffix...)
	// rangeEnd = min(rangeEnd, region.EndKey)
	if len(region.GetEndKey()) > 0 && bytes.Compare(rangeEnd, region.GetEndKey()) > 0 {
		rangeEnd = region.GetEndKey()
	}

	if bytes.Compare(rangeStart, rangeEnd) > 0 {
		log.Fatal("range start exceed range end",
			zap.Stringer("start", utils.WrapKey(rangeStart)),
			zap.Stringer("end", utils.WrapKey(rangeEnd)))
	}

	log.Debug("get sstMeta",
		utils.ZapFile(file),
		zap.Stringer("rangeStart", utils.WrapKey(rangeStart)),
		zap.Stringer("rangeEnd", utils.WrapKey(rangeEnd)))

	return import_sstpb.SSTMeta{
		Uuid:   id,
		CfName: cfName,
		Range: &import_sstpb.Range{
			Start: rangeStart,
			End:   rangeEnd,
		},
		Length:      file.GetSize_(),
		RegionId:    region.GetId(),
		RegionEpoch: region.GetRegionEpoch(),
	}
}

// MakeDBPool makes a session pool with specficated size by sessionFactory.
func MakeDBPool(size uint, dbFactory func() (*DB, error)) ([]*DB, error) {
	dbPool := make([]*DB, 0, size)
	for i := uint(0); i < size; i++ {
		db, e := dbFactory()
		if e != nil {
			return dbPool, e
		}
		dbPool = append(dbPool, db)
	}
	return dbPool, nil
}

// EstimateRangeSize estimates the total range count by file.
func EstimateRangeSize(files []*backup.File) int {
	result := 0
	for _, f := range files {
		if strings.HasSuffix(f.GetName(), "_write.sst") {
			result++
		}
	}
	return result
}

// ValidateFileRanges checks and returns the ranges of the files.
func ValidateFileRanges(
	files []*backup.File,
	rewriteRules *RewriteRules,
) ([]rtree.Range, error) {
	ranges := make([]rtree.Range, 0, len(files))
	fileAppended := make(map[string]bool)

	for _, file := range files {
		// We skips all default cf files because we don't range overlap.
		if !fileAppended[file.GetName()] && strings.Contains(file.GetName(), "write") {
			rng, err := validateAndGetFileRange(file, rewriteRules)
			if err != nil {
				return nil, err
			}
			ranges = append(ranges, rng)
			fileAppended[file.GetName()] = true
		}
	}
	return ranges, nil
}

// MapTableToFiles makes a map that mapping table ID to its backup files.
// aware that one file can and only can hold one table.
func MapTableToFiles(files []*backup.File) map[int64][]*backup.File {
	result := map[int64][]*backup.File{}
	for _, file := range files {
		tableID := tablecodec.DecodeTableID(file.GetStartKey())
		tableEndID := tablecodec.DecodeTableID(file.GetEndKey())
		if tableID != tableEndID {
			log.Panic("key range spread between many files.",
				zap.String("file name", file.Name),
				zap.Stringer("start key", utils.WrapKey(file.GetStartKey())),
				zap.Stringer("end key", utils.WrapKey(file.GetEndKey())))
		}
		if tableID == 0 {
			log.Panic("invalid table key of file",
				zap.String("file name", file.Name),
				zap.Stringer("start key", utils.WrapKey(file.GetStartKey())),
				zap.Stringer("end key", utils.WrapKey(file.GetEndKey())))
		}
		result[tableID] = append(result[tableID], file)
	}
	return result
}

// GoValidateFileRanges validate files by a stream of tables and yields tables with range.
func GoValidateFileRanges(
	ctx context.Context,
	tableStream <-chan CreatedTable,
	fileOfTable map[int64][]*backup.File,
	errCh chan<- error,
) <-chan TableWithRange {
	// Could we have a smaller outCh size?
	outCh := make(chan TableWithRange, len(fileOfTable))
	go func() {
		defer close(outCh)
		defer log.Info("all range generated")
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case t, ok := <-tableStream:
				if !ok {
					return
				}
				files := fileOfTable[t.OldTable.Info.ID]
				if partitions := t.OldTable.Info.Partition; partitions != nil {
					log.Debug("table partition",
						zap.Stringer("database", t.OldTable.Db.Name),
						zap.Stringer("table", t.Table.Name),
						zap.Any("partition info", partitions),
					)
					for _, partition := range partitions.Definitions {
						files = append(files, fileOfTable[partition.ID]...)
					}
				}
				ranges, err := ValidateFileRanges(files, t.RewriteRule)
				if err != nil {
					errCh <- err
					return
				}
				tableWithRange := TableWithRange{
					CreatedTable: t,
					Range:        AttachFilesToRanges(files, ranges),
				}
				log.Debug("sending range info",
					zap.Stringer("table", t.Table.Name),
					zap.Int("files", len(files)),
					zap.Int("range size", len(ranges)),
					zap.Int("output channel size", len(outCh)))
				outCh <- tableWithRange
			}
		}
	}()
	return outCh
}

// validateAndGetFileRange validates a file, if success, return the key range of this file.
func validateAndGetFileRange(file *backup.File, rules *RewriteRules) (rtree.Range, error) {
	err := ValidateFileRewriteRule(file, rules)
	if err != nil {
		return rtree.Range{}, err
	}
	startID := tablecodec.DecodeTableID(file.GetStartKey())
	endID := tablecodec.DecodeTableID(file.GetEndKey())
	if startID != endID {
		log.Error("table ids mismatch",
			zap.Int64("startID", startID),
			zap.Int64("endID", endID),
			utils.ZapFile(file))
		return rtree.Range{}, errors.New("table ids mismatch")
	}
	r := rtree.Range{StartKey: file.GetStartKey(), EndKey: file.GetEndKey()}
	return r, nil
}

// AttachFilesToRanges attach files to ranges.
// Panic if range is overlapped or no range for files.
func AttachFilesToRanges(
	files []*backup.File,
	ranges []rtree.Range,
) []rtree.Range {
	rangeTree := rtree.NewRangeTree()
	for _, rg := range ranges {
		rangeTree.Update(rg)
	}
	for _, f := range files {
		rg := rangeTree.Find(&rtree.Range{
			StartKey: f.GetStartKey(),
			EndKey:   f.GetEndKey(),
		})
		if rg == nil {
			log.Fatal("range not found",
				zap.Stringer("startKey", utils.WrapKey(f.GetStartKey())),
				zap.Stringer("endKey", utils.WrapKey(f.GetEndKey())))
		}
		file := *f
		rg.Files = append(rg.Files, &file)
	}
	if rangeTree.Len() != len(ranges) {
		log.Fatal("ranges overlapped",
			zap.Int("ranges length", len(ranges)),
			zap.Int("tree length", rangeTree.Len()))
	}
	sortedRanges := rangeTree.GetSortedRanges()
	return sortedRanges
}

// ValidateFileRewriteRule uses rewrite rules to validate the ranges of a file.
func ValidateFileRewriteRule(file *backup.File, rewriteRules *RewriteRules) error {
	// Check if the start key has a matched rewrite key
	_, startRule := rewriteRawKey(file.GetStartKey(), rewriteRules)
	if rewriteRules != nil && startRule == nil {
		tableID := tablecodec.DecodeTableID(file.GetStartKey())
		log.Error(
			"cannot find rewrite rule for file start key",
			zap.Int64("tableID", tableID),
			utils.ZapFile(file),
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
			utils.ZapFile(file),
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
			utils.ZapFile(file),
		)
		return errors.Errorf("unexpected rewrite rules")
	}
	return nil
}

// Rewrites a raw key and returns a encoded key.
func rewriteRawKey(key []byte, rewriteRules *RewriteRules) ([]byte, *import_sstpb.RewriteRule) {
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

func matchOldPrefix(key []byte, rewriteRules *RewriteRules) *import_sstpb.RewriteRule {
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

func matchNewPrefix(key []byte, rewriteRules *RewriteRules) *import_sstpb.RewriteRule {
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
	if len(key) == 0 {
		return nil
	}
	return key[:len(key)-8]
}

// SplitRanges splits region by
// 1. data range after rewrite.
// 2. rewrite rules.
func SplitRanges(
	ctx context.Context,
	client *Client,
	ranges []rtree.Range,
	rewriteRules *RewriteRules,
	updateCh glue.Progress,
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		summary.CollectDuration("split region", elapsed)
	}()
	splitter := NewRegionSplitter(NewSplitClient(client.GetPDClient(), client.GetTLSConfig()))

	return splitter.Split(ctx, ranges, rewriteRules, func(keys [][]byte) {
		for range keys {
			updateCh.Inc()
		}
	})
}

func rewriteFileKeys(file *backup.File, rewriteRules *RewriteRules) (startKey, endKey []byte, err error) {
	startID := tablecodec.DecodeTableID(file.GetStartKey())
	endID := tablecodec.DecodeTableID(file.GetEndKey())
	var rule *import_sstpb.RewriteRule
	if startID == endID {
		startKey, rule = rewriteRawKey(file.GetStartKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			log.Error("cannot find rewrite rule",
				zap.Stringer("startKey", utils.WrapKey(file.GetStartKey())),
				zap.Reflect("rewrite table", rewriteRules.Table),
				zap.Reflect("rewrite data", rewriteRules.Data))
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
			zap.Stringer("startKey", utils.WrapKey(startKey)),
			zap.Stringer("endKey", utils.WrapKey(endKey)))
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

// PaginateScanRegion scan regions with a limit pagination and
// return all regions at once.
// It reduces max gRPC message size.
func PaginateScanRegion(
	ctx context.Context, client SplitClient, startKey, endKey []byte, limit int,
) ([]*RegionInfo, error) {
	if len(endKey) != 0 && bytes.Compare(startKey, endKey) >= 0 {
		return nil, errors.Errorf("startKey >= endKey, startKey %s, endkey %s",
			hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	}

	regions := []*RegionInfo{}
	for {
		batch, err := client.ScanRegions(ctx, startKey, endKey, limit)
		if err != nil {
			return nil, errors.Trace(err)
		}
		regions = append(regions, batch...)
		if len(batch) < limit {
			// No more region
			break
		}
		startKey = batch[len(batch)-1].Region.GetEndKey()
		if len(startKey) == 0 ||
			(len(endKey) > 0 && bytes.Compare(startKey, endKey) >= 0) {
			// All key space have scanned
			break
		}
	}
	return regions, nil
}

func hasRejectStorePeer(
	ctx context.Context,
	client SplitClient,
	regionID uint64,
	rejectStores map[uint64]bool,
) (bool, error) {
	regionInfo, err := client.GetRegionByID(ctx, regionID)
	if err != nil {
		return false, err
	}
	if regionInfo == nil {
		return false, nil
	}
	for _, peer := range regionInfo.Region.GetPeers() {
		if rejectStores[peer.GetStoreId()] {
			return true, nil
		}
	}
	retryTimes := ctx.Value(retryTimes).(int)
	if retryTimes > 10 {
		log.Warn("get region info", utils.ZapRegion(regionInfo.Region))
	}
	return false, nil
}

func waitForRemoveRejectStores(
	ctx context.Context,
	client SplitClient,
	regionInfo *RegionInfo,
	rejectStores map[uint64]bool,
) bool {
	interval := RejectStoreCheckInterval
	regionID := regionInfo.Region.GetId()
	for i := 0; i < RejectStoreCheckRetryTimes; i++ {
		ctx1 := context.WithValue(ctx, retryTimes, i)
		ok, err := hasRejectStorePeer(ctx1, client, regionID, rejectStores)
		if err != nil {
			log.Warn("wait for rejecting store failed",
				utils.ZapRegion(regionInfo.Region),
				zap.Error(err))
			return false
		}
		// Do not have any peer in the rejected store, return true
		if !ok {
			return true
		}
		interval = 2 * interval
		if interval > RejectStoreMaxCheckInterval {
			interval = RejectStoreMaxCheckInterval
		}
		time.Sleep(interval)
	}

	return false
}

// ZapTables make zap field of table for debuging, including table names.
func ZapTables(tables []CreatedTable) zapcore.Field {
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, fmt.Sprintf("%s.%s", t.OldTable.Db.Name, t.OldTable.Info.Name))
	}
	return zap.Strings("tables", tableNames)
}

// ZapRanges make zap fields for debuging, which contains kv, size and count of ranges.
func ZapRanges(ranges []rtree.Range) []zapcore.Field {
	totalKV := uint64(0)
	totalSize := uint64(0)
	for _, r := range ranges {
		for _, f := range r.Files {
			totalKV += f.GetTotalKvs()
			totalSize += f.GetTotalBytes()
		}
	}

	return []zap.Field{
		zap.Int("ranges", len(ranges)),
		zap.Uint64("total kv", totalKV),
		zap.Uint64("total size", totalSize),
	}
}

func parseQuoteName(name string) (string, string) {
	schemaRune := make([]rune, 0, len(name))
	for i, c := range name {
		schemaRune = append(schemaRune, c)
		if i == 0 {
			continue
		}
		if name[i] == '`' && name[i-1] != '`' {
			break
		}
	}
	schema := string(schemaRune)
	return unQuoteName(schema), unQuoteName(strings.TrimPrefix(name, schema+"."))
}

func unQuoteName(name string) string {
	name = strings.TrimLeft(name, "`")
	return strings.TrimRight(name, "`")
}