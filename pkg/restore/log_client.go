// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"go.uber.org/zap"
)

const (
	tableLogPrefix = "t_"
	logPrefix      = "cdclog"

	metaFile      = "log.meta"
	ddlEventsDir  = "ddls"
	ddlFilePrefix = "ddl"

	maxUint64 = ^uint64(0)
)

type LogMeta struct {
	Names            map[int64]string `json:"names"`
	GlobalResolvedTS uint64           `json:"global_resolved_ts"`
}

// LogClient sends requests to restore files.
type LogClient struct {
	restoreClient *Client

	// range of log backup
	startTS uint64
	endTs   uint64

	// meta info parsed from log backup
	meta *LogMeta

	tableFilter filter.Filter
}

// NewLogRestoreClient returns a new LogRestoreClient.
func NewLogRestoreClient(
	ctx context.Context,
	restoreClient *Client,
	startTs uint64,
	endTS uint64,
	tableFilter filter.Filter,
) (*LogClient, error) {
	var err error
	if endTS == 0 {
		// means restore all log data,
		// so we get current ts from restore cluster
		endTS, err = restoreClient.GetTS(ctx)
		if err != nil {
			return nil, err
		}
	}

	return &LogClient{
		restoreClient,
		startTs,
		endTS,
		new(LogMeta),
		tableFilter,
	}, nil
}

func (l *LogClient) tsInRange(ts uint64) bool {
	if ts < l.startTS || ts > l.endTs {
		return false
	}
	return true
}

func (l *LogClient) needRestoreDDL(fileName string) (bool, error) {
	names := strings.Split(fileName, ".")
	if len(names) != 2 {
		log.Warn("found wrong format of ddl file", zap.String("file", fileName))
		return false, nil
	}
	if names[0] != ddlFilePrefix {
		log.Warn("file doesn't start with ddl", zap.String("file", fileName))
		return false, nil
	}
	ts, err := strconv.ParseUint(names[1], 10, 64)
	if err != nil {
		return false, errors.AddStack(err)
	}
	ts = maxUint64 - ts
	if l.tsInRange(ts) {
		return true, nil
	}
	return false, nil
}

func (l *LogClient) collectDDLFiles(ctx context.Context) ([]string, error) {
	ddlFiles := make([]string, 0)
	err := l.restoreClient.storage.WalkDir(ctx, ddlEventsDir, -1, func(path string, size int64) error {
		fileName := filepath.Base(path)
		shouldRestore, err := l.needRestoreDDL(fileName)
		if err != nil {
			return err
		}
		if shouldRestore {
			ddlFiles = append(ddlFiles, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ddlFiles, nil
}

func (l *LogClient) needRestoreRowChange(fileName string) (bool, error) {
	names := strings.Split(fileName, ".")
	if len(names) != 2 {
		log.Warn("found wrong format of row changes file", zap.String("file", fileName))
		return false, nil
	}
	if names[0] != logPrefix {
		log.Warn("file doesn't start with row changes file", zap.String("file", fileName))
		return false, nil
	}
	ts, err := strconv.ParseUint(names[1], 10, 64)
	if err != nil {
		return false, errors.AddStack(err)
	}
	if l.tsInRange(ts) {
		return true, nil
	}
	return false, nil
}

func (l *LogClient) collectRowChangeFiles(ctx context.Context) (map[int64][]string, error) {
	// we should collect all related tables row change files
	// by log meta info and by given table filter
	rowChangeFiles := make(map[int64][]string)

	// need collect restore tableIDs
	tableIDs := make([]int64, 0, len(l.meta.Names))
	for tableID, name := range l.meta.Names {
		schema, table := parseQuoteName(name)
		if !l.tableFilter.MatchTable(schema, table) {
			log.Info("filter tables",
				zap.String("schema", schema),
				zap.String("table", table),
				zap.Int64("tableID", tableID),
			)
			continue
		}
		tableIDs = append(tableIDs, tableID)
	}

	for _, tableID := range tableIDs {
		// FIXME update log meta logic here
		dir := fmt.Sprintf("%s%d", tableLogPrefix, tableID)
		err := l.restoreClient.storage.WalkDir(ctx, dir, -1, func(path string, size int64) error {
			fileName := filepath.Base(path)
			shouldRestore, err := l.needRestoreRowChange(fileName)
			if err != nil {
				return err
			}
			if shouldRestore {
				if _, ok := rowChangeFiles[tableID]; ok {
					rowChangeFiles[tableID] = append(rowChangeFiles[tableID], path)
				} else {
					rowChangeFiles[tableID] = []string{path}
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return rowChangeFiles, nil
}

func (l *LogClient) restoreFiles(
	ctx context.Context,
	ddlFiles []string,
	rowChangedFiles map[int64][]string) error {

	return nil
}

// RestoreLogData restore specify log data from storage.
func (l *LogClient) RestoreLogData(ctx context.Context) error {
	// 1. Retrieve log data from storage
	// 2. Find proper data by TS range
	// 3. Encode and ingest data to tikv

	// parse meta file
	data, err := l.restoreClient.storage.Read(ctx, metaFile)
	if err != nil {
		return errors.Trace(err)
	}
	err = json.Unmarshal(data, l.meta)
	if err != nil {
		return errors.Trace(err)
	}

	if l.startTS > l.meta.GlobalResolvedTS {
		return errors.Errorf("start ts:%d is greater than resolved ts:%d",
			l.startTS, l.meta.GlobalResolvedTS)
	}
	if l.endTs > l.meta.GlobalResolvedTS {
		log.Info("end ts is greater than resolved ts,"+
			" to keep consistency we only recover data until resolved ts",
			zap.Uint64("end ts", l.endTs),
			zap.Uint64("resolved ts", l.meta.GlobalResolvedTS))
		l.endTs = l.meta.GlobalResolvedTS
	}

	// collect ddl files
	ddlFiles, err := l.collectDDLFiles(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// collect row change files
	rowChangesFiles, err := l.collectRowChangeFiles(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// restore files
	return l.restoreFiles(ctx, ddlFiles, rowChangesFiles)
}
