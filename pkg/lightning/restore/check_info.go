// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"

	"github.com/pingcap/br/pkg/lightning/backend"
	"github.com/pingcap/br/pkg/lightning/checkpoints"
	"github.com/pingcap/br/pkg/storage"

	"github.com/minio/minio/pkg/disk"
)

const (
	pdWriteFlow           = "pd/api/v1/regions/writeflow"
	pdReadFlow            = "pd/api/v1/regions/readflow"
	OnlineBytesLimitation = 1 << 20
	OnlineKeysLimitation  = 100

	pdStores    = "pd/api/v1/stores"
	pdReplicate = "pd/api/v1/config/replicate"

	defaultCSVSize = 10 << 30
)

func (rc *Controller) isSourceInLocal() bool {
	return strings.HasPrefix(rc.store.URI(), "file:")
}

func (rc *Controller) getReplicaCount(ctx context.Context) (uint64, error) {
	result := &config.ReplicationConfig{}
	err := rc.tls.WithHost(rc.cfg.TiDB.PdAddr).GetJSON(ctx, pdReplicate, &result)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return result.MaxReplicas, nil
}

// ClusterIsOnline check cluster is online. this test can be skipped by user requirement.
func (rc *Controller) ClusterIsOnline(ctx context.Context) error {
	passed := true
	message := "cluster has no other loads"
	defer func() {
		rc.checkTemplate.PerformanceCollect(passed, message)
	}()

	result := api.RegionsInfo{}
	err := rc.tls.WithHost(rc.cfg.TiDB.PdAddr).GetJSON(ctx, pdWriteFlow, &result)
	if err != nil {
		return errors.Trace(err)
	}
	for _, region := range result.Regions {
		if region.WrittenBytes > OnlineBytesLimitation || region.WrittenKeys > OnlineKeysLimitation {
			passed = false
			message = fmt.Sprintf("cluster has write flow more than expection")
			return nil
		}
	}
	result = api.RegionsInfo{}
	err = rc.tls.WithHost(rc.cfg.TiDB.PdAddr).GetJSON(ctx, pdReadFlow, &result)
	if err != nil {
		return errors.Trace(err)
	}
	for _, region := range result.Regions {
		if region.ReadBytes > OnlineBytesLimitation || region.ReadKeys > OnlineKeysLimitation {
			passed = false
			message = fmt.Sprintf("cluster has read flow more than expection")
			return nil
		}
	}
	return nil
}

// ClusterResource check cluster has enough resource to import data. this test can by skipped.
func (rc *Controller) ClusterResource(ctx context.Context) error {
	passed := true
	message := "cluster resources are rich"
	defer func() {
		rc.checkTemplate.CriticalCollect(passed, message)
	}()

	result := api.StoresInfo{}
	err := rc.tls.WithHost(rc.cfg.TiDB.PdAddr).GetJSON(ctx, pdStores, result)
	if err != nil {
		return errors.Trace(err)
	}
	totalAvailable := typeutil.ByteSize(0)
	for _, store := range result.Stores {
		totalAvailable += store.Status.Available
	}
	var sourceSize int64
	err = rc.store.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
		sourceSize += size
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	replicaCount, err := rc.getReplicaCount(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// sourceSize is the total size of current csv/parquet/sql files.
	// it's not a simple multiple relationship with the final cluster occupancy, because
	//   1. sourceSize didn't compress with RocksDB.
	//   2. the index size was not included in sourceSize.
	// so we have to make estimateSize redundant with 1.5.
	estimateSize := uint64(sourceSize) * replicaCount * 3 / 2

	if typeutil.ByteSize(estimateSize) > totalAvailable {
		passed = false
		message = fmt.Sprintf("cluster doesn't have enough space, %s is avaiable, but we need %s",
			units.BytesSize(float64(totalAvailable)), units.BytesSize(float64(estimateSize)))
	}
	return nil
}

// ClusterIsAvailable check cluster is available to import data. this test can be skipped.
func (rc *Controller) ClusterIsAvailable(ctx context.Context) error {
	passed := true
	message := "cluster is available"
	defer func() {
		rc.checkTemplate.CriticalCollect(passed, message)
	}()
	// skip requirement check if explicitly turned off
	if !rc.cfg.App.CheckRequirements {
		message = "cluster available check is skipped by user requirement"
		return nil
	}
	// check backend is available(check version is match)
	// TODO change return value from err to (bool, err)
	checkCtx := &backend.CheckCtx{
		DBMetas: rc.dbMetas,
	}
	if err := rc.backend.CheckRequirements(ctx, checkCtx); err != nil {
		passed = false
		message = fmt.Sprintf("cluster available check failed: %s", err.Error())
	}
	return nil
}

// StoragePermission checks whether Lightning has enough permission to storage.
// this test cannot be skipped.
func (rc *Controller) StoragePermission(ctx context.Context) error {
	passed := true
	message := "Lightning has the correct storage permission"
	defer func() {
		rc.checkTemplate.CriticalCollect(passed, message)
	}()

	u, err := storage.ParseBackend(rc.cfg.Mydumper.SourceDir, nil)
	if err != nil {
		return errors.Annotate(err, "parse backend failed")
	}
	s3 := u.GetS3()
	if s3 != nil {
		// TODO finish s3 permission check
	}
	return nil
}

// TableHasDataInCluster checks whether table already has data in cluster. if table has data before import.
// in local or import backend, the checksum will failed.
func (rc *Controller) TableHasDataInCluster(ctx context.Context, tableName string) error {
	passed := true
	message := fmt.Sprintf("Table %s is empty, checksum won't passed", tableName)
	defer func() {
		rc.checkTemplate.CriticalCollect(passed, message)
	}()
	db, err := rc.tidbGlue.GetDB()
	if err != nil {
		return err
	}
	query := "select 1 from " + tableName + " limit 1"
	var dump int
	err = db.QueryRowContext(ctx, query).Scan(&dump)

	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		return errors.AddStack(err)
	default:
		message = fmt.Sprintf("table %s not empty, please clean up the table first", tableName)
		passed = false
	}
	return nil
}

// HasLargeCSV checks whether input csvs is fit for Lightning import.
// If strictFormat is false, and csv file is large. Lightning will have performance issue.
// this test cannot be skipped.
func (rc *Controller) HasLargeCSV(ctx context.Context) error {
	passed := true
	message := "source csv files size is proper"
	defer func() {
		rc.checkTemplate.PerformanceCollect(passed, message)
	}()
	if rc.cfg.Mydumper.StrictFormat == false {
		err := rc.store.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
			if !strings.HasSuffix(path, "csv") {
				message = "no csv files detected"
				return nil
			}
			if size > defaultCSVSize {
				message = fmt.Sprintf("large csv: %s file exists and it will slow down import performance", path)
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		message = "skip csv size check, because StrictFormat is true"
	}
	return nil
}

// LocalResource checks the local node has enough resources for this import when local backend enabled;
func (rc *Controller) LocalResource(ctx context.Context) error {
	if rc.isSourceInLocal() {
		same, err := disk.SameDisk(rc.cfg.Mydumper.SourceDir, rc.cfg.TikvImporter.SortedKVDir)
		if err != nil {
			return errors.Trace(err)
		}
		if same {
			rc.checkTemplate.PerformanceCollect(false,
				fmt.Sprintf("sorted-kv-dir and data-source-dir are in the same disk, may slow down performance"))
		}
	}
	var sourceSize int64
	err := rc.store.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
		sourceSize += size
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	var localSize int64
	err = filepath.Walk(rc.cfg.TikvImporter.SortedKVDir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			localSize += info.Size()
		}
		return err
	})

	message := "local disk resource is enough"
	if sourceSize*3/2 > localSize {
		message = fmt.Sprintf("local disk space may not enough to finish import, source dir has %d, but local has %d", sourceSize, localSize)
	} else if sourceSize > localSize {
		message = fmt.Sprintf("local disk space is not enough to finish import, source dir has %d, but local has %d,"+
			"so, enable disk-quota automatically", sourceSize, localSize)
	} else {
		// TODO enable disk-quota, so this checks is always passed.
	}
	rc.checkTemplate.CriticalCollect(true, message)
	return nil
}

// CheckpointIsValid checks whether we can start this import with this checkpoint.
func (rc *Controller) CheckpointIsValid(ctx context.Context, tableName string) error {
	passed := true
	message := fmt.Sprintf("Table:%s checkpoint checks passed", tableName)

	defer func() {
		rc.checkTemplate.CriticalCollect(passed, message)
	}()

	tableCheckPoint, err := rc.checkpointsDB.Get(ctx, tableName)
	if err != nil {
		return errors.Trace(err)
	}
	// if checkpoint enable and not missing, we skip the check table empty progress.
	if tableCheckPoint.Status <= checkpoints.CheckpointStatusMissing {
		message = fmt.Sprintf("Table:%s checkpoint not exists", tableName)
		return nil
	}

	for _, eng := range tableCheckPoint.Engines {
		if len(eng.Chunks) > 0 {
			chunk := eng.Chunks[0]
			if filepath.Dir(chunk.FileMeta.Path) != rc.cfg.Mydumper.SourceDir {
				message = fmt.Sprintf("chunk checkpoints path is not equal to config"+
					"checkpoint is %s, config source dir is %s", chunk.FileMeta.Path, rc.cfg.Mydumper.SourceDir)
				passed = false
			}
		} else {
			return errors.Errorf("meet invalid checkpoint in", tableName)
		}
	}
	// TODO check table checkpoint has same columns with DB schema
	// we can use checkpoints columnPer and rc.dbMetas meta file to spell column.
	return nil
}

// SchemaIsValid checks the import file and cluster schema is match.
func (rc *Controller) SchemaIsValid(ctx context.Context) error {
	// TODO check schema between rc.dbMetas and rc.dbInfos in TiDB.
	return nil
}
