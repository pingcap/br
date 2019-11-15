package raw

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/utils"
)

// BackupSchemas is task for backuping schemas
type BackupSchemas struct {
	// name -> schema
	schemas        map[string]backup.Schema
	backupSchemaCh chan backup.Schema
	errCh          chan error
	wg             sync.WaitGroup
	skipChecksum   bool
}

func newBackupSchemas() *BackupSchemas {
	return &BackupSchemas{
		schemas:        make(map[string]backup.Schema),
		backupSchemaCh: make(chan backup.Schema),
		errCh:          make(chan error),
	}
}

func (pending *BackupSchemas) pushPending(
	schema backup.Schema,
	dbName, tableName string,
) {
	name := fmt.Sprintf("%s.%s",
		utils.EncloseName(dbName), utils.EncloseName(tableName))
	pending.schemas[name] = schema
}

// SetSkipChecksum sets whether it should skip checksum
func (pending *BackupSchemas) SetSkipChecksum(skip bool) {
	pending.skipChecksum = skip
}

// Start backups schemas
func (pending *BackupSchemas) Start(
	ctx context.Context,
	store kv.Storage,
	backupTS uint64,
	concurrency uint,
	updateCh chan<- struct{},
) {
	workerPool := utils.NewWorkerPool(concurrency, fmt.Sprintf("BackupSchemas"))
	go func() {
		for n, s := range pending.schemas {
			log.Info("admin checksum from TiDB start", zap.String("table", n))
			name := n
			schema := s
			pending.wg.Add(1)
			workerPool.Apply(func() {
				defer pending.wg.Done()

				if pending.skipChecksum {
					pending.backupSchemaCh <- schema
					updateCh <- struct{}{}
					return
				}

				dbSession, err := session.CreateSession(store)
				if err != nil {
					pending.errCh <- errors.Trace(err)
					return
				}
				defer dbSession.Close()
				start := time.Now()

				// TODO figure out why
				// must set to true to avoid load global vars, otherwise we got error
				dbSession.GetSessionVars().CommonGlobalLoaded = true
				// make FastChecksum snapshot is same as backup snapshot
				dbSession.GetSessionVars().SnapshotTS = backupTS
				checksum, err := getChecksumFromTiDB(ctx, dbSession, name)
				if err != nil {
					pending.errCh <- err
					return
				}
				schema.Crc64Xor = checksum.checksum
				schema.TotalKvs = checksum.totalKvs
				schema.TotalBytes = checksum.totalBytes
				log.Info("admin checksum from TiDB finished",
					zap.String("table", name),
					zap.Uint64("Crc64Xor", checksum.checksum),
					zap.Uint64("TotalKvs", checksum.totalKvs),
					zap.Uint64("TotalBytes", checksum.totalBytes),
					zap.Duration("take", time.Since(start)))
				pending.backupSchemaCh <- schema

				updateCh <- struct{}{}
			})
		}
		pending.wg.Wait()
		close(pending.backupSchemaCh)
	}()
}

func (pending *BackupSchemas) finishTableChecksum() ([]*backup.Schema, error) {
	schemas := make([]*backup.Schema, 0, len(pending.schemas))
	for {
		select {
		case s, ok := <-pending.backupSchemaCh:
			if !ok {
				return schemas, nil
			}
			schemas = append(schemas, &s)
		case err := <-pending.errCh:
			return nil, errors.Trace(err)
		}
	}
}

// Len returns the number of schemas.
func (pending *BackupSchemas) Len() int {
	return len(pending.schemas)
}

type tableChecksum struct {
	checksum   uint64
	totalKvs   uint64
	totalBytes uint64
}

func getChecksumFromTiDB(
	ctx context.Context,
	dbSession session.Session,
	name string,
) (*tableChecksum, error) {
	var recordSets []sqlexec.RecordSet
	recordSets, err := dbSession.Execute(ctx, fmt.Sprintf(
		"ADMIN CHECKSUM TABLE %s", name))
	if err != nil {
		return nil, errors.Trace(err)
	}
	records, err := utils.ResultSetToStringSlice(ctx, dbSession, recordSets[0])
	if err != nil {
		return nil, errors.Trace(err)
	}

	record := records[0]
	checksum, err := strconv.ParseUint(record[2], 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	totalKvs, err := strconv.ParseUint(record[3], 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	totalBytes, err := strconv.ParseUint(record[4], 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &tableChecksum{
		checksum:   checksum,
		totalKvs:   totalKvs,
		totalBytes: totalBytes,
	}, nil
}
