// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/checksum"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

const (
	// DefaultSchemaConcurrency is the default number of the concurrent
	// backup schema tasks.
	DefaultSchemaConcurrency = 64
)

// Schemas is task for backuping schemas.
type Schemas struct {
	// name -> schema
	schemas        map[string]backup.Schema
	backupSchemaCh chan backup.Schema
	errCh          chan error
	wg             *sync.WaitGroup
}

func newBackupSchemas() *Schemas {
	return &Schemas{
		schemas:        make(map[string]backup.Schema),
		backupSchemaCh: make(chan backup.Schema),
		errCh:          make(chan error),
		wg:             new(sync.WaitGroup),
	}
}

func (pending *Schemas) pushPending(
	schema backup.Schema,
	dbName, tableName string,
) {
	name := fmt.Sprintf("%s.%s",
		utils.EncloseName(dbName), utils.EncloseName(tableName))
	pending.schemas[name] = schema
}

// Start backups schemas
func (pending *Schemas) Start(
	ctx context.Context,
	store kv.Storage,
	backupTS uint64,
	concurrency uint,
	updateCh glue.Progress,
) {
	workerPool := utils.NewWorkerPool(concurrency, "Schemas")
	go func() {
		startAll := time.Now()
		for n, s := range pending.schemas {
			log.Info("table checksum start", zap.String("table", n))
			name := n
			schema := s
			pending.wg.Add(1)
			workerPool.Apply(func() {
				defer pending.wg.Done()

				start := time.Now()
				table := model.TableInfo{}
				err := json.Unmarshal(schema.Table, &table)
				if err != nil {
					pending.errCh <- err
					return
				}
				checksumResp, err := calculateChecksum(
					ctx, &table, store.GetClient(), backupTS)
				if err != nil {
					pending.errCh <- err
					return
				}
				schema.Crc64Xor = checksumResp.Checksum
				schema.TotalKvs = checksumResp.TotalKvs
				schema.TotalBytes = checksumResp.TotalBytes
				log.Info("table checksum finished",
					zap.String("table", name),
					zap.Uint64("Crc64Xor", checksumResp.Checksum),
					zap.Uint64("TotalKvs", checksumResp.TotalKvs),
					zap.Uint64("TotalBytes", checksumResp.TotalBytes),
					zap.Duration("take", time.Since(start)))
				pending.backupSchemaCh <- schema

				updateCh.Inc()
			})
		}
		pending.wg.Wait()
		close(pending.backupSchemaCh)
		log.Info("backup checksum",
			zap.Duration("take", time.Since(startAll)))
		summary.CollectDuration("backup checksum", time.Since(startAll))
	}()
}

// FinishTableChecksum waits until all schemas' checksums are verified.
func (pending *Schemas) FinishTableChecksum() ([]*backup.Schema, error) {
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
func (pending *Schemas) Len() int {
	return len(pending.schemas)
}

func calculateChecksum(
	ctx context.Context,
	table *model.TableInfo,
	client kv.Client,
	backupTS uint64,
) (*tipb.ChecksumResponse, error) {
	exe, err := checksum.NewExecutorBuilder(table, backupTS).Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	checksumResp, err := exe.Execute(ctx, client, func() {
		// TODO: update progress here.
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return checksumResp, nil
}
