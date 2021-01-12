// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

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
}

func newBackupSchemas() *Schemas {
	return &Schemas{
		schemas:        make(map[string]backup.Schema),
		backupSchemaCh: make(chan backup.Schema),
		errCh:          make(chan error),
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

// Start backups schemas.
func (pending *Schemas) Start(
	ctx context.Context,
	store kv.Storage,
	backupTS uint64,
	concurrency uint,
	copConcurrency uint,
	updateCh glue.Progress,
) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Schemas.Start", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	workerPool := utils.NewWorkerPool(concurrency, "Schemas")
	errg, ectx := errgroup.WithContext(ctx)
	go func() {
		startAll := time.Now()
		for n, s := range pending.schemas {
			log.Info("table checksum start", zap.String("table", n))
			name := n
			schema := s
			workerPool.ApplyOnErrorGroup(errg, func() error {
				start := time.Now()
				table := model.TableInfo{}
				err := json.Unmarshal(schema.Table, &table)
				if err != nil {
					return errors.Trace(err)
				}
				checksumResp, err := calculateChecksum(
					ectx, &table, store.GetClient(), backupTS, copConcurrency)
				if err != nil {
					return errors.Trace(err)
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
				return nil
			})
		}
		if err := errg.Wait(); err != nil {
			pending.errCh <- err
		}
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

// CopyMeta copies schema metadata directly from pending backupSchemas, without calculating checksum.
// use this when user skip the checksum generating.
func (pending *Schemas) CopyMeta() []*backup.Schema {
	schemas := make([]*backup.Schema, 0, len(pending.schemas))
	for _, v := range pending.schemas {
		schema := v
		schemas = append(schemas, &schema)
	}
	return schemas
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
	concurrency uint,
) (*tipb.ChecksumResponse, error) {
	exe, err := checksum.NewExecutorBuilder(table, backupTS).
		SetConcurrency(concurrency).
		Build()
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
