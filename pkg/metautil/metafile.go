// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package metautil

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/tablecodec"
	"go.uber.org/zap"

	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/logutil"
	"github.com/pingcap/br/pkg/storage"
)

const (
	// MetaFile represents file name
	MetaFile = "backupmeta"
)

func WalkLeafMetaFile(
	ctx context.Context, storage storage.ExternalStorage, file *backuppb.MetaFile, output func(*backuppb.MetaFile),
) error {
	if len(file.MetaFiles) == 0 {
		output(file)
		return nil
	}
	for _, node := range file.MetaFiles {
		content, err := storage.ReadFile(ctx, node.Name)
		if err != nil {
			return errors.Trace(err)
		}
		checksum := sha256.Sum256(content)
		if !bytes.Equal(node.Sha256, checksum[:]) {
			return errors.Annotatef(berrors.ErrInvalidMetaFile,
				"checksum mismatch expect %s, got %s", hex.EncodeToString(node.Sha256), hex.EncodeToString(checksum[:]))
		}
		child := &backuppb.MetaFile{}
		if err = proto.Unmarshal(content, child); err != nil {
			return errors.Trace(err)
		}
		if err = WalkLeafMetaFile(ctx, storage, child, output); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Table wraps the schema and files of a table.
type Table struct {
	DB              *model.DBInfo
	Info            *model.TableInfo
	Crc64Xor        uint64
	TotalKvs        uint64
	TotalBytes      uint64
	Files           []*backuppb.File
	TiFlashReplicas int
	Stats           *handle.JSONTable
}

type MetaReader struct {
	storage    storage.ExternalStorage
	backupMeta *backuppb.BackupMeta
}

func NewMetaReader(backpMeta *backuppb.BackupMeta, storage storage.ExternalStorage) *MetaReader {
	return &MetaReader{
		storage:    storage,
		backupMeta: backpMeta,
	}
}

func (reader *MetaReader) ReadSchemas(ctx context.Context, output chan<- *backuppb.Schema) error {
	// Read backupmeta v1 schemas.
	for _, s := range reader.backupMeta.Schemas {
		output <- s
	}
	// Read backupmeta v2 schemas.
	outputFn := func(m *backuppb.MetaFile) {
		for _, s := range m.Schemas {
			output <- s
		}
	}
	return WalkLeafMetaFile(ctx, reader.storage, reader.backupMeta.SchemaIndex, outputFn)
}

func (reader *MetaReader) ReadSchemasFiles(ctx context.Context, output chan<- *Table) error {
	const maxBatchSize = 1024
	ch := make(chan *backuppb.Schema, maxBatchSize)
	errCh := make(chan error)
	go func() {
		if err := reader.ReadSchemas(ctx, ch); err != nil {
			errCh <- errors.Trace(err)
		}
		close(errCh)
		close(ch)
	}()

	done := false
	for {
		// table ID -> *Table
		tableMap := make(map[int64]*Table, maxBatchSize)
		batchSize := 0
		for {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case err := <-errCh:
				return errors.Trace(err)
			case s, ok := <-ch:
				if !ok {
					done = true
					break
				}
				tableInfo := &model.TableInfo{}
				if err := json.Unmarshal(s.Table, tableInfo); err != nil {
					return errors.Trace(err)
				}
				dbInfo := &model.DBInfo{}
				if err := json.Unmarshal(s.Table, dbInfo); err != nil {
					return errors.Trace(err)
				}
				stats := &handle.JSONTable{}
				if s.Stats != nil {
					if err := json.Unmarshal(s.Stats, stats); err != nil {
						return errors.Trace(err)
					}
				}
				table := &Table{
					DB:              dbInfo,
					Info:            tableInfo,
					Crc64Xor:        s.Crc64Xor,
					TotalKvs:        s.TotalKvs,
					TotalBytes:      s.TotalBytes,
					TiFlashReplicas: int(s.TiflashReplicas),
					Stats:           stats,
				}
				tableMap[tableInfo.ID] = table
				if tableInfo.Partition != nil {
					// Partition table can have many table IDs (partition IDs).
					for _, p := range tableInfo.Partition.Definitions {
						tableMap[p.ID] = table
					}
				}

				// Break receive if batch is large enough.
				batchSize++
				if batchSize < maxBatchSize {
					break
				}
			}
		}

		outputFn := func(m *backuppb.MetaFile) {
			for _, file := range m.DataFiles {
				tableID := tablecodec.DecodeTableID(file.GetStartKey())
				if tableID == 0 {
					log.Panic("tableID must not equal to 0", logutil.File(file))
				}
				if table, ok := tableMap[tableID]; ok {
					table.Files = append(table.Files, file)
				}
			}
		}
		err := WalkLeafMetaFile(ctx, reader.storage, reader.backupMeta.SchemaIndex, outputFn)
		if err != nil {
			return errors.Trace(err)
		}

		for _, table := range tableMap {
			output <- table
		}

		if done {
			return nil
		}
	}
}

type sizedMetaFile struct {
	backuppb.MetaFile
	size int
}

type MetaWriter struct {
	storage           storage.ExternalStorage
	metafileSizeLimit int

	mu             sync.Mutex
	backupMeta     *backuppb.BackupMeta
	metafileSizes  map[string]int
	metafileSeqNum map[string]int
	schemas        *sizedMetaFile
}

func NewMetaWriter(storage storage.ExternalStorage, metafileSizeLimit int) *MetaWriter {
	return &MetaWriter{
		storage:           storage,
		metafileSizeLimit: metafileSizeLimit,
		mu:                sync.Mutex{},
		backupMeta:        &backuppb.BackupMeta{},
		metafileSizes:     make(map[string]int),
		schemas:           &sizedMetaFile{},
		metafileSeqNum:    make(map[string]int),
	}
}

func (builder *MetaWriter) Update(f func(m *backuppb.BackupMeta)) {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	f(builder.backupMeta)
}

func (builder *MetaWriter) WriteSchemas(ctx context.Context, schemas ...*backuppb.Schema) error {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	totalSize := 0
	for i := range schemas {
		size := schemas[i].Size()
		if size+builder.schemas.size > builder.metafileSizeLimit {
			// Incoming schema is too large, we need to flush buffered scheams.
			builder.flushSchemasLocked(ctx)
		}
		builder.schemas.Schemas = append(builder.schemas.Schemas, schemas[i])
		builder.schemas.size += size
		totalSize += size
	}
	builder.metafileSizes["schemas"] += totalSize

	return nil
}

func (builder *MetaWriter) flushSchemasLocked(ctx context.Context) error {
	if len(builder.schemas.Schemas) == 0 {
		return nil
	}
	content, err := builder.schemas.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	// Flush schemas to external storage.
	builder.metafileSeqNum["schemas"] += 1
	fname := fmt.Sprintf("backupmeta.schema.%09d", builder.metafileSeqNum["schemas"])
	if err = builder.storage.WriteFile(ctx, fname, content); err != nil {
		return errors.Trace(err)
	}
	checksum := sha256.Sum256(content)
	file := &backuppb.File{
		Name:   fname,
		Sha256: checksum[:],
		Size_:  uint64(len(content)),
	}
	// Add the metafile to backupmeta and reset schemas.
	builder.backupMeta.SchemaIndex.MetaFiles = append(builder.backupMeta.SchemaIndex.MetaFiles, file)
	builder.schemas = &sizedMetaFile{}
	return nil
}

func (builder *MetaWriter) FlushSchemas(ctx context.Context) error {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	return builder.flushSchemasLocked(ctx)
}

func (builder *MetaWriter) ArchiveSize() uint64 {
	builder.mu.Lock()
	defer builder.mu.Unlock()

	total := uint64(builder.backupMeta.Size())
	for _, file := range builder.backupMeta.Files {
		total += file.Size_
	}
	for _, size := range builder.metafileSizes {
		total += uint64(size)
	}
	return total
}

func (builder *MetaWriter) Flush(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("MetaWriter.Finish", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	builder.mu.Lock()
	defer builder.mu.Unlock()

	// Flush buffered schemas.
	if err := builder.flushSchemasLocked(ctx); err != nil {
		return nil
	}

	log.Debug("backup meta", zap.Reflect("meta", builder.backupMeta))
	content, err := builder.backupMeta.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("save backup meta", zap.Int("size", len(content)))
	err = builder.storage.WriteFile(ctx, MetaFile, content)
	return errors.Trace(err)
}

func (builder *MetaWriter) Backupmeta() *backuppb.BackupMeta {
	builder.mu.Lock()
	defer builder.mu.Unlock()
	clone := proto.Clone(builder.backupMeta)
	return clone.(*backuppb.BackupMeta)
}
