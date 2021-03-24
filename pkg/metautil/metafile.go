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
	if file == nil {
		return nil
	}
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

func (reader *MetaReader) readSchemas(ctx context.Context, output func(*backuppb.Schema)) error {
	// Read backupmeta v1 schemas.
	for _, s := range reader.backupMeta.Schemas {
		output(s)
	}
	// Read backupmeta v2 schemas.
	outputFn := func(m *backuppb.MetaFile) {
		for _, s := range m.Schemas {
			output(s)
		}
	}
	return WalkLeafMetaFile(ctx, reader.storage, reader.backupMeta.SchemaIndex, outputFn)
}

func (reader *MetaReader) readDataFiles(ctx context.Context, output func(*backuppb.File)) error {
	// Read backupmeta v1 data files.
	for _, f := range reader.backupMeta.Files {
		output(f)
	}
	// Read backupmeta v2 data files.
	outputFn := func(m *backuppb.MetaFile) {
		for _, f := range m.DataFiles {
			output(f)
		}
	}
	return WalkLeafMetaFile(ctx, reader.storage, reader.backupMeta.FileIndex, outputFn)
}

func (reader *MetaReader) ReadSchemasFiles(ctx context.Context, output chan<- *Table) error {
	const maxBatchSize = 1024
	ch := make(chan interface{}, maxBatchSize)
	errCh := make(chan error)
	go func() {
		if err := reader.readSchemas(ctx, func(s *backuppb.Schema) { ch <- s }); err != nil {
			errCh <- errors.Trace(err)
		}
		close(errCh)
		close(ch)
	}()

	for {
		// table ID -> *Table
		tableMap := make(map[int64]*Table, maxBatchSize)
		err := receiveBatch(ctx, errCh, ch, maxBatchSize, func(item interface{}) error {
			s := item.(*backuppb.Schema)
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
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		if len(tableMap) == 0 {
			// We have read all tables.
			return nil
		}

		outputFn := func(file *backuppb.File) {
			tableID := tablecodec.DecodeTableID(file.GetStartKey())
			if tableID == 0 {
				log.Panic("tableID must not equal to 0", logutil.File(file))
			}
			if table, ok := tableMap[tableID]; ok {
				table.Files = append(table.Files, file)
			}
		}
		err = reader.readDataFiles(ctx, outputFn)
		if err != nil {
			return errors.Trace(err)
		}

		for _, table := range tableMap {
			output <- table
		}
	}
}

func receiveBatch(
	ctx context.Context, errCh chan error, ch <-chan interface{}, maxBatchSize int,
	collectItem func(interface{}) error,
) error {
	batchSize := 0
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case err := <-errCh:
			return errors.Trace(err)
		case s, ok := <-ch:
			if !ok {
				return nil
			}
			if err := collectItem(s); err != nil {
				errors.Trace(err)
			}
		}
		// Return if the batch is large enough.
		batchSize++
		if batchSize >= maxBatchSize {
			return nil
		}
	}
}

type appendOp int

const (
	appendMetaFile appendOp = 0
	appendDataFile appendOp = 1
	appendSchema   appendOp = 2
)

// appends b to a
func (op appendOp) appendFile(a *backuppb.MetaFile, b interface{}) {
	switch op {
	case appendMetaFile:
		a.MetaFiles = append(a.MetaFiles, b.(*backuppb.File))
	case appendDataFile:
		a.DataFiles = append(a.DataFiles, b.(*backuppb.File))
	case appendSchema:
		a.Schemas = append(a.Schemas, b.(*backuppb.Schema))
	}
}

type sizedMetaFile struct {
	// A stack like array, we always append to the last node.
	nodes     []*backuppb.MetaFile
	size      int
	sizeLimit int

	filePrefix string
	fileSeqNum int

	storage storage.ExternalStorage
}

// flushFile flushes file to external storage and returns a file descriptor (*backuppb.File).
func (f *sizedMetaFile) flushFile(ctx context.Context, file *backuppb.MetaFile) (*backuppb.File, error) {
	if file == nil {
		return nil, errors.Annotate(berrors.ErrInvalidMetaFile, "nil metafile")
	}
	content, err := file.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	f.fileSeqNum++
	fname := fmt.Sprintf("%s.%09d", f.filePrefix, f.fileSeqNum)
	if err = f.storage.WriteFile(ctx, fname, content); err != nil {
		return nil, errors.Trace(err)
	}
	checksum := sha256.Sum256(content)
	return &backuppb.File{
		Name:   fname,
		Sha256: checksum[:],
		Size_:  uint64(len(content)),
	}, nil
}

func (f *sizedMetaFile) findWritableNode() *backuppb.MetaFile {
	if len(f.root.MetaFiles) != 0 {
		// Root node still have some space.
		return f.root
	}
	return f.leaf
}

func (f *sizedMetaFile) Append(ctx context.Context, file *backuppb.MetaFile, op appendOp) {
	fszie := file.Size()
	if f.size+fszie >= f.sizeLimit {
		// rotate root
		return
	}

	f.size += fszie
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

func (writer *MetaWriter) Update(f func(m *backuppb.BackupMeta)) {
	writer.mu.Lock()
	defer writer.mu.Unlock()

	f(writer.backupMeta)
}

func (writer *MetaWriter) WriteSchemas(ctx context.Context, schemas ...*backuppb.Schema) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()

	totalSize := 0
	for i := range schemas {
		size := schemas[i].Size()
		if size+writer.schemas.size > writer.metafileSizeLimit {
			// Incoming schema is too large, we need to flush buffered scheams.
			writer.flushSchemasLocked(ctx)
		}
		writer.schemas.root.Schemas = append(writer.schemas.root.Schemas, schemas[i])
		writer.schemas.size += size
		totalSize += size
	}
	writer.metafileSizes["schemas"] += totalSize

	return nil
}

func (writer *MetaWriter) flushSchemasLocked(ctx context.Context) error {
	if len(writer.schemas.root.Schemas) == 0 {
		return nil
	}
	content, err := writer.schemas.root.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	// Flush schemas to external storage.
	writer.metafileSeqNum["schemas"] += 1
	fname := fmt.Sprintf("backupmeta.schema.%09d", writer.metafileSeqNum["schemas"])
	if err = writer.storage.WriteFile(ctx, fname, content); err != nil {
		return errors.Trace(err)
	}
	checksum := sha256.Sum256(content)
	file := &backuppb.File{
		Name:   fname,
		Sha256: checksum[:],
		Size_:  uint64(len(content)),
	}
	// Add the metafile to backupmeta and reset schemas.
	if writer.backupMeta.SchemaIndex == nil {
		writer.backupMeta.SchemaIndex = &backuppb.MetaFile{}
	}
	writer.backupMeta.SchemaIndex.MetaFiles = append(writer.backupMeta.SchemaIndex.MetaFiles, file)
	writer.schemas = &sizedMetaFile{}
	return nil
}

func (writer *MetaWriter) FlushSchemas(ctx context.Context) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()

	return writer.flushSchemasLocked(ctx)
}

func (writer *MetaWriter) ArchiveSize() uint64 {
	writer.mu.Lock()
	defer writer.mu.Unlock()

	total := uint64(writer.backupMeta.Size())
	for _, file := range writer.backupMeta.Files {
		total += file.Size_
	}
	for _, size := range writer.metafileSizes {
		total += uint64(size)
	}
	return total
}

func (writer *MetaWriter) Flush(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("MetaWriter.Finish", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()

	// Flush buffered schemas.
	if err := writer.flushSchemasLocked(ctx); err != nil {
		return nil
	}

	log.Debug("backup meta", zap.Reflect("meta", writer.backupMeta))
	content, err := writer.backupMeta.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("save backup meta", zap.Int("size", len(content)))
	err = writer.storage.WriteFile(ctx, MetaFile, content)
	return errors.Trace(err)
}

func (writer *MetaWriter) Backupmeta() *backuppb.BackupMeta {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	clone := proto.Clone(writer.backupMeta)
	return clone.(*backuppb.BackupMeta)
}
