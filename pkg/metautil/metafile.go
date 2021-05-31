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

	"github.com/pingcap/br/pkg/utils"

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
	// Read backupmeta v1 metafiles.
	for _, s := range reader.backupMeta.Schemas {
		output(s)
	}
	// Read backupmeta v2 metafiles.
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

type AppendOp int

const (
	AppendMetaFile AppendOp = 0
	AppendDataFile AppendOp = 1
	AppendSchema   AppendOp = 2
	AppendDDL      AppendOp = 3
)

func (op AppendOp) name() string {
	switch op {
	case AppendMetaFile:
		return "metafile"
	case AppendDataFile:
		return "datafile"
	case AppendSchema:
		return "schema"
	case AppendDDL:
		return "ddl"
	default:
		log.Panic("unsupport op type", zap.Any("op", op))
	}
	return ""
}

// appends b to a
func (op AppendOp) appendFile(a *backuppb.MetaFile, b interface{}) int {
	size := 0
	switch op {
	case AppendMetaFile:
		a.MetaFiles = append(a.MetaFiles, b.(*backuppb.File))
		size += b.(*backuppb.File).Size()
	case AppendDataFile:
		// receive a batch of file because we need write and default sst are adjacent.
		files := b.([]*backuppb.File)
		a.DataFiles = append(a.DataFiles, files...)
		for _, f := range files {
			size += f.Size()
		}
	case AppendSchema:
		a.Schemas = append(a.Schemas, b.(*backuppb.Schema))
		size += b.(*backuppb.Schema).Size()
	case AppendDDL:
		a.Ddls = append(a.Ddls, b.([]byte))
		size += len(b.([]byte))
	}

	return size
}

type sizedMetaFile struct {
	// A stack like array, we always append to the last node.
	root *backuppb.MetaFile
	// nodes     []*sizedMetaFile
	size int

	filePrefix string
	fileSeqNum int
	sizeLimit  int

	storage storage.ExternalStorage
}

func NewSizedMetaFile(sizeLimit int) *sizedMetaFile {
	return &sizedMetaFile{
		root: &backuppb.MetaFile{
			Schemas:   make([]*backuppb.Schema, 0),
			DataFiles: make([]*backuppb.File, 0),
			RawRanges: make([]*backuppb.RawRange, 0),
		},
		sizeLimit: sizeLimit,
	}
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

func (f *sizedMetaFile) Append(ctx context.Context, file interface{}, op AppendOp) bool {
	// append to root
	// 	TODO maybe use multi level index
	size := op.appendFile(f.root, file)
	f.size += size
	if f.size > f.sizeLimit {
		// f.size would reset outside
		return true
	}
	return false
}

type MetaWriter struct {
	storage           storage.ExternalStorage
	metafileSizeLimit int
	useV2Meta         bool

	mu             sync.Mutex
	backupMeta     *backuppb.BackupMeta
	metafileSizes  map[string]int
	metafileSeqNum map[string]int
	metafiles      *sizedMetaFile

	wg sync.WaitGroup
	metasCh chan interface{}
	errCh   chan error
}

func NewMetaWriter(storage storage.ExternalStorage, metafileSizeLimit int, useV2Meta bool) *MetaWriter {
	return &MetaWriter{
		storage:           storage,
		metafileSizeLimit: metafileSizeLimit,
		useV2Meta:         useV2Meta,
		mu:                sync.Mutex{},
		backupMeta:        &backuppb.BackupMeta{},
		metafileSizes:     make(map[string]int),
		metafiles:         NewSizedMetaFile(metafileSizeLimit),
		metafileSeqNum:    make(map[string]int),

		// TODO use const chan buffer
		metasCh: make(chan interface{}, 1024),
	}
}

func (writer *MetaWriter) resetCh() {
	writer.metasCh = make(chan interface{}, 1024)
	writer.errCh = make(chan error)
}

func (writer *MetaWriter) Update(f func(m *backuppb.BackupMeta)) {
	writer.mu.Lock()
	defer writer.mu.Unlock()

	f(writer.backupMeta)
}

func (writer *MetaWriter) Send(m interface{}, op AppendOp) error {
	select {
	case writer.metasCh <- m:
	case err := <-writer.errCh:
		return errors.Trace(err)
	}
	return nil
}

func (writer *MetaWriter) Close() {
	close(writer.errCh)
	close(writer.metasCh)
}

// StartWriteMetasAsync writes four kind of meta into backupmeta.
// 1. file
// 2. schema
// 3. ddl
// 4. rawRange( raw kv )
// when useBackupMetaV2 enabled, it will generate multi-level index backupmetav2.
// else it will generate backupmeta as before for compatibility.
func (writer *MetaWriter) StartWriteMetasAsync(ctx context.Context, op AppendOp) {
	// always start one goroutine to write one kind of meta.
	writer.wg.Wait()
	writer.resetCh()
	go func() {
		writer.wg.Add(1)
		defer writer.wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Info("exit write metas by context done")
				return
			case meta, ok := <-writer.metasCh:
				if !ok {
					log.Info("write metas finished", zap.Any("op", op))
					return
				}
				needFlush := writer.metafiles.Append(ctx, meta, op)
				if writer.useV2Meta && needFlush {
					err := writer.FlushMetasV2(ctx, op)
					if err != nil {
						writer.errCh <- err
					}
				}
			}
		}
	}()
}

func (writer *MetaWriter) FlushAndClose(ctx context.Context, op AppendOp) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("MetaWriter.Finish", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	var err error
	// flush the buffered meta
	if !writer.useV2Meta {
		err = writer.FlushMetasV1(ctx, op)
	} else {
		err = writer.FlushMetasV2(ctx, op)
	}
	if err != nil {
		return errors.Trace(err)
	}
	writer.Close()
	return nil
}

// FlushMetasV1 keep the compatibility for old version.
func (writer *MetaWriter) FlushMetasV1(ctx context.Context, op AppendOp) error {
	switch op {
	case AppendDataFile:
		writer.backupMeta.Files = writer.metafiles.root.DataFiles
	case AppendSchema:
		writer.backupMeta.Schemas = writer.metafiles.root.Schemas
	case AppendDDL:
		b := bytes.Join(writer.metafiles.root.Ddls, []byte(`,`))
		copy(b[1:], b[0:])
		b[0] = byte('[')
		b = append(b, ']')
		writer.backupMeta.Ddls = b
	default:
		log.Panic("unsupport op type", zap.Any("op", op))
	}
	backupMetaData, err := proto.Marshal(writer.backupMeta)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("backup meta", zap.Reflect("meta", writer.backupMeta))
	log.Info("save backup meta", zap.Int("size", len(backupMetaData)))
	return writer.storage.WriteFile(ctx, utils.MetaFile, backupMetaData)
}

func (writer *MetaWriter) FlushMetasV2(ctx context.Context, op AppendOp) error {
	switch op {
	case AppendSchema:
		if len(writer.metafiles.root.Schemas) == 0 {
			return nil
		}
	case AppendDataFile:
		if len(writer.metafiles.root.DataFiles) == 0 {
			return nil
		}
	case AppendDDL:
		if len(writer.metafiles.root.Ddls) == 0 {
			return nil
		}
	}
	name := op.name()
	writer.metafileSizes[name] += writer.metafiles.size
	content, err := writer.metafiles.root.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	// Flush metafiles to external storage.
	writer.metafileSeqNum["metafiles"] += 1
	fname := fmt.Sprintf("backupmeta.%s.%09d", name, writer.metafileSeqNum["metafiles"])
	if err = writer.storage.WriteFile(ctx, fname, content); err != nil {
		return errors.Trace(err)
	}
	checksum := sha256.Sum256(content)
	file := &backuppb.File{
		Name:   fname,
		Sha256: checksum[:],
		Size_:  uint64(len(content)),
	}

	// Add the metafile to backupmeta and reset metafiles.
	if writer.backupMeta.FileIndex == nil {
		writer.backupMeta.FileIndex = &backuppb.MetaFile{}
	}
	writer.backupMeta.FileIndex.MetaFiles = append(writer.backupMeta.FileIndex.MetaFiles, file)
	writer.metafiles = NewSizedMetaFile(writer.metafiles.sizeLimit)
	return nil
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

func (writer *MetaWriter) Backupmeta() *backuppb.BackupMeta {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	clone := proto.Clone(writer.backupMeta)
	return clone.(*backuppb.BackupMeta)
}
