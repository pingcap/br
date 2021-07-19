package restore

import (
	"context"
	"fmt"
	"github.com/docker/go-units"
	"github.com/pingcap/br/pkg/lightning/backend"
	"github.com/pingcap/br/pkg/lightning/backend/kv"
	"github.com/pingcap/br/pkg/lightning/checkpoints"
	"github.com/pingcap/br/pkg/lightning/common"
	"github.com/pingcap/br/pkg/lightning/config"
	"github.com/pingcap/br/pkg/lightning/log"
	"github.com/pingcap/br/pkg/lightning/metric"
	"github.com/pingcap/br/pkg/lightning/mydump"
	verify "github.com/pingcap/br/pkg/lightning/verification"
	"github.com/pingcap/br/pkg/lightning/worker"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/meta/autoid"
	"go.uber.org/zap"
	"io"
	"time"
)

var (
	maxKVQueueSize         = 32             // Cache at most this number of rows before blocking the encode loop
	minDeliverBytes uint64 = 96 * units.KiB // 96 KB (data + index). batch at least this amount of bytes to reduce number of messages
)

type deliveredKVs struct {
	kvs     kv.Row // if kvs is nil, this indicated we've got the last message.
	columns []string
	offset  int64
	rowID   int64
}

type deliverResult struct {
	totalDur time.Duration
	err      error
}



type chunkRestore struct {
	parser mydump.Parser
	index  int
	chunk  *checkpoints.ChunkCheckpoint
}

func newChunkRestore(
	ctx context.Context,
	index int,
	cfg *config.Config,
	chunk *checkpoints.ChunkCheckpoint,
	ioWorkers *worker.Pool,
	store storage.ExternalStorage,
	tableInfo *checkpoints.TidbTableInfo,
) (*chunkRestore, error) {
	blockBufSize := int64(cfg.Mydumper.ReadBlockSize)

	var reader storage.ReadSeekCloser
	var err error
	if chunk.FileMeta.Type == mydump.SourceTypeParquet {
		reader, err = mydump.OpenParquetReader(ctx, store, chunk.FileMeta.Path, chunk.FileMeta.FileSize)
	} else {
		reader, err = store.Open(ctx, chunk.FileMeta.Path)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	var parser mydump.Parser
	switch chunk.FileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := cfg.Mydumper.CSV.Header && chunk.Chunk.Offset == 0
		parser = mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, blockBufSize, ioWorkers, hasHeader)
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(cfg.TiDB.SQLMode, reader, blockBufSize, ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, store, reader, chunk.FileMeta.Path)
		if err != nil {
			return nil, errors.Trace(err)
		}
	default:
		panic(fmt.Sprintf("file '%s' with unknown source type '%s'", chunk.Key.Path, chunk.FileMeta.Type.String()))
	}

	if err = parser.SetPos(chunk.Chunk.Offset, chunk.Chunk.PrevRowIDMax); err != nil {
		return nil, errors.Trace(err)
	}
	if len(chunk.ColumnPermutation) > 0 {
		parser.SetColumns(getColumnNames(tableInfo.Core, chunk.ColumnPermutation))
	}

	return &chunkRestore{
		parser: parser,
		index:  index,
		chunk:  chunk,
	}, nil
}

func (cr *chunkRestore) close() {
	cr.parser.Close()
}

//nolint:nakedret // TODO: refactor
func (cr *chunkRestore) deliverLoop(
	ctx context.Context,
	kvsCh <-chan []deliveredKVs,
	t *TableRestore,
	engineID int32,
	dataEngine, indexEngine *backend.LocalEngineWriter,
	rc *Controller,
) (deliverTotalDur time.Duration, err error) {
	var channelClosed bool

	deliverLogger := t.logger.With(
		zap.Int32("engineNumber", engineID),
		zap.Int("fileIndex", cr.index),
		zap.Stringer("path", &cr.chunk.Key),
		zap.String("task", "deliver"),
	)
	// Fetch enough KV pairs from the source.
	dataKVs := rc.backend.MakeEmptyRows()
	indexKVs := rc.backend.MakeEmptyRows()

	dataSynced := true
	for !channelClosed {
		var dataChecksum, indexChecksum verify.KVChecksum
		var columns []string
		var kvPacket []deliveredKVs
		// init these two field as checkpoint current value, so even if there are no kv pairs delivered,
		// chunk checkpoint should stay the same
		offset := cr.chunk.Chunk.Offset
		rowID := cr.chunk.Chunk.PrevRowIDMax

	populate:
		for dataChecksum.SumSize()+indexChecksum.SumSize() < minDeliverBytes {
			select {
			case kvPacket = <-kvsCh:
				if len(kvPacket) == 0 {
					channelClosed = true
					break populate
				}
				for _, p := range kvPacket {
					p.kvs.ClassifyAndAppend(&dataKVs, &dataChecksum, &indexKVs, &indexChecksum)
					columns = p.columns
					offset = p.offset
					rowID = p.rowID
				}
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}

		err = func() error {
			// We use `TryRLock` with sleep here to avoid blocking current goroutine during importing when disk-quota is
			// triggered, so that we can save chunkCheckpoint as soon as possible after `FlushEngine` is called.
			// This implementation may not be very elegant or even completely correct, but it is currently a relatively
			// simple and effective solution.
			for !rc.diskQuotaLock.TryRLock() {
				// try to update chunk checkpoint, this can help save checkpoint after importing when disk-quota is triggered
				if !dataSynced {
					dataSynced = cr.maybeSaveCheckpoint(rc, t, engineID, cr.chunk, dataEngine, indexEngine)
				}
				time.Sleep(time.Millisecond)
			}
			defer rc.diskQuotaLock.RUnlock()

			// Write KVs into the engine
			start := time.Now()

			if err = dataEngine.WriteRows(ctx, columns, dataKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					deliverLogger.Error("write to data engine failed", log.ShortError(err))
				}

				return errors.Trace(err)
			}
			if err = indexEngine.WriteRows(ctx, columns, indexKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					deliverLogger.Error("write to index engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}

			deliverDur := time.Since(start)
			deliverTotalDur += deliverDur
			metric.BlockDeliverSecondsHistogram.Observe(deliverDur.Seconds())
			metric.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(dataChecksum.SumSize()))
			metric.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(indexChecksum.SumSize()))
			metric.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(dataChecksum.SumKVS()))
			metric.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(indexChecksum.SumKVS()))
			return nil
		}()
		if err != nil {
			return
		}
		dataSynced = false

		dataKVs = dataKVs.Clear()
		indexKVs = indexKVs.Clear()

		// Update the table, and save a checkpoint.
		// (the write to the importer is effective immediately, thus update these here)
		// No need to apply a lock since this is the only thread updating `cr.chunk.**`.
		// In local mode, we should write these checkpoint after engine flushed.
		cr.chunk.Checksum.Add(&dataChecksum)
		cr.chunk.Checksum.Add(&indexChecksum)
		cr.chunk.Chunk.Offset = offset
		cr.chunk.Chunk.PrevRowIDMax = rowID

		if dataChecksum.SumKVS() != 0 || indexChecksum.SumKVS() != 0 {
			// No need to save checkpoint if nothing was delivered.
			dataSynced = cr.maybeSaveCheckpoint(rc, t, engineID, cr.chunk, dataEngine, indexEngine)
		}
		failpoint.Inject("SlowDownWriteRows", func() {
			deliverLogger.Warn("Slowed down write rows")
		})
		failpoint.Inject("FailAfterWriteRows", nil)
		// TODO: for local backend, we may save checkpoint more frequently, e.g. after written
		// 10GB kv pairs to data engine, we can do a flush for both data & index engine, then we
		// can safely update current checkpoint.

		failpoint.Inject("LocalBackendSaveCheckpoint", func() {
			if !rc.isLocalBackend() && (dataChecksum.SumKVS() != 0 || indexChecksum.SumKVS() != 0) {
				// No need to save checkpoint if nothing was delivered.
				saveCheckpoint(rc, t, engineID, cr.chunk)
			}
		})
	}

	return
}

func (cr *chunkRestore) maybeSaveCheckpoint(
	rc *Controller,
	t *TableRestore,
	engineID int32,
	chunk *checkpoints.ChunkCheckpoint,
	data, index *backend.LocalEngineWriter,
) bool {
	if data.IsSynced() && index.IsSynced() {
		saveCheckpoint(rc, t, engineID, chunk)
		return true
	}
	return false
}

func saveCheckpoint(rc *Controller, t *TableRestore, engineID int32, chunk *checkpoints.ChunkCheckpoint) {
	// We need to update the AllocBase every time we've finished a file.
	// The AllocBase is determined by the maximum of the "handle" (_tidb_rowid
	// or integer primary key), which can only be obtained by reading all data.

	var base int64
	if t.tableInfo.Core.PKIsHandle && t.tableInfo.Core.ContainsAutoRandomBits() {
		base = t.alloc.Get(autoid.AutoRandomType).Base() + 1
	} else {
		base = t.alloc.Get(autoid.RowIDAllocType).Base() + 1
	}
	rc.saveCpCh <- saveCp{
		tableName: t.tableName,
		merger: &checkpoints.RebaseCheckpointMerger{
			AllocBase: base,
		},
	}
	rc.saveCpCh <- saveCp{
		tableName: t.tableName,
		merger: &checkpoints.ChunkCheckpointMerger{
			EngineID:          engineID,
			Key:               chunk.Key,
			Checksum:          chunk.Checksum,
			Pos:               chunk.Chunk.Offset,
			RowID:             chunk.Chunk.PrevRowIDMax,
			ColumnPermutation: chunk.ColumnPermutation,
		},
	}
}

//nolint:nakedret // TODO: refactor
func (cr *chunkRestore) encodeLoop(
	ctx context.Context,
	kvsCh chan<- []deliveredKVs,
	t *TableRestore,
	logger log.Logger,
	kvEncoder kv.Encoder,
	deliverCompleteCh <-chan deliverResult,
	rc *Controller,
) (readTotalDur time.Duration, encodeTotalDur time.Duration, err error) {
	send := func(kvs []deliveredKVs) error {
		select {
		case kvsCh <- kvs:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case deliverResult, ok := <-deliverCompleteCh:
			if deliverResult.err == nil && !ok {
				deliverResult.err = ctx.Err()
			}
			if deliverResult.err == nil {
				deliverResult.err = errors.New("unexpected premature fulfillment")
				logger.DPanic("unexpected: deliverCompleteCh prematurely fulfilled with no error", zap.Bool("chIsOpen", ok))
			}
			return errors.Trace(deliverResult.err)
		}
	}

	pauser, maxKvPairsCnt := rc.pauser, rc.cfg.TikvImporter.MaxKVPairs
	initializedColumns, reachEOF := false, false
	for !reachEOF {
		if err = pauser.Wait(ctx); err != nil {
			return
		}
		offset, _ := cr.parser.Pos()
		if offset >= cr.chunk.Chunk.EndOffset {
			break
		}

		var readDur, encodeDur time.Duration
		canDeliver := false
		kvPacket := make([]deliveredKVs, 0, maxKvPairsCnt)
		curOffset := offset
		var newOffset, rowID int64
		var kvSize uint64
	outLoop:
		for !canDeliver {
			readDurStart := time.Now()
			err = cr.parser.ReadRow()
			columnNames := cr.parser.Columns()
			newOffset, rowID = cr.parser.Pos()

			switch errors.Cause(err) {
			case nil:
				if !initializedColumns {
					if len(cr.chunk.ColumnPermutation) == 0 {
						if err = t.initializeColumns(columnNames, cr.chunk); err != nil {
							return
						}
					}
					initializedColumns = true
				}
			case io.EOF:
				reachEOF = true
				break outLoop
			default:
				err = errors.Annotatef(err, "in file %s at offset %d", &cr.chunk.Key, newOffset)
				return
			}
			readDur += time.Since(readDurStart)
			encodeDurStart := time.Now()
			lastRow := cr.parser.LastRow()
			// sql -> kv
			kvs, encodeErr := kvEncoder.Encode(logger, lastRow.Row, lastRow.RowID, cr.chunk.ColumnPermutation, curOffset)
			encodeDur += time.Since(encodeDurStart)
			cr.parser.RecycleRow(lastRow)
			if encodeErr != nil {
				err = errors.Annotatef(encodeErr, "in file %s at offset %d", &cr.chunk.Key, newOffset)
				return
			}
			kvPacket = append(kvPacket, deliveredKVs{kvs: kvs, columns: columnNames, offset: newOffset, rowID: rowID})
			kvSize += kvs.Size()
			failpoint.Inject("mock-kv-size", func(val failpoint.Value) {
				kvSize += uint64(val.(int))
			})
			// pebble cannot allow > 4.0G kv in one batch.
			// we will meet pebble panic when import sql file and each kv has the size larger than 4G / maxKvPairsCnt.
			// so add this check.
			if kvSize >= minDeliverBytes || len(kvPacket) >= maxKvPairsCnt || newOffset == cr.chunk.Chunk.EndOffset {
				canDeliver = true
				kvSize = 0
			}
			curOffset = newOffset
		}
		encodeTotalDur += encodeDur
		metric.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())
		readTotalDur += readDur
		metric.RowReadSecondsHistogram.Observe(readDur.Seconds())
		metric.RowReadBytesHistogram.Observe(float64(newOffset - offset))

		if len(kvPacket) != 0 {
			deliverKvStart := time.Now()
			if err = send(kvPacket); err != nil {
				return
			}
			metric.RowKVDeliverSecondsHistogram.Observe(time.Since(deliverKvStart).Seconds())
		}
	}

	err = send([]deliveredKVs{})
	return
}

func (cr *chunkRestore) restore(
	ctx context.Context,
	t *TableRestore,
	engineID int32,
	dataEngine, indexEngine *backend.LocalEngineWriter,
	rc *Controller,
) error {
	// Create the encoder.
	kvEncoder, err := rc.backend.NewEncoder(t.encTable, &kv.SessionOptions{
		SQLMode:   rc.cfg.TiDB.SQLMode,
		Timestamp: cr.chunk.Timestamp,
		SysVars:   rc.sysVars,
		// use chunk.PrevRowIDMax as the auto random seed, so it can stay the same value after recover from checkpoint.
		AutoRandomSeed: cr.chunk.Chunk.PrevRowIDMax,
	})
	if err != nil {
		return err
	}

	kvsCh := make(chan []deliveredKVs, maxKVQueueSize)
	deliverCompleteCh := make(chan deliverResult)

	defer func() {
		kvEncoder.Close()
		kvEncoder = nil
		close(kvsCh)
	}()

	go func() {
		defer close(deliverCompleteCh)
		dur, err := cr.deliverLoop(ctx, kvsCh, t, engineID, dataEngine, indexEngine, rc)
		select {
		case <-ctx.Done():
		case deliverCompleteCh <- deliverResult{dur, err}:
		}
	}()

	logTask := t.logger.With(
		zap.Int32("engineNumber", engineID),
		zap.Int("fileIndex", cr.index),
		zap.Stringer("path", &cr.chunk.Key),
	).Begin(zap.InfoLevel, "restore file")

	readTotalDur, encodeTotalDur, err := cr.encodeLoop(ctx, kvsCh, t, logTask.Logger, kvEncoder, deliverCompleteCh, rc)
	if err != nil {
		return err
	}

	select {
	case deliverResult, ok := <-deliverCompleteCh:
		if ok {
			logTask.End(zap.ErrorLevel, deliverResult.err,
				zap.Duration("readDur", readTotalDur),
				zap.Duration("encodeDur", encodeTotalDur),
				zap.Duration("deliverDur", deliverResult.totalDur),
				zap.Object("checksum", &cr.chunk.Checksum),
			)
			return errors.Trace(deliverResult.err)
		}
		// else, this must cause by ctx cancel
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}


