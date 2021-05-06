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

package local

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/google/btree"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/br/pkg/lightning/backend"
	"github.com/pingcap/br/pkg/lightning/backend/kv"
	"github.com/pingcap/br/pkg/lightning/common"
	"github.com/pingcap/br/pkg/lightning/config"
	"github.com/pingcap/br/pkg/lightning/glue"
	"github.com/pingcap/br/pkg/lightning/log"
	"github.com/pingcap/br/pkg/lightning/manual"
	"github.com/pingcap/br/pkg/lightning/metric"
	"github.com/pingcap/br/pkg/lightning/tikv"
	"github.com/pingcap/br/pkg/lightning/worker"
	"github.com/pingcap/br/pkg/logutil"
	split "github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/br/pkg/version"
)

const (
	dialTimeout             = 5 * time.Second
	bigValueSize            = 1 << 16 // 64K
	maxRetryTimes           = 5
	defaultRetryBackoffTime = 3 * time.Second

	gRPCKeepAliveTime    = 10 * time.Second
	gRPCKeepAliveTimeout = 3 * time.Second
	gRPCBackOffMaxDelay  = 3 * time.Second

	// See: https://github.com/tikv/tikv/blob/e030a0aae9622f3774df89c62f21b2171a72a69e/etc/config-template.toml#L360
	regionMaxKeyCount = 1_440_000

	propRangeIndex = "tikv.range_index"

	defaultPropSizeIndexDistance = 4 * units.MiB
	defaultPropKeysIndexDistance = 40 * 1024

	// the lower threshold of max open files for pebble db.
	openFilesLowerThreshold = 128
)

var (
	// Local backend is compatible with TiDB [4.0.0, NextMajorVersion).
	localMinTiDBVersion = *semver.New("4.0.0")
	localMinTiKVVersion = *semver.New("4.0.0")
	localMinPDVersion   = *semver.New("4.0.0")
	localMaxTiDBVersion = version.NextMajorVersion()
	localMaxTiKVVersion = version.NextMajorVersion()
	localMaxPDVersion   = version.NextMajorVersion()
	tiFlashMinVersion   = *semver.New("4.0.5")
)

var (
	engineMetaKey      = []byte{0, 'm', 'e', 't', 'a'}
	normalIterStartKey = []byte{1}
)

// Range record start and end key for localStoreDir.DB
// so we can write it to tikv in streaming
type Range struct {
	start []byte
	end   []byte
}

// localFileMeta contains some field that is necessary to continue the engine restore/import process.
// These field should be written to disk when we update chunk checkpoint
type localFileMeta struct {
	TS uint64 `json:"ts"`
	// Length is the number of KV pairs stored by the engine.
	Length atomic.Int64 `json:"length"`
	// TotalSize is the total pre-compressed KV byte size stored by engine.
	TotalSize atomic.Int64 `json:"total_size"`
}

type importMutexState uint32

const (
	importMutexStateImport importMutexState = 1 << iota
	importMutexStateClose
	// importMutexStateReadLock is a special state because in this state we lock engine with read lock
	// and add isImportingAtomic with this value. In other state, we directly store with the state value.
	// so this must always the last value of this enum.
	importMutexStateReadLock
)

// either a sstMeta or a flush message
type metaOrFlush struct {
	meta    *sstMeta
	flushCh chan struct{}
}

type File struct {
	localFileMeta
	db           *pebble.DB
	UUID         uuid.UUID
	localWriters sync.Map

	// isImportingAtomic is an atomic variable indicating whether this engine is importing.
	// This should not be used as a "spin lock" indicator.
	isImportingAtomic atomic.Uint32
	// flush and ingest sst hold the rlock, other operation hold the wlock.
	mutex sync.RWMutex

	ctx            context.Context
	cancel         context.CancelFunc
	sstDir         string
	sstMetasChan   chan metaOrFlush
	ingestErr      common.OnceError
	wg             sync.WaitGroup
	sstIngester    sstIngester
	finishedRanges syncedRanges

	config backend.LocalEngineConfig

	// total size of SST files waiting to be ingested
	pendingFileSize atomic.Int64

	// statistics for pebble kv iter.
	importedKVSize  atomic.Int64
	importedKVCount atomic.Int64
}

func (e *File) setError(err error) {
	if err != nil {
		e.ingestErr.Set(err)
		e.cancel()
	}
}

func (e *File) Close() error {
	log.L().Debug("closing local engine", zap.Stringer("engine", e.UUID), zap.Stack("stack"))
	if e.db == nil {
		return nil
	}
	err := errors.Trace(e.db.Close())
	e.db = nil
	return err
}

// Cleanup remove meta and db files
func (e *File) Cleanup(dataDir string) error {
	if err := os.RemoveAll(e.sstDir); err != nil {
		return errors.Trace(err)
	}

	dbPath := filepath.Join(dataDir, e.UUID.String())
	return os.RemoveAll(dbPath)
}

// Exist checks if db folder existing (meta sometimes won't flush before lightning exit)
func (e *File) Exist(dataDir string) error {
	dbPath := filepath.Join(dataDir, e.UUID.String())
	if _, err := os.Stat(dbPath); err != nil {
		return err
	}
	return nil
}

func (e *File) getSizeProperties() (*sizeProperties, error) {
	sstables, err := e.db.SSTables(pebble.WithProperties())
	if err != nil {
		log.L().Warn("get table properties failed", zap.Stringer("engine", e.UUID), log.ShortError(err))
		return nil, errors.Trace(err)
	}

	sizeProps := newSizeProperties()
	for _, level := range sstables {
		for _, info := range level {
			if prop, ok := info.Properties.UserProperties[propRangeIndex]; ok {
				data := hack.Slice(prop)
				rangeProps, err := decodeRangeProperties(data)
				if err != nil {
					log.L().Warn("decodeRangeProperties failed", zap.Stringer("engine", e.UUID),
						zap.Stringer("fileNum", info.FileNum), log.ShortError(err))
					return nil, errors.Trace(err)
				}

				sizeProps.addAll(rangeProps)
			}
		}
	}

	return sizeProps, nil
}

func isStateLocked(state importMutexState) bool {
	return state&(importMutexStateClose|importMutexStateImport) != 0
}

func (e *File) isLocked() bool {
	// the engine is locked only in import or close state.
	return isStateLocked(importMutexState(e.isImportingAtomic.Load()))
}

func (e *File) getEngineFileSize() backend.EngineFileSize {
	metrics := e.db.Metrics()
	total := metrics.Total()
	var memSize int64
	e.localWriters.Range(func(k, v interface{}) bool {
		w := k.(*Writer)
		if w.writer != nil {
			memSize += int64(w.writer.writer.EstimatedSize())
		} else {
			// if kvs are still in memory, only calculate half of the total size
			// in our tests, SST file size is about 50% of the raw kv size
			memSize += w.batchSize / 2
		}

		return true
	})

	pendingSize := e.pendingFileSize.Load()
	// TODO: should also add the in-processing compaction sst writer size into MemSize
	return backend.EngineFileSize{
		UUID:        e.UUID,
		DiskSize:    total.Size + pendingSize,
		MemSize:     memSize,
		IsImporting: e.isLocked(),
	}
}

// rLock locks the local file with shard read state. Only used for flush and ingest SST files.
func (e *File) rLock() {
	e.mutex.RLock()
	e.isImportingAtomic.Add(uint32(importMutexStateReadLock))
}

func (e *File) rUnlock() {
	if e == nil {
		return
	}

	e.isImportingAtomic.Sub(uint32(importMutexStateReadLock))
	e.mutex.RUnlock()
}

// lock locks the local file for importing.
func (e *File) lock(state importMutexState) {
	e.mutex.Lock()
	e.isImportingAtomic.Store(uint32(state))
}

// lockUnless tries to lock the local file unless it is already locked into the state given by
// ignoreStateMask. Returns whether the lock is successful.
func (e *File) lockUnless(newState, ignoreStateMask importMutexState) bool {
	curState := e.isImportingAtomic.Load()
	if curState&uint32(ignoreStateMask) != 0 {
		return false
	}
	e.lock(newState)
	return true
}

// tryRLock tries to read-lock the local file unless it is already write locked.
// Returns whether the lock is successful.
func (e *File) tryRLock() bool {
	curState := e.isImportingAtomic.Load()
	// engine is in import/close state.
	if isStateLocked(importMutexState(curState)) {
		return false
	}
	e.rLock()
	return true
}

func (e *File) unlock() {
	if e == nil {
		return
	}
	e.isImportingAtomic.Store(0)
	e.mutex.Unlock()
}

type intHeap struct {
	arr []int32
}

func (h *intHeap) Len() int {
	return len(h.arr)
}

func (h *intHeap) Less(i, j int) bool {
	return h.arr[i] < h.arr[j]
}

func (h *intHeap) Swap(i, j int) {
	h.arr[i], h.arr[j] = h.arr[j], h.arr[i]
}

func (h *intHeap) Push(x interface{}) {
	h.arr = append(h.arr, x.(int32))
}

func (h *intHeap) Pop() interface{} {
	item := h.arr[len(h.arr)-1]
	h.arr = h.arr[:len(h.arr)-1]
	return item
}

func (e *File) ingestSSTLoop() {
	defer e.wg.Done()

	type flushSeq struct {
		seq int32
		ch  chan struct{}
	}

	seq := atomic.NewInt32(0)
	finishedSeq := atomic.NewInt32(0)
	var seqLock sync.Mutex
	// a flush is finished iff all the compaction&ingest tasks with a lower seq number are finished.
	flushQueue := make([]flushSeq, 0)
	// inSyncSeqs is a heap that stores all the finished compaction tasks whose seq is bigger than `finishedSeq + 1`
	// this mean there are still at lease one compaction task with a lower seq unfinished.
	inSyncSeqs := &intHeap{arr: make([]int32, 0)}

	type metaAndSeq struct {
		metas []*sstMeta
		seq   int32
	}

	concurrency := e.config.CompactConcurrency
	// when compaction is disabled, ingest is an serial action, so 1 routine is enough
	if !e.config.Compact {
		concurrency = 1
	}
	metaChan := make(chan metaAndSeq, concurrency)
	for i := 0; i < concurrency; i++ {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			defer func() {
				if e.ingestErr.Get() != nil {
					seqLock.Lock()
					for _, f := range flushQueue {
						f.ch <- struct{}{}
					}
					flushQueue = flushQueue[:0]
					seqLock.Unlock()
				}
			}()
			for {
				select {
				case <-e.ctx.Done():
					return
				case metas, ok := <-metaChan:
					if !ok {
						return
					}
					ingestMetas := metas.metas
					if e.config.Compact {
						newMeta, err := e.sstIngester.mergeSSTs(metas.metas, e.sstDir)
						if err != nil {
							e.setError(err)
							return
						}
						ingestMetas = []*sstMeta{newMeta}
					}

					if err := e.batchIngestSSTs(ingestMetas); err != nil {
						e.setError(err)
						return
					}
					seqLock.Lock()
					finSeq := finishedSeq.Load()
					if metas.seq == finSeq+1 {
						finSeq = metas.seq
						for len(inSyncSeqs.arr) > 0 {
							if inSyncSeqs.arr[0] == finSeq+1 {
								finSeq++
								heap.Remove(inSyncSeqs, 0)
							} else {
								break
							}
						}

						var flushChans []chan struct{}
						for _, seq := range flushQueue {
							if seq.seq <= finSeq {
								flushChans = append(flushChans, seq.ch)
							} else {
								break
							}
						}
						flushQueue = flushQueue[len(flushChans):]
						finishedSeq.Store(finSeq)
						seqLock.Unlock()
						for _, c := range flushChans {
							c <- struct{}{}
						}
					} else {
						heap.Push(inSyncSeqs, metas.seq)
						seqLock.Unlock()
					}
				}
			}
		}()
	}

	compactAndIngestSSTs := func(metas []*sstMeta) {
		if len(metas) > 0 {
			seqLock.Lock()
			metaSeq := seq.Add(1)
			seqLock.Unlock()
			select {
			case <-e.ctx.Done():
			case metaChan <- metaAndSeq{metas: metas, seq: metaSeq}:
			}
		}
	}

	pendingMetas := make([]*sstMeta, 0, 16)
	totalSize := int64(0)
	metasTmp := make([]*sstMeta, 0)
	addMetas := func() {
		if len(metasTmp) == 0 {
			return
		}
		metas := metasTmp
		metasTmp = make([]*sstMeta, 0, len(metas))
		if !e.config.Compact {
			compactAndIngestSSTs(metas)
			return
		}
		for _, m := range metas {
			if m.totalCount > 0 {
				pendingMetas = append(pendingMetas, m)
				totalSize += m.totalSize
				if totalSize >= e.config.CompactThreshold {
					compactMetas := pendingMetas
					pendingMetas = make([]*sstMeta, 0, len(pendingMetas))
					totalSize = 0
					compactAndIngestSSTs(compactMetas)
				}
			}
		}
	}
readMetaLoop:
	for {
		closed := false
		select {
		case <-e.ctx.Done():
			close(metaChan)
			return
		case m, ok := <-e.sstMetasChan:
			if !ok {
				closed = true
				break
			}
			if m.flushCh != nil {
				// meet a flush event, we should trigger a ingest task if there are pending metas,
				// and then waiting for all the running flush tasks to be done.
				if len(metasTmp) > 0 {
					addMetas()
				}
				if len(pendingMetas) > 0 {
					seqLock.Lock()
					metaSeq := seq.Add(1)
					flushQueue = append(flushQueue, flushSeq{ch: m.flushCh, seq: metaSeq})
					seqLock.Unlock()
					select {
					case metaChan <- metaAndSeq{metas: pendingMetas, seq: metaSeq}:
					case <-e.ctx.Done():
						close(metaChan)
						return
					}

					pendingMetas = make([]*sstMeta, 0, len(pendingMetas))
					totalSize = 0
				} else {
					// none remaining metas needed to be ingested
					seqLock.Lock()
					curSeq := seq.Load()
					finSeq := finishedSeq.Load()
					// if all pending SST files are written, directly do a db.Flush
					if curSeq == finSeq {
						seqLock.Unlock()
						m.flushCh <- struct{}{}
					} else {
						// waiting for pending compaction tasks
						flushQueue = append(flushQueue, flushSeq{ch: m.flushCh, seq: curSeq})
						seqLock.Unlock()
					}
				}
				continue readMetaLoop
			}
			metasTmp = append(metasTmp, m.meta)
			// try to drain all the sst meta from the chan to make sure all the SSTs are processed before handle a flush msg.
			if len(e.sstMetasChan) > 0 {
				continue readMetaLoop
			}

			addMetas()
		}
		if closed {
			compactAndIngestSSTs(pendingMetas)
			close(metaChan)
			return
		}
	}
}

func (e *File) addSST(ctx context.Context, m *sstMeta) error {
	// set pending size after SST file is generated
	e.pendingFileSize.Add(m.fileSize)
	select {
	case e.sstMetasChan <- metaOrFlush{meta: m}:
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ctx.Done():
	}
	return e.ingestErr.Get()
}

func (e *File) batchIngestSSTs(metas []*sstMeta) error {
	if len(metas) == 0 {
		return nil
	}
	sort.Slice(metas, func(i, j int) bool {
		return bytes.Compare(metas[i].minKey, metas[j].minKey) < 0
	})

	metaLevels := make([][]*sstMeta, 0)
	for _, meta := range metas {
		inserted := false
		for i, l := range metaLevels {
			if bytes.Compare(l[len(l)-1].maxKey, meta.minKey) >= 0 {
				continue
			}
			metaLevels[i] = append(l, meta)
			inserted = true
			break
		}
		if !inserted {
			metaLevels = append(metaLevels, []*sstMeta{meta})
		}
	}

	for _, l := range metaLevels {
		if err := e.ingestSSTs(l); err != nil {
			return err
		}
	}
	return nil
}

func (e *File) ingestSSTs(metas []*sstMeta) error {
	// use raw RLock to avoid change the lock state during flushing.
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	totalSize := int64(0)
	totalCount := int64(0)
	fileSize := int64(0)
	for _, m := range metas {
		totalSize += m.totalSize
		totalCount += m.totalCount
		fileSize += m.fileSize
	}
	log.L().Info("write data to local DB",
		zap.Int64("size", totalSize),
		zap.Int64("kvs", totalCount),
		zap.Int64("sstFileSize", fileSize),
		zap.String("file", metas[0].path),
		logutil.Key("firstKey", metas[0].minKey),
		logutil.Key("lastKey", metas[len(metas)-1].maxKey))
	if err := e.sstIngester.ingest(metas); err != nil {
		return errors.Trace(err)
	}
	count := int64(0)
	size := int64(0)
	for _, m := range metas {
		count += m.totalCount
		size += m.totalSize
	}
	e.Length.Add(count)
	e.TotalSize.Add(size)
	return nil
}

func (e *File) flushLocalWriters(parentCtx context.Context) error {
	eg, ctx := errgroup.WithContext(parentCtx)
	e.localWriters.Range(func(k, v interface{}) bool {
		eg.Go(func() error {
			w := k.(*Writer)
			return w.flush(ctx)
		})
		return true
	})
	return eg.Wait()
}

func (e *File) flushEngineWithoutLock(ctx context.Context) error {
	if err := e.flushLocalWriters(ctx); err != nil {
		return err
	}
	flushChan := make(chan struct{}, 1)
	select {
	case e.sstMetasChan <- metaOrFlush{flushCh: flushChan}:
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ctx.Done():
		return e.ctx.Err()
	}

	select {
	case <-flushChan:
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
	if err := e.ingestErr.Get(); err != nil {
		return errors.Trace(err)
	}
	if err := e.saveEngineMeta(); err != nil {
		return err
	}

	flushFinishedCh, err := e.db.AsyncFlush()
	if err != nil {
		return errors.Trace(err)
	}
	select {
	case <-flushFinishedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
}

// saveEngineMeta saves the metadata about the DB into the DB itself.
// This method should be followed by a Flush to ensure the data is actually synchronized
func (e *File) saveEngineMeta() error {
	jsonBytes, err := json.Marshal(&e.localFileMeta)
	if err != nil {
		return errors.Trace(err)
	}
	log.L().Debug("save engine meta", zap.Stringer("uuid", e.UUID), zap.Int64("count", e.Length.Load()),
		zap.Int64("size", e.TotalSize.Load()))
	// note: we can't set Sync to true since we disabled WAL.
	return errors.Trace(e.db.Set(engineMetaKey, jsonBytes, &pebble.WriteOptions{Sync: false}))
}

func (e *File) loadEngineMeta() {
	jsonBytes, closer, err := e.db.Get(engineMetaKey)
	if err != nil {
		log.L().Debug("local db missing engine meta", zap.Stringer("uuid", e.UUID), zap.Error(err))
		return
	}
	defer closer.Close()

	err = json.Unmarshal(jsonBytes, &e.localFileMeta)
	if err != nil {
		log.L().Warn("local db failed to deserialize meta", zap.Stringer("uuid", e.UUID), zap.ByteString("content", jsonBytes), zap.Error(err))
	}
	log.L().Debug("load engine meta", zap.Stringer("uuid", e.UUID), zap.Int64("count", e.Length.Load()),
		zap.Int64("size", e.TotalSize.Load()))
}

type gRPCConns struct {
	mu    sync.Mutex
	conns map[uint64]*connPool
}

func (conns *gRPCConns) Close() {
	conns.mu.Lock()
	defer conns.mu.Unlock()

	for _, cp := range conns.conns {
		cp.Close()
	}
}

type local struct {
	engines sync.Map // sync version of map[uuid.UUID]*File

	conns    gRPCConns
	splitCli split.SplitClient
	tls      *common.TLS
	pdAddr   string
	g        glue.Glue

	localStoreDir   string
	regionSplitSize int64

	rangeConcurrency  *worker.Pool
	ingestConcurrency *worker.Pool
	batchWriteKVPairs int
	checkpointEnabled bool

	tcpConcurrency int
	maxOpenFiles   int

	engineMemCacheSize      int
	localWriterMemCacheSize int64
}

// connPool is a lazy pool of gRPC channels.
// When `Get` called, it lazily allocates new connection if connection not full.
// If it's full, then it will return allocated channels round-robin.
type connPool struct {
	mu sync.Mutex

	conns   []*grpc.ClientConn
	next    int
	cap     int
	newConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (p *connPool) takeConns() (conns []*grpc.ClientConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns, conns = nil, p.conns
	p.next = 0
	return conns
}

// Close closes the conn pool.
func (p *connPool) Close() {
	for _, c := range p.takeConns() {
		if err := c.Close(); err != nil {
			log.L().Warn("failed to close clientConn", zap.String("target", c.Target()), log.ShortError(err))
		}
	}
}

// get tries to get an existing connection from the pool, or make a new one if the pool not full.
func (p *connPool) get(ctx context.Context) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.conns) < p.cap {
		c, err := p.newConn(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p.conns = append(p.conns, c)
		return c, nil
	}

	conn := p.conns[p.next]
	p.next = (p.next + 1) % p.cap
	return conn, nil
}

// newConnPool creates a new connPool by the specified conn factory function and capacity.
func newConnPool(cap int, newConn func(ctx context.Context) (*grpc.ClientConn, error)) *connPool {
	return &connPool{
		cap:     cap,
		conns:   make([]*grpc.ClientConn, 0, cap),
		newConn: newConn,

		mu: sync.Mutex{},
	}
}

// NewLocalBackend creates new connections to tikv.
func NewLocalBackend(
	ctx context.Context,
	tls *common.TLS,
	pdAddr string,
	cfg *config.TikvImporter,
	enableCheckpoint bool,
	g glue.Glue,
	maxOpenFiles int,
) (backend.Backend, error) {
	localFile := cfg.SortedKVDir
	rangeConcurrency := cfg.RangeConcurrency

	pdCli, err := pd.NewClientWithContext(ctx, []string{pdAddr}, tls.ToPDSecurityOption())
	if err != nil {
		return backend.MakeBackend(nil), errors.Annotate(err, "construct pd client failed")
	}
	splitCli := split.NewSplitClient(pdCli, tls.TLSConfig())

	shouldCreate := true
	if enableCheckpoint {
		if info, err := os.Stat(localFile); err != nil {
			if !os.IsNotExist(err) {
				return backend.MakeBackend(nil), err
			}
		} else if info.IsDir() {
			shouldCreate = false
		}
	}

	if shouldCreate {
		err = os.Mkdir(localFile, 0o700)
		if err != nil {
			return backend.MakeBackend(nil), errors.Annotate(err, "invalid sorted-kv-dir for local backend, please change the config or delete the path")
		}
	}

	local := &local{
		engines:  sync.Map{},
		splitCli: splitCli,
		tls:      tls,
		pdAddr:   pdAddr,
		g:        g,

		localStoreDir:   localFile,
		regionSplitSize: int64(cfg.RegionSplitSize),

		rangeConcurrency:  worker.NewPool(ctx, rangeConcurrency, "range"),
		ingestConcurrency: worker.NewPool(ctx, rangeConcurrency*2, "ingest"),
		tcpConcurrency:    rangeConcurrency,
		batchWriteKVPairs: cfg.SendKVPairs,
		checkpointEnabled: enableCheckpoint,
		maxOpenFiles:      utils.MaxInt(maxOpenFiles, openFilesLowerThreshold),

		engineMemCacheSize:      int(cfg.EngineMemCacheSize),
		localWriterMemCacheSize: int64(cfg.LocalWriterMemCacheSize),
	}
	local.conns.conns = make(map[uint64]*connPool)
	return backend.MakeBackend(local), nil
}

// rlock read locks a local file and returns the File instance if it exists.
func (local *local) rLockEngine(engineId uuid.UUID) *File {
	if e, ok := local.engines.Load(engineId); ok {
		engine := e.(*File)
		engine.rLock()
		return engine
	}
	return nil
}

// lock locks a local file and returns the File instance if it exists.
func (local *local) lockEngine(engineID uuid.UUID, state importMutexState) *File {
	if e, ok := local.engines.Load(engineID); ok {
		engine := e.(*File)
		engine.lock(state)
		return engine
	}
	return nil
}

// tryRLockAllEngines tries to read lock all engines, return all `File`s that are successfully locked.
func (local *local) tryRLockAllEngines() []*File {
	var allEngines []*File
	local.engines.Range(func(k, v interface{}) bool {
		engine := v.(*File)
		if engine.tryRLock() {
			allEngines = append(allEngines, engine)
		}
		return true
	})
	return allEngines
}

// lockAllEnginesUnless tries to lock all engines, unless those which are already locked in the
// state given by ignoreStateMask. Returns the list of locked engines.
func (local *local) lockAllEnginesUnless(newState, ignoreStateMask importMutexState) []*File {
	var allEngines []*File
	local.engines.Range(func(k, v interface{}) bool {
		engine := v.(*File)
		if engine.lockUnless(newState, ignoreStateMask) {
			allEngines = append(allEngines, engine)
		}
		return true
	})
	return allEngines
}

func (local *local) makeConn(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	store, err := local.splitCli.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithInsecure()
	if local.tls.TLSConfig() != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(local.tls.TLSConfig()))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)

	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	// we should use peer address for tiflash. for tikv, peer address is empty
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	conn, err := grpc.DialContext(
		ctx,
		addr,
		opt,
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                gRPCKeepAliveTime,
			Timeout:             gRPCKeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	cancel()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return conn, nil
}

func (local *local) getGrpcConnLocked(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	if _, ok := local.conns.conns[storeID]; !ok {
		local.conns.conns[storeID] = newConnPool(local.tcpConcurrency, func(ctx context.Context) (*grpc.ClientConn, error) {
			return local.makeConn(ctx, storeID)
		})
	}
	return local.conns.conns[storeID].get(ctx)
}

// Close the local backend.
func (local *local) Close() {
	allEngines := local.lockAllEnginesUnless(importMutexStateClose, 0)
	local.engines = sync.Map{}

	for _, engine := range allEngines {
		engine.Close()
		engine.unlock()
	}
	local.conns.Close()

	// if checkpoint is disable or we finish load all data successfully, then files in this
	// dir will be useless, so we clean up this dir and all files in it.
	if !local.checkpointEnabled || common.IsEmptyDir(local.localStoreDir) {
		err := os.RemoveAll(local.localStoreDir)
		if err != nil {
			log.L().Warn("remove local db file failed", zap.Error(err))
		}
	}
}

// FlushEngine ensure the written data is saved successfully, to make sure no data lose after restart
func (local *local) FlushEngine(ctx context.Context, engineID uuid.UUID) error {
	engineFile := local.rLockEngine(engineID)

	// the engine cannot be deleted after while we've acquired the lock identified by UUID.
	if engineFile == nil {
		return errors.Errorf("engine '%s' not found", engineID)
	}
	defer engineFile.rUnlock()
	return engineFile.flushEngineWithoutLock(ctx)
}

func (local *local) FlushAllEngines(parentCtx context.Context) (err error) {
	allEngines := local.tryRLockAllEngines()
	defer func() {
		for _, engine := range allEngines {
			engine.rUnlock()
		}
	}()

	eg, ctx := errgroup.WithContext(parentCtx)
	for _, engineFile := range allEngines {
		ef := engineFile
		eg.Go(func() error {
			return ef.flushEngineWithoutLock(ctx)
		})
	}
	return eg.Wait()
}

func (local *local) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

func (local *local) MaxChunkSize() int {
	// a batch size write to leveldb
	return int(local.regionSplitSize)
}

func (local *local) ShouldPostProcess() bool {
	return true
}

func (local *local) openEngineDB(engineUUID uuid.UUID, readOnly bool) (*pebble.DB, error) {
	opt := &pebble.Options{
		MemTableSize: local.engineMemCacheSize,
		// the default threshold value may cause write stall.
		MemTableStopWritesThreshold: 8,
		MaxConcurrentCompactions:    16,
		// set threshold to half of the max open files to avoid trigger compaction
		L0CompactionThreshold: math.MaxInt32,
		L0StopWritesThreshold: math.MaxInt32,
		LBaseMaxBytes:         16 * units.TiB,
		MaxOpenFiles:          local.maxOpenFiles,
		DisableWAL:            true,
		ReadOnly:              readOnly,
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			newRangePropertiesCollector,
		},
	}
	// set level target file size to avoid pebble auto triggering compaction that split ingest SST files into small SST.
	opt.Levels = []pebble.LevelOptions{
		{
			TargetFileSize: 16 * units.GiB,
		},
	}

	dbPath := filepath.Join(local.localStoreDir, engineUUID.String())
	db, err := pebble.Open(dbPath, opt)
	return db, errors.Trace(err)
}

// This method must be called with holding mutex of File
func (local *local) OpenEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	engineCfg := backend.LocalEngineConfig{}
	if cfg.Local != nil {
		engineCfg = *cfg.Local
	}
	db, err := local.openEngineDB(engineUUID, false)
	if err != nil {
		return err
	}

	sstDir := engineSSTDir(local.localStoreDir, engineUUID)
	if err := os.RemoveAll(sstDir); err != nil {
		return errors.Trace(err)
	}
	if !common.IsDirExists(sstDir) {
		if err := os.Mkdir(sstDir, 0o755); err != nil {
			return errors.Trace(err)
		}
	}
	engineCtx, cancel := context.WithCancel(ctx)
	e, _ := local.engines.LoadOrStore(engineUUID, &File{
		UUID:         engineUUID,
		sstDir:       sstDir,
		sstMetasChan: make(chan metaOrFlush, 64),
		ctx:          engineCtx,
		cancel:       cancel,
		config:       engineCfg,
	})
	engine := e.(*File)
	engine.db = db
	engine.sstIngester = dbSSTIngester{e: engine}
	engine.loadEngineMeta()
	engine.wg.Add(1)
	go engine.ingestSSTLoop()
	return nil
}

// Close backend engine by uuid
// NOTE: we will return nil if engine is not exist. This will happen if engine import&cleanup successfully
// but exit before update checkpoint. Thus after restart, we will try to import this engine again.
func (local *local) CloseEngine(ctx context.Context, engineUUID uuid.UUID) error {
	// flush mem table to storage, to free memory,
	// ask others' advise, looks like unnecessary, but with this we can control memory precisely.
	engine, ok := local.engines.Load(engineUUID)
	if !ok {
		// recovery mode, we should reopen this engine file
		db, err := local.openEngineDB(engineUUID, true)
		if err != nil {
			// if engine db does not exist, just skip
			if os.IsNotExist(errors.Cause(err)) {
				return nil
			}
			return err
		}
		engineFile := &File{
			UUID:         engineUUID,
			db:           db,
			sstMetasChan: make(chan metaOrFlush),
		}
		engineFile.sstIngester = dbSSTIngester{e: engineFile}
		engineFile.loadEngineMeta()
		local.engines.Store(engineUUID, engineFile)
		return nil
	}

	engineFile := engine.(*File)
	engineFile.rLock()
	err := engineFile.flushEngineWithoutLock(ctx)
	engineFile.rUnlock()
	close(engineFile.sstMetasChan)
	if err != nil {
		return errors.Trace(err)
	}
	engineFile.wg.Wait()
	return engineFile.ingestErr.Get()
}

func (local *local) getImportClient(ctx context.Context, peer *metapb.Peer) (sst.ImportSSTClient, error) {
	local.conns.mu.Lock()
	defer local.conns.mu.Unlock()

	conn, err := local.getGrpcConnLocked(ctx, peer.GetStoreId())
	if err != nil {
		return nil, err
	}
	return sst.NewImportSSTClient(conn), nil
}

type rangeStats struct {
	count      int64
	totalBytes int64
}

// WriteToTiKV writer engine key-value pairs to tikv and return the sst meta generated by tikv.
// we don't need to do cleanup for the pairs written to tikv if encounters an error,
// tikv will takes the responsibility to do so.
func (local *local) WriteToTiKV(
	ctx context.Context,
	engineFile *File,
	region *split.RegionInfo,
	start, end []byte,
) ([]*sst.SSTMeta, Range, rangeStats, error) {
	begin := time.Now()
	regionRange := intersectRange(region.Region, Range{start: start, end: end})
	opt := &pebble.IterOptions{LowerBound: regionRange.start, UpperBound: regionRange.end}
	iter := engineFile.db.NewIter(opt)
	defer iter.Close()

	stats := rangeStats{}

	iter.First()
	if iter.Error() != nil {
		return nil, Range{}, stats, errors.Annotate(iter.Error(), "failed to read the first key")
	}
	if !iter.Valid() {
		log.L().Info("keys within region is empty, skip ingest", logutil.Key("start", start),
			logutil.Key("regionStart", region.Region.StartKey), logutil.Key("end", end),
			logutil.Key("regionEnd", region.Region.EndKey))
		return nil, regionRange, stats, nil
	}

	firstKey := codec.EncodeBytes([]byte{}, iter.Key())
	iter.Last()
	if iter.Error() != nil {
		return nil, Range{}, stats, errors.Annotate(iter.Error(), "failed to seek to the last key")
	}
	lastKey := codec.EncodeBytes([]byte{}, iter.Key())

	u := uuid.New()
	meta := &sst.SSTMeta{
		Uuid:        u[:],
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Range: &sst.Range{
			Start: firstKey,
			End:   lastKey,
		},
	}

	leaderID := region.Leader.GetId()
	clients := make([]sst.ImportSST_WriteClient, 0, len(region.Region.GetPeers()))
	requests := make([]*sst.WriteRequest, 0, len(region.Region.GetPeers()))
	for _, peer := range region.Region.GetPeers() {
		cli, err := local.getImportClient(ctx, peer)
		if err != nil {
			return nil, Range{}, stats, err
		}

		wstream, err := cli.Write(ctx)
		if err != nil {
			return nil, Range{}, stats, errors.Trace(err)
		}

		// Bind uuid for this write request
		req := &sst.WriteRequest{
			Chunk: &sst.WriteRequest_Meta{
				Meta: meta,
			},
		}
		if err = wstream.Send(req); err != nil {
			return nil, Range{}, stats, errors.Trace(err)
		}
		req.Chunk = &sst.WriteRequest_Batch{
			Batch: &sst.WriteBatch{
				CommitTs: engineFile.TS,
			},
		}
		clients = append(clients, wstream)
		requests = append(requests, req)
	}

	bytesBuf := newBytesBuffer()
	defer bytesBuf.destroy()
	pairs := make([]*sst.Pair, 0, local.batchWriteKVPairs)
	count := 0
	size := int64(0)
	totalCount := int64(0)
	firstLoop := true
	regionMaxSize := local.regionSplitSize * 4 / 3

	for iter.First(); iter.Valid(); iter.Next() {
		size += int64(len(iter.Key()) + len(iter.Value()))
		// here we reuse the `*sst.Pair`s to optimize object allocation
		if firstLoop {
			pair := &sst.Pair{
				Key:   bytesBuf.addBytes(iter.Key()),
				Value: bytesBuf.addBytes(iter.Value()),
			}
			pairs = append(pairs, pair)
		} else {
			pairs[count].Key = bytesBuf.addBytes(iter.Key())
			pairs[count].Value = bytesBuf.addBytes(iter.Value())
		}
		count++
		totalCount++

		if count >= local.batchWriteKVPairs {
			for i := range clients {
				requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
				if err := clients[i].Send(requests[i]); err != nil {
					return nil, Range{}, stats, errors.Trace(err)
				}
			}
			count = 0
			bytesBuf.reset()
			firstLoop = false
		}
		if size >= regionMaxSize || totalCount >= regionMaxKeyCount {
			break
		}
	}

	if iter.Error() != nil {
		return nil, Range{}, stats, errors.Trace(iter.Error())
	}

	if count > 0 {
		for i := range clients {
			requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
			if err := clients[i].Send(requests[i]); err != nil {
				return nil, Range{}, stats, errors.Trace(err)
			}
		}
	}

	var leaderPeerMetas []*sst.SSTMeta
	for i, wStream := range clients {
		if resp, closeErr := wStream.CloseAndRecv(); closeErr != nil {
			return nil, Range{}, stats, errors.Trace(closeErr)
		} else if leaderID == region.Region.Peers[i].GetId() {
			leaderPeerMetas = resp.Metas
			log.L().Debug("get metas after write kv stream to tikv", zap.Reflect("metas", leaderPeerMetas))
		}
	}

	// if there is not leader currently, we should directly return an error
	if leaderPeerMetas == nil {
		log.L().Warn("write to tikv no leader", logutil.Region(region.Region), logutil.Leader(region.Leader),
			zap.Uint64("leader_id", leaderID), logutil.SSTMeta(meta),
			zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", size))
		return nil, Range{}, stats, errors.Errorf("write to tikv with no leader returned, region '%d', leader: %d",
			region.Region.Id, leaderID)
	}

	log.L().Debug("write to kv", zap.Reflect("region", region), zap.Uint64("leader", leaderID),
		zap.Reflect("meta", meta), zap.Reflect("return metas", leaderPeerMetas),
		zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", size),
		zap.Int64("buf_size", bytesBuf.totalSize()),
		zap.Stringer("takeTime", time.Since(begin)))

	finishedRange := regionRange
	if iter.Valid() && iter.Next() {
		firstKey := append([]byte{}, iter.Key()...)
		finishedRange = Range{start: regionRange.start, end: firstKey}
		log.L().Info("write to tikv partial finish", zap.Int64("count", totalCount),
			zap.Int64("size", size), logutil.Key("startKey", regionRange.start), logutil.Key("endKey", regionRange.end),
			logutil.Key("remainStart", firstKey), logutil.Key("remainEnd", regionRange.end),
			logutil.Region(region.Region), logutil.Leader(region.Leader))
	}
	stats.count = totalCount
	stats.totalBytes = size

	return leaderPeerMetas, finishedRange, stats, nil
}

func (local *local) Ingest(ctx context.Context, meta *sst.SSTMeta, region *split.RegionInfo) (*sst.IngestResponse, error) {
	leader := region.Leader
	if leader == nil {
		leader = region.Region.GetPeers()[0]
	}

	cli, err := local.getImportClient(ctx, leader)
	if err != nil {
		return nil, err
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Peer:        leader,
	}

	req := &sst.IngestRequest{
		Context: reqCtx,
		Sst:     meta,
	}
	resp, err := cli.Ingest(ctx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func splitRangeBySizeProps(fullRange Range, sizeProps *sizeProperties, sizeLimit int64, keysLimit int64) []Range {
	ranges := make([]Range, 0, sizeProps.totalSize/uint64(sizeLimit))
	curSize := uint64(0)
	curKeys := uint64(0)
	curKey := fullRange.start
	sizeProps.iter(func(p *rangeProperty) bool {
		if bytes.Equal(p.Key, engineMetaKey) {
			return true
		}
		curSize += p.Size
		curKeys += p.Keys
		if int64(curSize) >= sizeLimit || int64(curKeys) >= keysLimit {
			// in case the sizeLimit or keysLimit is too small
			endKey := p.Key
			if bytes.Equal(curKey, endKey) {
				endKey = nextKey(endKey)
			}
			ranges = append(ranges, Range{start: curKey, end: endKey})
			curKey = endKey
			curSize = 0
			curKeys = 0
		}
		return true
	})

	if curKeys > 0 {
		ranges = append(ranges, Range{start: curKey, end: fullRange.end})
	} else {
		ranges[len(ranges)-1].end = fullRange.end
	}
	return ranges
}

func (local *local) readAndSplitIntoRange(engineFile *File) ([]Range, error) {
	iter := engineFile.db.NewIter(&pebble.IterOptions{LowerBound: normalIterStartKey})
	defer iter.Close()

	iterError := func(e string) error {
		err := iter.Error()
		if err != nil {
			return errors.Annotate(err, e)
		}
		return errors.New(e)
	}

	var firstKey, lastKey []byte
	if iter.First() {
		firstKey = append([]byte{}, iter.Key()...)
	} else {
		return nil, iterError("could not find first pair")
	}
	if iter.Last() {
		lastKey = append([]byte{}, iter.Key()...)
	} else {
		return nil, iterError("could not find last pair")
	}
	endKey := nextKey(lastKey)

	engineFileTotalSize := engineFile.TotalSize.Load()
	engineFileLength := engineFile.Length.Load()

	// <= 96MB no need to split into range
	if engineFileTotalSize <= local.regionSplitSize && engineFileLength <= regionMaxKeyCount {
		ranges := []Range{{start: firstKey, end: endKey}}
		return ranges, nil
	}

	sizeProps, err := engineFile.getSizeProperties()
	if err != nil {
		return nil, errors.Trace(err)
	}

	ranges := splitRangeBySizeProps(Range{start: firstKey, end: endKey}, sizeProps,
		local.regionSplitSize, regionMaxKeyCount*2/3)

	log.L().Info("split engine key ranges", zap.Stringer("engine", engineFile.UUID),
		zap.Int64("totalSize", engineFileTotalSize), zap.Int64("totalCount", engineFileLength),
		logutil.Key("firstKey", firstKey), logutil.Key("lastKey", lastKey),
		zap.Int("ranges", len(ranges)))

	return ranges, nil
}

type bytesRecycleChan struct {
	ch chan []byte
}

// recycleChan is used for reusing allocated []byte so we can use memory more efficiently
//
// NOTE: we don't used a `sync.Pool` because when will sync.Pool release is depending on the
// garbage collector which always release the memory so late. Use a fixed size chan to reuse
// can decrease the memory usage to 1/3 compare with sync.Pool.
var recycleChan = &bytesRecycleChan{
	ch: make(chan []byte, 1024),
}

func (c *bytesRecycleChan) Acquire() []byte {
	select {
	case b := <-c.ch:
		return b
	default:
		return manual.New(1 << 20) // 1M
	}
}

func (c *bytesRecycleChan) Release(w []byte) {
	select {
	case c.ch <- w:
		return
	default:
		manual.Free(w)
	}
}

type bytesBuffer struct {
	bufs      [][]byte
	curBuf    []byte
	curIdx    int
	curBufIdx int
	curBufLen int
}

func newBytesBuffer() *bytesBuffer {
	return &bytesBuffer{bufs: make([][]byte, 0, 128), curBufIdx: -1}
}

func (b *bytesBuffer) addBuf() {
	if b.curBufIdx < len(b.bufs)-1 {
		b.curBufIdx++
		b.curBuf = b.bufs[b.curBufIdx]
	} else {
		buf := recycleChan.Acquire()
		b.bufs = append(b.bufs, buf)
		b.curBuf = buf
		b.curBufIdx = len(b.bufs) - 1
	}

	b.curBufLen = len(b.curBuf)
	b.curIdx = 0
}

func (b *bytesBuffer) reset() {
	if len(b.bufs) > 0 {
		b.curBuf = b.bufs[0]
		b.curBufLen = len(b.bufs[0])
		b.curBufIdx = 0
		b.curIdx = 0
	}
}

func (b *bytesBuffer) destroy() {
	for _, buf := range b.bufs {
		recycleChan.Release(buf)
	}
	b.bufs = b.bufs[:0]
}

func (b *bytesBuffer) totalSize() int64 {
	return int64(len(b.bufs)) * int64(1<<20)
}

func (b *bytesBuffer) addBytes(bytes []byte) []byte {
	if len(bytes) > bigValueSize {
		return append([]byte{}, bytes...)
	}

	if b.curIdx+len(bytes) > b.curBufLen {
		b.addBuf()
	}
	idx := b.curIdx
	copy(b.curBuf[idx:], bytes)
	b.curIdx += len(bytes)
	return b.curBuf[idx:b.curIdx]
}

func (local *local) writeAndIngestByRange(
	ctxt context.Context,
	engineFile *File,
	start, end []byte,
) error {
	ito := &pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}

	iter := engineFile.db.NewIter(ito)
	defer iter.Close()
	// Needs seek to first because NewIter returns an iterator that is unpositioned
	hasKey := iter.First()
	if iter.Error() != nil {
		return errors.Annotate(iter.Error(), "failed to read the first key")
	}
	if !hasKey {
		log.L().Info("There is no pairs in iterator",
			logutil.Key("start", start),
			logutil.Key("end", end),
			logutil.Key("next end", nextKey(end)))
		engineFile.finishedRanges.add(Range{start: start, end: end})
		return nil
	}
	pairStart := append([]byte{}, iter.Key()...)
	iter.Last()
	if iter.Error() != nil {
		return errors.Annotate(iter.Error(), "failed to seek to the last key")
	}
	pairEnd := append([]byte{}, iter.Key()...)

	var regions []*split.RegionInfo
	var err error
	ctx, cancel := context.WithCancel(ctxt)
	defer cancel()

WriteAndIngest:
	for retry := 0; retry < maxRetryTimes; {
		if retry != 0 {
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		startKey := codec.EncodeBytes([]byte{}, pairStart)
		endKey := codec.EncodeBytes([]byte{}, nextKey(pairEnd))
		regions, err = paginateScanRegion(ctx, local.splitCli, startKey, endKey, 128)
		if err != nil || len(regions) == 0 {
			log.L().Warn("scan region failed", log.ShortError(err), zap.Int("region_len", len(regions)),
				logutil.Key("startKey", startKey), logutil.Key("endKey", endKey), zap.Int("retry", retry))
			retry++
			continue WriteAndIngest
		}

		for _, region := range regions {
			log.L().Debug("get region", zap.Int("retry", retry), zap.Binary("startKey", startKey),
				zap.Binary("endKey", endKey), zap.Uint64("id", region.Region.GetId()),
				zap.Stringer("epoch", region.Region.GetRegionEpoch()), zap.Binary("start", region.Region.GetStartKey()),
				zap.Binary("end", region.Region.GetEndKey()), zap.Reflect("peers", region.Region.GetPeers()))

			w := local.ingestConcurrency.Apply()
			err = local.writeAndIngestPairs(ctx, engineFile, region, pairStart, end)
			local.ingestConcurrency.Recycle(w)
			if err != nil {
				if common.IsContextCanceledError(err) {
					return err
				}
				_, regionStart, _ := codec.DecodeBytes(region.Region.StartKey, []byte{})
				// if we have at least succeeded one region, retry without increasing the retry count
				if bytes.Compare(regionStart, pairStart) > 0 {
					pairStart = regionStart
				} else {
					retry++
				}
				log.L().Info("retry write and ingest kv pairs", logutil.Key("startKey", pairStart),
					logutil.Key("endKey", end), log.ShortError(err), zap.Int("retry", retry))
				continue WriteAndIngest
			}
		}

		return err
	}

	return err
}

type retryType int

const (
	retryNone retryType = iota
	retryWrite
	retryIngest
)

func (local *local) writeAndIngestPairs(
	ctx context.Context,
	engineFile *File,
	region *split.RegionInfo,
	start, end []byte,
) error {
	var err error

loopWrite:
	for i := 0; i < maxRetryTimes; i++ {
		var metas []*sst.SSTMeta
		var finishedRange Range
		var rangeStats rangeStats
		metas, finishedRange, rangeStats, err = local.WriteToTiKV(ctx, engineFile, region, start, end)
		if err != nil {
			if common.IsContextCanceledError(err) {
				return err
			}

			log.L().Warn("write to tikv failed", log.ShortError(err), zap.Int("retry", i))
			continue loopWrite
		}

		for _, meta := range metas {
			errCnt := 0
			for errCnt < maxRetryTimes {
				log.L().Debug("ingest meta", zap.Reflect("meta", meta))
				var resp *sst.IngestResponse
				failpoint.Inject("FailIngestMeta", func(val failpoint.Value) {
					// only inject the error once
					switch val.(string) {
					case "notleader":
						resp = &sst.IngestResponse{
							Error: &errorpb.Error{
								NotLeader: &errorpb.NotLeader{
									RegionId: region.Region.Id,
									Leader:   region.Leader,
								},
							},
						}
					case "epochnotmatch":
						resp = &sst.IngestResponse{
							Error: &errorpb.Error{
								EpochNotMatch: &errorpb.EpochNotMatch{
									CurrentRegions: []*metapb.Region{region.Region},
								},
							},
						}
					}
					if resp != nil {
						err = nil
					}
				})
				if resp == nil {
					resp, err = local.Ingest(ctx, meta, region)
				}
				if err != nil {
					if common.IsContextCanceledError(err) {
						return err
					}
					log.L().Warn("ingest failed", log.ShortError(err), logutil.SSTMeta(meta),
						logutil.Region(region.Region), logutil.Leader(region.Leader))
					errCnt++
					continue
				}

				var retryTy retryType
				var newRegion *split.RegionInfo
				retryTy, newRegion, err = local.isIngestRetryable(ctx, resp, region, meta)
				if common.IsContextCanceledError(err) {
					return err
				}
				if err == nil {
					// ingest next meta
					break
				}
				switch retryTy {
				case retryNone:
					log.L().Warn("ingest failed noretry", log.ShortError(err), logutil.SSTMeta(meta),
						logutil.Region(region.Region), logutil.Leader(region.Leader))
					// met non-retryable error retry whole Write procedure
					return err
				case retryWrite:
					region = newRegion
					continue loopWrite
				case retryIngest:
					region = newRegion
					continue
				}
			}
		}

		if err != nil {
			log.L().Warn("write and ingest region, will retry import full range", log.ShortError(err),
				logutil.Region(region.Region), logutil.Key("start", start),
				logutil.Key("end", end))
		} else {
			engineFile.importedKVSize.Add(rangeStats.totalBytes)
			engineFile.importedKVCount.Add(rangeStats.count)
			engineFile.finishedRanges.add(finishedRange)
			metric.BytesCounter.WithLabelValues(metric.TableStateImported).Add(float64(rangeStats.totalBytes))
		}
		return errors.Trace(err)
	}

	return errors.Trace(err)
}

func (local *local) writeAndIngestByRanges(ctx context.Context, engineFile *File, ranges []Range) error {
	if engineFile.Length.Load() == 0 {
		// engine is empty, this is likes because it's a index engine but the table contains no index
		log.L().Info("engine contains no data", zap.Stringer("uuid", engineFile.UUID))
		return nil
	}
	log.L().Debug("the ranges Length write to tikv", zap.Int("Length", len(ranges)))

	var allErrLock sync.Mutex
	var allErr error
	var wg sync.WaitGroup

	wg.Add(len(ranges))

	for _, r := range ranges {
		startKey := r.start
		endKey := r.end
		w := local.rangeConcurrency.Apply()
		go func(w *worker.Worker) {
			defer func() {
				local.rangeConcurrency.Recycle(w)
				wg.Done()
			}()
			var err error
			// max retry backoff time: 2+4+8+16=30s
			backOffTime := time.Second
			for i := 0; i < maxRetryTimes; i++ {
				err = local.writeAndIngestByRange(ctx, engineFile, startKey, endKey)
				if err == nil || common.IsContextCanceledError(err) {
					return
				}
				log.L().Warn("write and ingest by range failed",
					zap.Int("retry time", i+1), log.ShortError(err))
				backOffTime *= 2
				select {
				case <-time.After(backOffTime):
				case <-ctx.Done():
					return
				}
			}

			allErrLock.Lock()
			allErr = multierr.Append(allErr, err)
			allErrLock.Unlock()
		}(w)
	}

	// wait for all sub tasks finish to avoid panic. if we return on the first error,
	// the outer tasks may close the pebble db but some sub tasks still read from the db
	wg.Wait()
	return allErr
}

type syncedRanges struct {
	sync.Mutex
	ranges []Range
}

func (r *syncedRanges) add(g Range) {
	r.Lock()
	r.ranges = append(r.ranges, g)
	r.Unlock()
}

func (r *syncedRanges) reset() {
	r.Lock()
	r.ranges = r.ranges[:0]
	r.Unlock()
}

func (local *local) ImportEngine(ctx context.Context, engineUUID uuid.UUID) error {
	lf := local.lockEngine(engineUUID, importMutexStateImport)
	if lf == nil {
		// skip if engine not exist. See the comment of `CloseEngine` for more detail.
		return nil
	}
	defer lf.unlock()

	lfTotalSize := lf.TotalSize.Load()
	lfLength := lf.Length.Load()
	if lfTotalSize == 0 {
		log.L().Info("engine contains no kv, skip import", zap.Stringer("engine", engineUUID))
		return nil
	}

	// split sorted file into range by 96MB size per file
	ranges, err := local.readAndSplitIntoRange(lf)
	if err != nil {
		return err
	}

	log.L().Info("start import engine", zap.Stringer("uuid", engineUUID),
		zap.Int("ranges", len(ranges)))
	for {
		unfinishedRanges := lf.unfinishedRanges(ranges)
		if len(unfinishedRanges) == 0 {
			break
		}
		log.L().Info("import engine unfinished ranges", zap.Int("count", len(unfinishedRanges)))

		// if all the kv can fit in one region, skip split regions. TiDB will split one region for
		// the table when table is created.
		needSplit := len(unfinishedRanges) > 1 || lfTotalSize > local.regionSplitSize || lfLength > regionMaxKeyCount
		// split region by given ranges
		for i := 0; i < maxRetryTimes; i++ {
			err = local.SplitAndScatterRegionByRanges(ctx, unfinishedRanges, needSplit)
			if err == nil || common.IsContextCanceledError(err) {
				break
			}

			log.L().Warn("split and scatter failed in retry", zap.Stringer("uuid", engineUUID),
				log.ShortError(err), zap.Int("retry", i))
		}
		if err != nil {
			log.L().Error("split & scatter ranges failed", zap.Stringer("uuid", engineUUID), log.ShortError(err))
			return err
		}

		// start to write to kv and ingest
		err = local.writeAndIngestByRanges(ctx, lf, unfinishedRanges)
		if err != nil {
			log.L().Error("write and ingest engine failed", log.ShortError(err))
			return err
		}
	}

	log.L().Info("import engine success", zap.Stringer("uuid", engineUUID),
		zap.Int64("size", lfTotalSize), zap.Int64("kvs", lfLength),
		zap.Int64("importedSize", lf.importedKVSize.Load()), zap.Int64("importedCount", lf.importedKVCount.Load()))
	return nil
}

func (engine *File) unfinishedRanges(ranges []Range) []Range {
	engine.finishedRanges.Lock()
	defer engine.finishedRanges.Unlock()

	engine.finishedRanges.ranges = sortAndMergeRanges(engine.finishedRanges.ranges)

	return filterOverlapRange(ranges, engine.finishedRanges.ranges)
}

// sortAndMergeRanges sort the ranges and merge range that overlaps with each other into a single range.
func sortAndMergeRanges(ranges []Range) []Range {
	if len(ranges) == 0 {
		return ranges
	}

	sort.Slice(ranges, func(i, j int) bool {
		return bytes.Compare(ranges[i].start, ranges[j].start) < 0
	})

	curEnd := ranges[0].end
	i := 0
	for j := 1; j < len(ranges); j++ {
		if bytes.Compare(curEnd, ranges[j].start) >= 0 {
			if bytes.Compare(curEnd, ranges[j].end) < 0 {
				curEnd = ranges[j].end
			}
		} else {
			ranges[i].end = curEnd
			i++
			ranges[i].start = ranges[j].start
			curEnd = ranges[j].end
		}
	}
	ranges[i].end = curEnd
	return ranges[:i+1]
}

func filterOverlapRange(ranges []Range, finishedRanges []Range) []Range {
	if len(finishedRanges) == 0 {
		return ranges
	}

	result := make([]Range, 0, len(ranges))
	rIdx := 0
	fIdx := 0
	for rIdx < len(ranges) && fIdx < len(finishedRanges) {
		if bytes.Compare(ranges[rIdx].end, finishedRanges[fIdx].start) <= 0 {
			result = append(result, ranges[rIdx])
			rIdx++
		} else if bytes.Compare(ranges[rIdx].start, finishedRanges[fIdx].end) >= 0 {
			fIdx++
		} else if bytes.Compare(ranges[rIdx].start, finishedRanges[fIdx].start) < 0 {
			result = append(result, Range{start: ranges[rIdx].start, end: finishedRanges[fIdx].start})
			switch bytes.Compare(ranges[rIdx].end, finishedRanges[fIdx].end) {
			case -1:
				rIdx++
			case 0:
				rIdx++
				fIdx++
			case 1:
				ranges[rIdx].start = finishedRanges[fIdx].end
				fIdx++
			}
		} else if bytes.Compare(ranges[rIdx].end, finishedRanges[fIdx].end) > 0 {
			ranges[rIdx].start = finishedRanges[fIdx].end
			fIdx++
		} else {
			rIdx++
		}
	}

	if rIdx < len(ranges) {
		result = append(result, ranges[rIdx:]...)
	}

	return result
}

func (local *local) ResetEngine(ctx context.Context, engineUUID uuid.UUID) error {
	// the only way to reset the engine + reclaim the space is to delete and reopen it 🤷
	localEngine := local.lockEngine(engineUUID, importMutexStateClose)
	if localEngine == nil {
		log.L().Warn("could not find engine in cleanupEngine", zap.Stringer("uuid", engineUUID))
		return nil
	}
	defer localEngine.unlock()
	if err := localEngine.Close(); err != nil {
		return err
	}
	if err := localEngine.Cleanup(local.localStoreDir); err != nil {
		return err
	}
	db, err := local.openEngineDB(engineUUID, false)
	if err == nil {
		localEngine.db = db
		localEngine.localFileMeta = localFileMeta{}
		if !common.IsDirExists(localEngine.sstDir) {
			if err := os.Mkdir(localEngine.sstDir, 0o755); err != nil {
				return errors.Trace(err)
			}
		}
	}
	localEngine.pendingFileSize.Store(0)
	localEngine.finishedRanges.reset()

	return err
}

func (local *local) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	localEngine := local.lockEngine(engineUUID, importMutexStateClose)
	// release this engine after import success
	if localEngine == nil {
		log.L().Warn("could not find engine in cleanupEngine", zap.Stringer("uuid", engineUUID))
		return nil
	}
	defer localEngine.unlock()

	// since closing the engine causes all subsequent operations on it panic,
	// we make sure to delete it from the engine map before calling Close().
	// (note that Close() returning error does _not_ mean the pebble DB
	// remains open/usable.)
	local.engines.Delete(engineUUID)
	err := localEngine.Close()
	if err != nil {
		return err
	}
	err = localEngine.Cleanup(local.localStoreDir)
	if err != nil {
		return err
	}
	localEngine.TotalSize.Store(0)
	localEngine.Length.Store(0)
	return nil
}

func (local *local) CheckRequirements(ctx context.Context, checkCtx *backend.CheckCtx) error {
	versionStr, err := local.g.GetSQLExecutor().ObtainStringWithLog(
		ctx,
		"SELECT version();",
		"check TiDB version",
		log.L())
	if err != nil {
		return errors.Trace(err)
	}
	if err := checkTiDBVersion(ctx, versionStr, localMinTiDBVersion, localMaxTiDBVersion); err != nil {
		return err
	}
	if err := tikv.CheckPDVersion(ctx, local.tls, local.pdAddr, localMinPDVersion, localMaxPDVersion); err != nil {
		return err
	}
	if err := tikv.CheckTiKVVersion(ctx, local.tls, local.pdAddr, localMinTiKVVersion, localMaxTiKVVersion); err != nil {
		return err
	}

	tidbVersion, _ := version.ExtractTiDBVersion(versionStr)

	return checkTiFlashVersion(ctx, local.g, checkCtx, *tidbVersion)
}

func checkTiDBVersion(_ context.Context, versionStr string, requiredMinVersion, requiredMaxVersion semver.Version) error {
	return version.CheckTiDBVersion(versionStr, requiredMinVersion, requiredMaxVersion)
}

var tiFlashReplicaQuery = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TIFLASH_REPLICA WHERE REPLICA_COUNT > 0;"

type tblName struct {
	schema string
	name   string
}

type tblNames []tblName

func (t tblNames) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, n := range t {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(common.UniqueTable(n.schema, n.name))
	}
	b.WriteByte(']')
	return b.String()
}

// check TiFlash replicas.
// local backend doesn't support TiFlash before tidb v4.0.5
func checkTiFlashVersion(ctx context.Context, g glue.Glue, checkCtx *backend.CheckCtx, tidbVersion semver.Version) error {
	if tidbVersion.Compare(tiFlashMinVersion) >= 0 {
		return nil
	}

	res, err := g.GetSQLExecutor().QueryStringsWithLog(ctx, tiFlashReplicaQuery, "fetch tiflash replica info", log.L())
	if err != nil {
		return errors.Annotate(err, "fetch tiflash replica info failed")
	}

	tiFlashTablesMap := make(map[tblName]struct{}, len(res))
	for _, tblInfo := range res {
		name := tblName{schema: tblInfo[0], name: tblInfo[1]}
		tiFlashTablesMap[name] = struct{}{}
	}

	tiFlashTables := make(tblNames, 0)
	for _, dbMeta := range checkCtx.DBMetas {
		for _, tblMeta := range dbMeta.Tables {
			if len(tblMeta.DataFiles) == 0 {
				continue
			}
			name := tblName{schema: tblMeta.DB, name: tblMeta.Name}
			if _, ok := tiFlashTablesMap[name]; ok {
				tiFlashTables = append(tiFlashTables, name)
			}
		}
	}

	if len(tiFlashTables) > 0 {
		helpInfo := "Please either upgrade TiDB to version >= 4.0.5 or add TiFlash replica after load data."
		return errors.Errorf("lightning local backend doesn't support TiFlash in this TiDB version. conflict tables: %s. "+helpInfo, tiFlashTables)
	}
	return nil
}

func (local *local) FetchRemoteTableModels(ctx context.Context, schemaName string) ([]*model.TableInfo, error) {
	return tikv.FetchRemoteTableModelsFromTLS(ctx, local.tls, schemaName)
}

func (local *local) MakeEmptyRows() kv.Rows {
	return kv.MakeRowsFromKvPairs(nil)
}

func (local *local) NewEncoder(tbl table.Table, options *kv.SessionOptions) (kv.Encoder, error) {
	return kv.NewTableKVEncoder(tbl, options)
}

func engineSSTDir(storeDir string, engineUUID uuid.UUID) string {
	return filepath.Join(storeDir, engineUUID.String()+".sst")
}

func (local *local) LocalWriter(ctx context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	e, ok := local.engines.Load(engineUUID)
	if !ok {
		return nil, errors.Errorf("could not find engine for %s", engineUUID.String())
	}
	engineFile := e.(*File)
	return openLocalWriter(ctx, cfg, engineFile, local.localWriterMemCacheSize)
}

func openLocalWriter(ctx context.Context, cfg *backend.LocalWriterConfig, f *File, cacheSize int64) (*Writer, error) {
	w := &Writer{
		local:              f,
		memtableSizeLimit:  cacheSize,
		kvBuffer:           newBytesBuffer(),
		isWriteBatchSorted: cfg.IsKVSorted,
	}
	f.localWriters.Store(w, nil)
	return w, nil
}

func (local *local) isIngestRetryable(
	ctx context.Context,
	resp *sst.IngestResponse,
	region *split.RegionInfo,
	meta *sst.SSTMeta,
) (retryType, *split.RegionInfo, error) {
	if resp.GetError() == nil {
		return retryNone, nil, nil
	}

	getRegion := func() (*split.RegionInfo, error) {
		for i := 0; ; i++ {
			newRegion, err := local.splitCli.GetRegion(ctx, region.Region.GetStartKey())
			if err != nil {
				return nil, errors.Trace(err)
			}
			if newRegion != nil {
				return newRegion, nil
			}
			log.L().Warn("get region by key return nil, will retry", logutil.Region(region.Region), logutil.Leader(region.Leader),
				zap.Int("retry", i))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}

	var newRegion *split.RegionInfo
	var err error
	switch errPb := resp.GetError(); {
	case errPb.NotLeader != nil:
		if newLeader := errPb.GetNotLeader().GetLeader(); newLeader != nil {
			newRegion = &split.RegionInfo{
				Leader: newLeader,
				Region: region.Region,
			}
		} else {
			newRegion, err = getRegion()
			if err != nil {
				return retryNone, nil, errors.Trace(err)
			}
		}
		// TODO: because in some case, TiKV may return retryable error while the ingest is succeeded.
		// Thus directly retry ingest may cause TiKV panic. So always return retryWrite here to avoid
		// this issue.
		// See: https://github.com/tikv/tikv/issues/9496
		return retryWrite, newRegion, errors.Errorf("not leader: %s", errPb.GetMessage())
	case errPb.EpochNotMatch != nil:
		if currentRegions := errPb.GetEpochNotMatch().GetCurrentRegions(); currentRegions != nil {
			var currentRegion *metapb.Region
			for _, r := range currentRegions {
				if insideRegion(r, meta) {
					currentRegion = r
					break
				}
			}
			if currentRegion != nil {
				var newLeader *metapb.Peer
				for _, p := range currentRegion.Peers {
					if p.GetStoreId() == region.Leader.GetStoreId() {
						newLeader = p
						break
					}
				}
				if newLeader != nil {
					newRegion = &split.RegionInfo{
						Leader: newLeader,
						Region: currentRegion,
					}
				}
			}
		}
		retryTy := retryNone
		if newRegion != nil {
			retryTy = retryWrite
		}
		return retryTy, newRegion, errors.Errorf("epoch not match: %s", errPb.GetMessage())
	case strings.Contains(errPb.Message, "raft: proposal dropped"):
		// TODO: we should change 'Raft raft: proposal dropped' to a error type like 'NotLeader'
		newRegion, err = getRegion()
		if err != nil {
			return retryNone, nil, errors.Trace(err)
		}
		return retryWrite, newRegion, errors.New(errPb.GetMessage())
	}
	return retryNone, nil, errors.Errorf("non-retryable error: %s", resp.GetError().GetMessage())
}

// return the smallest []byte that is bigger than current bytes.
// special case when key is empty, empty bytes means infinity in our context, so directly return itself.
func nextKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{}
	}

	// in tikv <= 4.x, tikv will truncate the row key, so we should fetch the next valid row key
	// See: https://github.com/tikv/tikv/blob/f7f22f70e1585d7ca38a59ea30e774949160c3e8/components/raftstore/src/coprocessor/split_observer.rs#L36-L41
	if tablecodec.IsRecordKey(key) {
		tableID, handle, _ := tablecodec.DecodeRecordKey(key)
		return tablecodec.EncodeRowKeyWithHandle(tableID, handle.Next())
	}

	// if key is an index, directly append a 0x00 to the key.
	res := make([]byte, 0, len(key)+1)
	res = append(res, key...)
	res = append(res, 0)
	return res
}

type rangeOffsets struct {
	Size uint64
	Keys uint64
}

type rangeProperty struct {
	Key []byte
	rangeOffsets
}

func (r *rangeProperty) Less(than btree.Item) bool {
	ta := than.(*rangeProperty)
	return bytes.Compare(r.Key, ta.Key) < 0
}

var _ btree.Item = &rangeProperty{}

type rangeProperties []rangeProperty

func decodeRangeProperties(data []byte) (rangeProperties, error) {
	r := make(rangeProperties, 0, 16)
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, io.ErrUnexpectedEOF
		}
		keyLen := int(binary.BigEndian.Uint32(data[:4]))
		data = data[4:]
		if len(data) < keyLen+8*2 {
			return nil, io.ErrUnexpectedEOF
		}
		key := data[:keyLen]
		data = data[keyLen:]
		size := binary.BigEndian.Uint64(data[:8])
		keys := binary.BigEndian.Uint64(data[8:])
		data = data[16:]
		r = append(r, rangeProperty{Key: key, rangeOffsets: rangeOffsets{Size: size, Keys: keys}})
	}

	return r, nil
}

func (r rangeProperties) Encode() []byte {
	b := make([]byte, 0, 1024)
	idx := 0
	for _, p := range r {
		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(len(p.Key)))
		idx += 4
		b = append(b, p.Key...)
		idx += len(p.Key)

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.Size)
		idx += 8

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.Keys)
		idx += 8
	}
	return b
}

func (r rangeProperties) get(key []byte) rangeOffsets {
	idx := sort.Search(len(r), func(i int) bool {
		return bytes.Compare(r[i].Key, key) >= 0
	})
	return r[idx].rangeOffsets
}

type RangePropertiesCollector struct {
	props               rangeProperties
	lastOffsets         rangeOffsets
	lastKey             []byte
	currentOffsets      rangeOffsets
	propSizeIdxDistance uint64
	propKeysIdxDistance uint64
}

func newRangePropertiesCollector() pebble.TablePropertyCollector {
	return &RangePropertiesCollector{
		props:               make([]rangeProperty, 0, 1024),
		propSizeIdxDistance: defaultPropSizeIndexDistance,
		propKeysIdxDistance: defaultPropKeysIndexDistance,
	}
}

func (c *RangePropertiesCollector) sizeInLastRange() uint64 {
	return c.currentOffsets.Size - c.lastOffsets.Size
}

func (c *RangePropertiesCollector) keysInLastRange() uint64 {
	return c.currentOffsets.Keys - c.lastOffsets.Keys
}

func (c *RangePropertiesCollector) insertNewPoint(key []byte) {
	c.lastOffsets = c.currentOffsets
	c.props = append(c.props, rangeProperty{Key: append([]byte{}, key...), rangeOffsets: c.currentOffsets})
}

// implement `pebble.TablePropertyCollector`
// implement `TablePropertyCollector.Add`
func (c *RangePropertiesCollector) Add(key pebble.InternalKey, value []byte) error {
	c.currentOffsets.Size += uint64(len(value)) + uint64(len(key.UserKey))
	c.currentOffsets.Keys++
	if len(c.lastKey) == 0 || c.sizeInLastRange() >= c.propSizeIdxDistance ||
		c.keysInLastRange() >= c.propKeysIdxDistance {
		c.insertNewPoint(key.UserKey)
	}
	c.lastKey = append(c.lastKey[:0], key.UserKey...)
	return nil
}

func (c *RangePropertiesCollector) Finish(userProps map[string]string) error {
	if c.sizeInLastRange() > 0 || c.keysInLastRange() > 0 {
		c.insertNewPoint(c.lastKey)
	}

	userProps[propRangeIndex] = string(c.props.Encode())
	return nil
}

// The name of the property collector.
func (c *RangePropertiesCollector) Name() string {
	return propRangeIndex
}

type sizeProperties struct {
	totalSize    uint64
	indexHandles *btree.BTree
}

func newSizeProperties() *sizeProperties {
	return &sizeProperties{indexHandles: btree.New(32)}
}

func (s *sizeProperties) add(item *rangeProperty) {
	if old := s.indexHandles.ReplaceOrInsert(item); old != nil {
		o := old.(*rangeProperty)
		item.Keys += o.Keys
		item.Size += o.Size
	}
}

func (s *sizeProperties) addAll(props rangeProperties) {
	prevRange := rangeOffsets{}
	for _, r := range props {
		s.add(&rangeProperty{
			Key:          r.Key,
			rangeOffsets: rangeOffsets{Keys: r.Keys - prevRange.Keys, Size: r.Size - prevRange.Size},
		})
		prevRange = r.rangeOffsets
	}
	if len(props) > 0 {
		s.totalSize = props[len(props)-1].Size
	}
}

// iter the tree until f return false
func (s *sizeProperties) iter(f func(p *rangeProperty) bool) {
	s.indexHandles.Ascend(func(i btree.Item) bool {
		prop := i.(*rangeProperty)
		return f(prop)
	})
}

type sstMeta struct {
	path       string
	minKey     []byte
	maxKey     []byte
	totalSize  int64
	totalCount int64
	// used for calculate disk-quota
	fileSize int64
}

type Writer struct {
	sync.Mutex
	local              *File
	sstDir             string
	memtableSizeLimit  int64
	writeBatch         []common.KvPair
	isWriteBatchSorted bool

	batchCount int
	batchSize  int64
	totalSize  int64
	totalCount int64

	kvBuffer *bytesBuffer
	writer   *sstWriter
}

func (w *Writer) appendRowsSorted(kvs []common.KvPair) error {
	if w.writer == nil {
		writer, err := w.createSSTWriter()
		if err != nil {
			return errors.Trace(err)
		}
		w.writer = writer
		w.writer.minKey = append([]byte{}, kvs[0].Key...)
	}
	for _, pair := range kvs {
		w.batchSize += int64(len(pair.Key) + len(pair.Val))
	}
	w.batchCount += len(kvs)
	w.totalCount += int64(len(kvs))
	return w.writer.writeKVs(kvs)
}

func (w *Writer) appendRowsUnsorted(ctx context.Context, kvs []common.KvPair) error {
	l := len(w.writeBatch)
	cnt := w.batchCount
	for _, pair := range kvs {
		w.batchSize += int64(len(pair.Key) + len(pair.Val))
		key := w.kvBuffer.addBytes(pair.Key)
		val := w.kvBuffer.addBytes(pair.Val)
		if cnt < l {
			w.writeBatch[cnt].Key = key
			w.writeBatch[cnt].Val = val
		} else {
			w.writeBatch = append(w.writeBatch, common.KvPair{Key: key, Val: val})
		}
		cnt++
	}
	w.batchCount = cnt

	if w.batchSize > w.memtableSizeLimit {
		if err := w.flushKVs(ctx); err != nil {
			return err
		}
	}
	w.totalCount += int64(len(kvs))
	return nil
}

func (local *local) EngineFileSizes() (res []backend.EngineFileSize) {
	local.engines.Range(func(k, v interface{}) bool {
		engine := v.(*File)
		res = append(res, engine.getEngineFileSize())
		return true
	})
	return
}

func (w *Writer) AppendRows(ctx context.Context, tableName string, columnNames []string, ts uint64, rows kv.Rows) error {
	kvs := kv.KvPairsFromRows(rows)
	if len(kvs) == 0 {
		return nil
	}

	w.Lock()
	defer w.Unlock()

	// if chunk has _tidb_rowid field, we can't ensure that the rows are sorted.
	if w.isWriteBatchSorted && w.writer == nil {
		for _, c := range columnNames {
			if c == model.ExtraHandleName.L {
				w.isWriteBatchSorted = false
			}
		}
	}

	w.local.TS = ts
	if w.isWriteBatchSorted {
		return w.appendRowsSorted(kvs)
	}
	return w.appendRowsUnsorted(ctx, kvs)
}

func (w *Writer) flush(ctx context.Context) error {
	w.Lock()
	defer w.Unlock()
	if w.batchCount == 0 {
		return nil
	}

	w.totalSize += w.batchSize
	if len(w.writeBatch) > 0 {
		if err := w.flushKVs(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	if w.writer != nil {
		meta, err := w.writer.close()
		if err != nil {
			return errors.Trace(err)
		}
		w.writer = nil
		if meta != nil && meta.totalSize > 0 {
			return w.local.addSST(ctx, meta)
		}
	}

	return nil
}

func (w *Writer) Close(ctx context.Context) error {
	defer w.kvBuffer.destroy()
	defer w.local.localWriters.Delete(w)
	err := w.flush(ctx)
	// FIXME: in theory this line is useless, but In our benchmark with go1.15
	// this can resolve the memory consistently increasing issue.
	// maybe this is a bug related to go GC mechanism.
	w.writeBatch = nil
	return err
}

func (w *Writer) flushKVs(ctx context.Context) error {
	writer, err := w.createSSTWriter()
	if err != nil {
		return errors.Trace(err)
	}
	sort.Slice(w.writeBatch[:w.batchCount], func(i, j int) bool {
		return bytes.Compare(w.writeBatch[i].Key, w.writeBatch[j].Key) < 0
	})
	writer.minKey = append(writer.minKey[:0], w.writeBatch[0].Key...)
	err = writer.writeKVs(w.writeBatch[:w.batchCount])
	if err != nil {
		return errors.Trace(err)
	}
	meta, err := writer.close()
	if err != nil {
		return errors.Trace(err)
	}
	err = w.local.addSST(ctx, meta)
	if err != nil {
		return errors.Trace(err)
	}

	w.totalSize += w.batchSize
	w.batchSize = 0
	w.batchCount = 0
	w.kvBuffer.reset()
	return nil
}

func (w *Writer) createSSTWriter() (*sstWriter, error) {
	path := filepath.Join(w.local.sstDir, uuid.New().String()+".sst")
	writer, err := newSSTWriter(path)
	if err != nil {
		return nil, err
	}
	sw := &sstWriter{sstMeta: &sstMeta{path: path}, writer: writer}
	return sw, nil
}

var errorUnorderedSSTInsertion = errors.New("inserting KVs into SST without order")

type localIngestDescription uint8

const (
	localIngestDescriptionFlushed localIngestDescription = 1 << iota
	localIngestDescriptionImmediate
)

type sstWriter struct {
	*sstMeta
	writer *sstable.Writer
}

func newSSTWriter(path string) (*sstable.Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	writer := sstable.NewWriter(f, sstable.WriterOptions{
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			newRangePropertiesCollector,
		},
		BlockSize: 16 * 1024,
	})
	return writer, nil
}

func (sw *sstWriter) writeKVs(kvs []common.KvPair) error {
	if len(kvs) == 0 {
		return nil
	}

	if bytes.Compare(kvs[0].Key, sw.maxKey) <= 0 {
		return errorUnorderedSSTInsertion
	}

	internalKey := sstable.InternalKey{
		Trailer: uint64(sstable.InternalKeyKindSet),
	}
	for _, p := range kvs {
		internalKey.UserKey = p.Key
		if err := sw.writer.Add(internalKey, p.Val); err != nil {
			return errors.Trace(err)
		}
		sw.totalSize += int64(len(p.Key)) + int64(len(p.Val))
	}
	sw.totalCount += int64(len(kvs))
	sw.maxKey = append(sw.maxKey[:0], kvs[len(kvs)-1].Key...)
	return nil
}

func (sw *sstWriter) close() (*sstMeta, error) {
	if err := sw.writer.Close(); err != nil {
		return nil, errors.Trace(err)
	}
	meta, err := sw.writer.Metadata()
	if err != nil {
		return nil, errors.Trace(err)
	}
	sw.fileSize = int64(meta.Size)
	return sw.sstMeta, nil
}

type sstIter struct {
	name   string
	key    []byte
	val    []byte
	iter   sstable.Iterator
	reader *sstable.Reader
	valid  bool
}

func (i *sstIter) Close() error {
	if err := i.iter.Close(); err != nil {
		return errors.Trace(err)
	}
	err := i.reader.Close()
	return errors.Trace(err)
}

type sstIterHeap struct {
	iters []*sstIter
}

func (h *sstIterHeap) Len() int {
	return len(h.iters)
}

func (h *sstIterHeap) Less(i, j int) bool {
	return bytes.Compare(h.iters[i].key, h.iters[j].key) < 0
}

func (h *sstIterHeap) Swap(i, j int) {
	h.iters[i], h.iters[j] = h.iters[j], h.iters[i]
}

func (h *sstIterHeap) Push(x interface{}) {
	h.iters = append(h.iters, x.(*sstIter))
}

func (h *sstIterHeap) Pop() interface{} {
	item := h.iters[len(h.iters)-1]
	h.iters = h.iters[:len(h.iters)-1]
	return item
}

func (h *sstIterHeap) Next() ([]byte, []byte, error) {
	for {
		if len(h.iters) == 0 {
			return nil, nil, nil
		}

		iter := h.iters[0]
		if iter.valid {
			iter.valid = false
			return iter.key, iter.val, iter.iter.Error()
		}

		var k *pebble.InternalKey
		k, iter.val = iter.iter.Next()
		if k != nil {
			iter.key = k.UserKey
			iter.valid = true
			heap.Fix(h, 0)
		} else {
			err := iter.Close()
			heap.Remove(h, 0)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
	}
}

// sstIngester is a interface used to merge and ingest SST files.
// it's a interface mainly used for test convenience
type sstIngester interface {
	mergeSSTs(metas []*sstMeta, dir string) (*sstMeta, error)
	ingest([]*sstMeta) error
}

type dbSSTIngester struct {
	e *File
}

func (i dbSSTIngester) mergeSSTs(metas []*sstMeta, dir string) (*sstMeta, error) {
	if len(metas) == 0 {
		return nil, errors.New("sst metas is empty")
	} else if len(metas) == 1 {
		return metas[0], nil
	}

	start := time.Now()
	newMeta := &sstMeta{}
	mergeIter := &sstIterHeap{
		iters: make([]*sstIter, 0, len(metas)),
	}
	for _, p := range metas {
		f, err := os.Open(p.path)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reader, err := sstable.NewReader(f, sstable.ReaderOptions{})
		if err != nil {
			return nil, errors.Trace(err)
		}
		iter, err := reader.NewIter(nil, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		key, val := iter.Next()
		if key == nil {
			continue
		}
		if iter.Error() != nil {
			return nil, errors.Trace(iter.Error())
		}
		mergeIter.iters = append(mergeIter.iters, &sstIter{
			name:   p.path,
			iter:   iter,
			key:    key.UserKey,
			val:    val,
			reader: reader,
			valid:  true,
		})
		newMeta.totalSize += p.totalSize
		newMeta.totalCount += p.totalCount
	}
	heap.Init(mergeIter)

	name := filepath.Join(dir, fmt.Sprintf("%s.sst", uuid.New()))
	writer, err := newSSTWriter(name)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newMeta.path = name

	internalKey := sstable.InternalKey{
		Trailer: uint64(sstable.InternalKeyKindSet),
	}
	key, val, err := mergeIter.Next()
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, errors.New("all ssts are empty!")
	}
	newMeta.minKey = append(newMeta.minKey[:0], key...)
	lastKey := make([]byte, 0)
	for {
		if bytes.Equal(lastKey, key) {
			log.L().Warn("duplicated key found, skipped", zap.Binary("key", lastKey))
			continue
		}
		internalKey.UserKey = key
		err = writer.Add(internalKey, val)
		if err != nil {
			return nil, err
		}
		lastKey = append(lastKey[:0], key...)
		key, val, err = mergeIter.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
	}
	err = writer.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta, err := writer.Metadata()
	if err != nil {
		return nil, errors.Trace(err)
	}
	newMeta.maxKey = lastKey
	newMeta.fileSize = int64(meta.Size)

	dur := time.Since(start)
	log.L().Info("compact sst", zap.Int("fileCount", len(metas)), zap.Int64("size", newMeta.totalSize),
		zap.Int64("count", newMeta.totalCount), zap.Duration("cost", dur), zap.String("file", name))

	// async clean raw SSTs.
	go func() {
		totalSize := int64(0)
		for _, m := range metas {
			totalSize += m.fileSize
			if err := os.Remove(m.path); err != nil {
				log.L().Warn("async cleanup sst file failed", zap.Error(err))
			}
		}
		// decrease the pending size after clean up
		i.e.pendingFileSize.Sub(totalSize)
	}()

	return newMeta, err
}

func (i dbSSTIngester) ingest(metas []*sstMeta) error {
	if len(metas) == 0 {
		return nil
	}
	paths := make([]string, 0, len(metas))
	for _, m := range metas {
		paths = append(paths, m.path)
	}
	return i.e.db.Ingest(paths)
}
