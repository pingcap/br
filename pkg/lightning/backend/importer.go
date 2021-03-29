// Copyright 2019 PingCAP, Inc.
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

package backend

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	kv "github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/table"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/br/pkg/lightning/common"
	"github.com/pingcap/br/pkg/lightning/glue"
	"github.com/pingcap/br/pkg/lightning/log"
	"github.com/pingcap/br/pkg/pdutil"
	"github.com/pingcap/br/pkg/version"
)

const (
	defaultRetryBackoffTime = time.Second * 3
)

var (
	// Importer backend is compatible with TiDB [2.1.0, NextMajorVersion).
	requiredMinTiDBVersion = *semver.New("2.1.0")
	requiredMinPDVersion   = *semver.New("2.1.0")
	requiredMinTiKVVersion = *semver.New("2.1.0")
	requiredMaxTiDBVersion = version.NextMajorVersion()
	requiredMaxPDVersion   = version.NextMajorVersion()
	requiredMaxTiKVVersion = version.NextMajorVersion()
)

// importer represents a gRPC connection to tikv-importer. This type is
// goroutine safe: you can share this instance and execute any method anywhere.
type importer struct {
	conn   *grpc.ClientConn
	cli    kv.ImportKVClient
	pdAddr string
	tls    *common.TLS

	mutationPool sync.Pool
}

// NewImporter creates a new connection to tikv-importer. A single connection
// per tidb-lightning instance is enough.
func NewImporter(ctx context.Context, tls *common.TLS, importServerAddr string, pdAddr string) (Backend, error) {
	conn, err := grpc.DialContext(ctx, importServerAddr, tls.ToGRPCDialOption())
	if err != nil {
		return MakeBackend(nil), errors.Trace(err)
	}

	return MakeBackend(&importer{
		conn:         conn,
		cli:          kv.NewImportKVClient(conn),
		pdAddr:       pdAddr,
		tls:          tls,
		mutationPool: sync.Pool{New: func() interface{} { return &kv.Mutation{} }},
	}), nil
}

// NewMockImporter creates an *unconnected* importer based on a custom
// ImportKVClient. This is provided for testing only. Do not use this function
// outside of tests.
func NewMockImporter(cli kv.ImportKVClient, pdAddr string) Backend {
	return MakeBackend(&importer{
		conn:         nil,
		cli:          cli,
		pdAddr:       pdAddr,
		mutationPool: sync.Pool{New: func() interface{} { return &kv.Mutation{} }},
	})
}

// Close the importer connection.
func (importer *importer) Close() {
	if importer.conn != nil {
		if err := importer.conn.Close(); err != nil {
			log.L().Warn("close importer gRPC connection failed", zap.Error(err))
		}
	}
}

func (*importer) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

func (*importer) MaxChunkSize() int {
	// 31 MB. hardcoded by importer, so do we
	return 31 << 10
}

func (*importer) ShouldPostProcess() bool {
	return true
}

// isIgnorableOpenCloseEngineError checks if the error from
// CloseEngine can be safely ignored.
func isIgnorableOpenCloseEngineError(err error) bool {
	// We allow "FileExists" error. This happens when the engine has been
	// closed before. This error typically arise when resuming from a
	// checkpoint with a partially-imported engine.
	//
	// If the error is legit in a no-checkpoints settings, the later WriteEngine
	// API will bail us out to keep us safe.
	return err == nil || strings.Contains(err.Error(), "FileExists")
}

func (importer *importer) OpenEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &kv.OpenEngineRequest{
		Uuid: engineUUID[:],
	}

	_, err := importer.cli.OpenEngine(ctx, req)
	return errors.Trace(err)
}

func (importer *importer) CloseEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &kv.CloseEngineRequest{
		Uuid: engineUUID[:],
	}

	_, err := importer.cli.CloseEngine(ctx, req)
	if !isIgnorableOpenCloseEngineError(err) {
		return errors.Trace(err)
	}
	return nil
}

func (importer *importer) Flush(_ context.Context, _ uuid.UUID) error {
	return nil
}

func (importer *importer) ImportEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &kv.ImportEngineRequest{
		Uuid:   engineUUID[:],
		PdAddr: importer.pdAddr,
	}

	_, err := importer.cli.ImportEngine(ctx, req)
	return errors.Trace(err)
}

func (importer *importer) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	req := &kv.CleanupEngineRequest{
		Uuid: engineUUID[:],
	}

	_, err := importer.cli.CleanupEngine(ctx, req)
	return errors.Trace(err)
}

func (importer *importer) WriteRows(
	ctx context.Context,
	engineUUID uuid.UUID,
	tableName string,
	columnNames []string,
	ts uint64,
	rows Rows,
) (finalErr error) {
	var err error
outside:
	for _, r := range rows.SplitIntoChunks(importer.MaxChunkSize()) {
		for i := 0; i < maxRetryTimes; i++ {
			err = importer.WriteRowsToImporter(ctx, engineUUID, ts, r)
			switch {
			case err == nil:
				continue outside
			case common.IsRetryableError(err):
				// retry next loop
			default:
				return err
			}
		}
		return errors.Annotatef(err, "[%s] write rows reach max retry %d and still failed", tableName, maxRetryTimes)
	}
	return nil
}

func (importer *importer) WriteRowsToImporter(
	ctx context.Context,
	engineUUID uuid.UUID,
	ts uint64,
	rows Rows,
) (finalErr error) {
	kvs := rows.(kvPairs)
	if len(kvs) == 0 {
		return nil
	}

	wstream, err := importer.cli.WriteEngine(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	logger := log.With(zap.Stringer("engineUUID", engineUUID))

	defer func() {
		resp, closeErr := wstream.CloseAndRecv()
		if closeErr == nil && resp != nil && resp.Error != nil {
			closeErr = errors.Errorf("Engine '%s' not found", resp.Error.EngineNotFound.Uuid)
		}
		if closeErr != nil {
			if finalErr == nil {
				finalErr = errors.Trace(closeErr)
			} else {
				// just log the close error, we need to propagate the earlier error instead
				logger.Warn("close write stream failed", log.ShortError(closeErr))
			}
		}
	}()

	// Bind uuid for this write request
	req := &kv.WriteEngineRequest{
		Chunk: &kv.WriteEngineRequest_Head{
			Head: &kv.WriteHead{
				Uuid: engineUUID[:],
			},
		},
	}
	if err := wstream.Send(req); err != nil {
		return errors.Trace(err)
	}

	// Send kv paris as write request content
	mutations := make([]*kv.Mutation, len(kvs))
	for i, pair := range kvs {
		mutations[i] = importer.mutationPool.Get().(*kv.Mutation)
		mutations[i].Op = kv.Mutation_Put
		mutations[i].Key = pair.Key
		mutations[i].Value = pair.Val
	}

	req.Reset()
	req.Chunk = &kv.WriteEngineRequest_Batch{
		Batch: &kv.WriteBatch{
			CommitTs:  ts,
			Mutations: mutations,
		},
	}

	err = wstream.Send(req)
	for _, mutation := range mutations {
		importer.mutationPool.Put(mutation)
	}

	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (*importer) MakeEmptyRows() Rows {
	return kvPairs(nil)
}

func (*importer) NewEncoder(tbl table.Table, options *SessionOptions) (Encoder, error) {
	return NewTableKVEncoder(tbl, options)
}

func (importer *importer) CheckRequirements(ctx context.Context) error {
	if err := checkTiDBVersionByTLS(ctx, importer.tls, requiredMinTiDBVersion, requiredMaxTiDBVersion); err != nil {
		return err
	}
	if err := checkPDVersion(ctx, importer.tls, importer.pdAddr, requiredMinPDVersion, requiredMaxPDVersion); err != nil {
		return err
	}
	if err := checkTiKVVersion(ctx, importer.tls, importer.pdAddr, requiredMinTiKVVersion, requiredMaxTiKVVersion); err != nil {
		return err
	}
	return nil
}

func checkTiDBVersionByTLS(ctx context.Context, tls *common.TLS, requiredMinVersion, requiredMaxVersion semver.Version) error {
	var status struct{ Version string }
	err := tls.GetJSON(ctx, "/status", &status)
	if err != nil {
		return err
	}

	return checkTiDBVersion(status.Version, requiredMinVersion, requiredMaxVersion)
}

func checkTiDBVersion(versionStr string, requiredMinVersion, requiredMaxVersion semver.Version) error {
	version, err := version.ExtractTiDBVersion(versionStr)
	if err != nil {
		return errors.Trace(err)
	}
	return checkVersion("TiDB", *version, requiredMinVersion, requiredMaxVersion)
}

func checkTiDBVersionBySQL(ctx context.Context, g glue.Glue, requiredMinVersion, requiredMaxVersion semver.Version) error {
	versionStr, err := g.GetSQLExecutor().ObtainStringWithLog(
		ctx,
		"SELECT version();",
		"check TiDB version",
		log.L())
	if err != nil {
		return errors.Trace(err)
	}

	return checkTiDBVersion(versionStr, requiredMinVersion, requiredMaxVersion)
}

func checkPDVersion(ctx context.Context, tls *common.TLS, pdAddr string, requiredMinVersion, requiredMaxVersion semver.Version) error {
	version, err := pdutil.FetchPDVersion(ctx, tls, pdAddr)
	if err != nil {
		return errors.Trace(err)
	}

	return checkVersion("PD", *version, requiredMinVersion, requiredMaxVersion)
}

func checkTiKVVersion(ctx context.Context, tls *common.TLS, pdAddr string, requiredMinVersion, requiredMaxVersion semver.Version) error {
	return ForAllStores(
		ctx,
		tls.WithHost(pdAddr),
		StoreStateDown,
		func(c context.Context, store *Store) error {
			component := fmt.Sprintf("TiKV (at %s)", store.Address)
			version, err := semver.NewVersion(strings.TrimPrefix(store.Version, "v"))
			if err != nil {
				return errors.Annotate(err, component)
			}
			return checkVersion(component, *version, requiredMinVersion, requiredMaxVersion)
		},
	)
}

func checkVersion(component string, actual, requiredMinVersion, requiredMaxVersion semver.Version) error {
	// actual version must be within [requiredMinVersion, requiredMaxVersion).
	if actual.Compare(requiredMinVersion) < 0 {
		return errors.Errorf(
			"%s version too old, required to be in [%s, %s), found '%s'",
			component,
			requiredMinVersion,
			requiredMaxVersion,
			actual,
		)
	}
	// Compare the major version number to make sure beta version does not pass
	// the check. This is because beta version may contains incompatible
	// changes.
	if actual.Major >= requiredMaxVersion.Major {
		return errors.Errorf(
			"%s version too new, major version expected to be within [%s, %d.0.0), found '%s'",
			component,
			requiredMinVersion,
			requiredMaxVersion.Major,
			actual,
		)
	}
	return nil
}

func (importer *importer) FetchRemoteTableModels(ctx context.Context, schema string) ([]*model.TableInfo, error) {
	return fetchRemoteTableModelsFromTLS(ctx, importer.tls, schema)
}

func (importer *importer) EngineFileSizes() []EngineFileSize {
	return nil
}

func (importer *importer) FlushEngine(context.Context, uuid.UUID) error {
	return nil
}

func (importer *importer) FlushAllEngines(context.Context) error {
	return nil
}

func (importer *importer) ResetEngine(context.Context, uuid.UUID) error {
	return errors.New("cannot reset an engine in importer backend")
}

func (importer *importer) LocalWriter(ctx context.Context, engineUUID uuid.UUID) (EngineWriter, error) {
	return &ImporterWriter{importer: importer, engineUUID: engineUUID}, nil
}

type ImporterWriter struct {
	importer   *importer
	engineUUID uuid.UUID
}

func (w *ImporterWriter) Close() error {
	return nil
}

func (w *ImporterWriter) AppendRows(ctx context.Context, tableName string, columnNames []string, ts uint64, rows Rows) error {
	return w.importer.WriteRows(ctx, w.engineUUID, tableName, columnNames, ts, rows)
}
