// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gluetikv

import (
	"context"

	"sync/atomic"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/prometheus/common/log"
	pd "github.com/tikv/pd/client"

	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

// Glue is an implementation of glue.Glue that accesses only TiKV without TiDB.
type Glue struct{}

// GetDomain implements glue.Glue.
func (Glue) GetDomain(store kv.Storage) (*domain.Domain, error) {
	return nil, nil
}

// CreateSession implements glue.Glue.
func (Glue) CreateSession(store kv.Storage) (glue.Session, error) {
	return nil, nil
}

// Open implements glue.Glue.
func (Glue) Open(path string, option pd.SecurityOption) (kv.Storage, error) {
	if option.CAPath != "" {
		conf := config.GetGlobalConfig()
		conf.Security.ClusterSSLCA = option.CAPath
		conf.Security.ClusterSSLCert = option.CertPath
		conf.Security.ClusterSSLKey = option.KeyPath
		config.StoreGlobalConfig(conf)
	}
	return tikv.Driver{}.Open(path)
}

// OwnsStorage implements glue.Glue.
func (Glue) OwnsStorage() bool {
	return true
}

// StartProgress implements glue.Glue.
func (Glue) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	return progress{ch: utils.StartProgress(ctx, cmdName, total, redirectLog), closed: new(atomic.Bool)}
}

// Record implements glue.Glue.
func (Glue) Record(name string, val uint64) {
	summary.CollectUint(name, val)
}

type progress struct {
	ch     chan<- struct{}
	closed int32
}

// Inc implements glue.Progress.
func (p progress) Inc() {
	if atomic.LoadInt32(&p.closed) != 0 {
		log.Warn("proposing a closed progress")
		return
	}
	// there might be buggy if the thread is yielded here.
	// however, there should not be gosched, at most time.
	// so send here probably is safe, even not totally safe.
	// since adding an extra lock should be costly, we just be optimistic.
	// (Maybe a spin lock here would be better?)
	p.ch <- struct{}{}
}

// Close implements glue.Progress.
func (p progress) Close() {
	// set closed to true firstly,
	// so we won't see a state that the channel is closed and the p.closed is false.
	atomic.StoreInt32(&p.closed, 1)
	close(p.ch)
}
