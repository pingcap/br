// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gluetikv

import (
	"context"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
<<<<<<< HEAD
	"github.com/pingcap/tidb/store/tikv"
=======
	"github.com/pingcap/tidb/store/driver"
>>>>>>> fa0f417... tests: replace br with run_br in integration tests (#763)
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
<<<<<<< HEAD
	return tikv.Driver{}.Open(path)
=======
	return driver.TiKVDriver{}.Open(path)
>>>>>>> fa0f417... tests: replace br with run_br in integration tests (#763)
}

// OwnsStorage implements glue.Glue.
func (Glue) OwnsStorage() bool {
	return true
}

// StartProgress implements glue.Glue.
func (Glue) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	return utils.StartProgress(ctx, cmdName, total, redirectLog, nil)
}

// Record implements glue.Glue.
func (Glue) Record(name string, val uint64) {
	summary.CollectUint(name, val)
}
