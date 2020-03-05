package gluetikv

import (
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"

	"github.com/pingcap/br/pkg/glue"
)

// Glue is an implementation of glue.Glue that accesses only TiKV without TiDB.
type Glue struct{}

// BootstrapSession implements glue.Glue
func (Glue) BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	return nil, nil
}

// CreateSession implements glue.Glue
func (Glue) CreateSession(store kv.Storage) (glue.Session, error) {
	return nil, nil
}

// Open implements glue.Glue
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
