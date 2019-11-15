package utils

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"go.uber.org/zap"
)

var pprofOnce sync.Once

// MockCluster is mock tidb cluster, includes tikv and pd.
type MockCluster struct {
	*server.Server
	*mocktikv.Cluster
	mocktikv.MVCCStore
	kv.Storage
	*server.TiDBDriver
	*domain.Domain
}

// NewMockCluster create a new mock cluster.
func NewMockCluster() (*MockCluster, error) {
	pprofOnce.Do(func() {
		go func() {
			// Make sure pprof is registered.
			_ = pprof.Handler
			addr := "0.0.0.0:12235"
			log.Info("start pprof", zap.String("addr", addr))
			if e := http.ListenAndServe(addr, nil); e != nil {
				log.Warn("fail to start pprof", zap.String("addr", addr), zap.Error(e))
			}
		}()
	})

	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	mvccStore := mocktikv.MustNewMVCCStore()
	storage, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
		mockstore.WithMVCCStore(mvccStore),
	)
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	if err != nil {
		return nil, err
	}
	dom, err := session.BootstrapSession(storage)
	if err != nil {
		return nil, err
	}
	return &MockCluster{
		Cluster:   cluster,
		MVCCStore: mvccStore,
		Storage:   storage,
		Domain:    dom,
	}, nil
}

// Start runs a mock cluster
func (mock *MockCluster) Start() error {
	mock.TiDBDriver = server.NewTiDBDriver(mock.Storage)
	cfg := config.NewConfig()
	cfg.Port = 4001
	cfg.Store = "tikv"
	cfg.Status.StatusPort = 10090
	cfg.Status.ReportStatus = true

	svr, err := server.NewServer(cfg, mock.TiDBDriver)
	if err != nil {
		return err
	}
	mock.Server = svr
	go func() {
		if err1 := svr.Run(); err != nil {
			panic(err1)
		}
	}()
	waitUntilServerOnline(cfg.Status.StatusPort)
	return nil
}

// Stop stops a mock cluster
func (mock *MockCluster) Stop() {
	if mock.Domain != nil {
		mock.Domain.Close()
	}
	if mock.Storage != nil {
		mock.Storage.Close()
	}
	if mock.Server != nil {
		mock.Server.Close()
	}
}

type configOverrider func(*mysql.Config)

const retryTime = 100

var defaultDSNConfig = mysql.Config{
	User: "root",
	Net:  "tcp",
	Addr: "127.0.0.1:4001",
}

// getDSN generates a DSN string for MySQL connection.
func getDSN(overriders ...configOverrider) string {
	var cfg = defaultDSNConfig
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(&cfg)
		}
	}
	return cfg.FormatDSN()
}

func waitUntilServerOnline(statusPort uint) {
	// connect server
	retry := 0
	for ; retry < retryTime; retry++ {
		time.Sleep(time.Millisecond * 10)
		db, err := sql.Open("mysql", getDSN())
		if err == nil {
			db.Close()
			break
		}
	}
	if retry == retryTime {
		log.Fatal("failed to connect DB in every 10 ms", zap.Int("retryTime", retryTime))
	}
	// connect http status
	statusURL := fmt.Sprintf("http://127.0.0.1:%d/status", statusPort)
	for retry = 0; retry < retryTime; retry++ {
		resp, err := http.Get(statusURL)
		if err == nil {
			// Ignore errors.
			_, _ = ioutil.ReadAll(resp.Body)
			_ = resp.Body.Close()
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		log.Fatal("failed to connect HTTP status in every 10 ms", zap.Int("retryTime", retryTime))
	}
}
