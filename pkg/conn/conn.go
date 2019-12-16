package conn

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/tikv"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

const (
	dialTimeout          = 5 * time.Second
	clusterVersionPrefix = "pd/api/v1/config/cluster-version"
	regionCountPrefix    = "pd/api/v1/stats/region"
)

// Mgr manages connections to a TiDB cluster.
type Mgr struct {
	pdClient pd.Client
	pdHTTP   struct {
		addrs []string
		cli   *http.Client
	}
	dom      *domain.Domain
	storage  tikv.Storage
	grpcClis struct {
		mu   sync.Mutex
		clis map[uint64]*grpc.ClientConn
	}
}

type pdHTTPGet func(context.Context, string, string, *http.Client) ([]byte, error)

func pdGet(ctx context.Context, addr string, prefix string, cli *http.Client) ([]byte, error) {
	if addr != "" && !strings.HasPrefix("http", addr) {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	url := fmt.Sprintf("%s/%s", u, prefix)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		res, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.Errorf("[%d] %s %s", resp.StatusCode, res, url)
	}

	r, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return r, nil
}

// NewMgr creates a new Mgr.
func NewMgr(ctx context.Context, pdAddrs string, storage tikv.Storage) (*Mgr, error) {
	addrs := strings.Split(pdAddrs, ",")

	failure := errors.Errorf("pd address (%s) has wrong format", pdAddrs)
	cli := &http.Client{Timeout: 30 * time.Second}
	for _, addr := range addrs {
		_, failure = pdGet(ctx, addr, clusterVersionPrefix, cli)
		// TODO need check cluster version >= 3.1 when br release
		if failure == nil {
			break
		}
	}
	if failure != nil {
		return nil, errors.Annotatef(failure, "pd address (%s) not available, please check network", pdAddrs)
	}

	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	if err != nil {
		log.Error("fail to create pd client", zap.Error(err))
		return nil, err
	}
	log.Info("new mgr", zap.String("pdAddrs", pdAddrs))

	// Check live tikv.
	stores, err := pdClient.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		log.Error("fail to get store", zap.Error(err))
		return nil, err
	}
	liveStoreCount := 0
	for _, s := range stores {
		if s.GetState() != metapb.StoreState_Up {
			continue
		}
		liveStoreCount++
	}
	if liveStoreCount == 0 &&
		// Assume 3 replicas
		len(stores) >= 3 && len(stores) > liveStoreCount+1 {
		log.Error("tikv cluster not health", zap.Reflect("stores", stores))
		return nil, errors.Errorf("tikv cluster not health %+v", stores)
	}

	dom, err := session.BootstrapSession(storage)
	if err != nil {
		return nil, errors.Trace(err)
	}

	mgr := &Mgr{
		pdClient: pdClient,
		storage:  storage,
		dom:      dom,
	}
	mgr.pdHTTP.addrs = addrs
	mgr.pdHTTP.cli = cli
	mgr.grpcClis.clis = make(map[uint64]*grpc.ClientConn)
	return mgr, nil
}

// SetPDHTTP set pd addrs and cli for test
func (mgr *Mgr) SetPDHTTP(addrs []string, cli *http.Client) {
	mgr.pdHTTP.addrs = addrs
	mgr.pdHTTP.cli = cli
}

// SetPDClient set pd addrs and cli for test
func (mgr *Mgr) SetPDClient(pdClient pd.Client) {
	mgr.pdClient = pdClient
}

// GetClusterVersion returns the current cluster version.
func (mgr *Mgr) GetClusterVersion(ctx context.Context) (string, error) {
	return mgr.getClusterVersionWith(ctx, pdGet)
}

func (mgr *Mgr) getClusterVersionWith(ctx context.Context, get pdHTTPGet) (string, error) {
	var err error
	for _, addr := range mgr.pdHTTP.addrs {
		v, e := get(ctx, addr, clusterVersionPrefix, mgr.pdHTTP.cli)
		if e != nil {
			err = e
			continue
		}
		return string(v), nil
	}

	return "", err
}

// GetRegionCount returns the region count in the specified range.
func (mgr *Mgr) GetRegionCount(ctx context.Context, startKey, endKey []byte) (int, error) {
	return mgr.getRegionCountWith(ctx, pdGet, startKey, endKey)
}

func (mgr *Mgr) getRegionCountWith(
	ctx context.Context, get pdHTTPGet, startKey, endKey []byte,
) (int, error) {
	var err error
	for _, addr := range mgr.pdHTTP.addrs {
		query := fmt.Sprintf(
			"%s?start_key=%s&end_key=%s",
			regionCountPrefix,
			url.QueryEscape(string(startKey)), url.QueryEscape(string(endKey)))
		v, e := get(ctx, addr, query, mgr.pdHTTP.cli)
		if e != nil {
			err = e
			continue
		}
		regionsMap := make(map[string]interface{})
		err = json.Unmarshal(v, &regionsMap)
		if err != nil {
			return 0, err
		}
		return int(regionsMap["count"].(float64)), nil
	}
	return 0, err
}

func (mgr *Mgr) getGrpcConnLocked(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	store, err := mgr.pdClient.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithInsecure()
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	keepAlive := 10
	keepAliveTimeout := 3
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = time.Second * 3
	conn, err := grpc.DialContext(
		ctx,
		store.GetAddress(),
		opt,
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Duration(keepAlive) * time.Second,
			Timeout:             time.Duration(keepAliveTimeout) * time.Second,
			PermitWithoutStream: true,
		}),
	)
	cancel()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Cache the conn.
	mgr.grpcClis.clis[storeID] = conn
	return conn, nil
}

// GetBackupClient get or create a backup client.
func (mgr *Mgr) GetBackupClient(ctx context.Context, storeID uint64) (backup.BackupClient, error) {
	mgr.grpcClis.mu.Lock()
	defer mgr.grpcClis.mu.Unlock()

	if conn, ok := mgr.grpcClis.clis[storeID]; ok {
		// Find a cached backup client.
		return backup.NewBackupClient(conn), nil
	}

	conn, err := mgr.getGrpcConnLocked(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return backup.NewBackupClient(conn), nil
}

// GetPDClient returns a pd client.
func (mgr *Mgr) GetPDClient() pd.Client {
	return mgr.pdClient
}

// GetTiKV returns a tikv storage.
func (mgr *Mgr) GetTiKV() tikv.Storage {
	return mgr.storage
}

// GetLockResolver gets the LockResolver.
func (mgr *Mgr) GetLockResolver() *tikv.LockResolver {
	return mgr.storage.GetLockResolver()
}

// GetDomain returns a tikv storage.
func (mgr *Mgr) GetDomain() *domain.Domain {
	return mgr.dom
}

// Close closes all client in Mgr.
func (mgr *Mgr) Close() {
	mgr.grpcClis.mu.Lock()
	for _, cli := range mgr.grpcClis.clis {
		err := cli.Close()
		if err != nil {
			log.Error("fail to close Mgr", zap.Error(err))
		}
	}
	mgr.grpcClis.mu.Unlock()

	// Gracefully shutdown domain so it does not affect other TiDB DDL.
	// Must close domain before closing storage, otherwise it gets stuck forever.
	mgr.dom.Close()

	atomic.StoreUint32(&tikv.ShuttingDown, 1)
	mgr.storage.Close()
	mgr.pdClient.Close()
}
