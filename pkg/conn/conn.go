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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/store/tikv"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	dialTimeout          = 5 * time.Second
	clusterVersionPrefix = "pd/api/v1/config/cluster-version"
	regionCountPrefix    = "pd/api/v1/regions/count"
)

// Mgr manages connections to a TiDB cluster.
type Mgr struct {
	PDClient pd.Client

	// make it public for unit test to mock
	PDHTTPGet func(string, string, *http.Client) ([]byte, error)

	pdHTTP struct {
		addrs []string
		cli   *http.Client
	}
	tikvCli  tikv.Storage
	grpcClis struct {
		mu   sync.Mutex
		clis map[uint64]*grpc.ClientConn
	}
}

var pdGet = func(addr string, prefix string, cli *http.Client) ([]byte, error) {
	if addr != "" && !strings.HasPrefix("http", addr) {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	url := fmt.Sprintf("%s/%s", u, prefix)
	req, err := http.NewRequest(http.MethodGet, url, nil)
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
func NewMgr(ctx context.Context, pdAddrs string) (*Mgr, error) {
	addrs := strings.Split(pdAddrs, ",")

	failure := errors.Errorf("pd address (%s) has wrong format", pdAddrs)
	cli := &http.Client{Timeout: 30 * time.Second}
	for _, addr := range addrs {
		_, failure = pdGet(addr, clusterVersionPrefix, cli)
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
		return nil, errors.Trace(err)
	}
	log.Info("new mgr", zap.String("pdAddrs", pdAddrs))
	tikvCli, err := tikv.Driver{}.Open(
		// Disable GC because TiDB enables GC already.
		fmt.Sprintf("tikv://%s?disableGC=true", pdAddrs))
	if err != nil {
		return nil, errors.Trace(err)
	}

	mgr := &Mgr{
		PDClient: pdClient,
		tikvCli:  tikvCli.(tikv.Storage),
	}
	mgr.pdHTTP.addrs = addrs
	mgr.pdHTTP.cli = cli
	mgr.grpcClis.clis = make(map[uint64]*grpc.ClientConn)
	mgr.PDHTTPGet = pdGet
	return mgr, nil
}

// SetPDHTTP set pd addrs and cli for test
func (mgr *Mgr) SetPDHTTP(addrs []string, cli *http.Client) {
	mgr.pdHTTP.addrs = addrs
	mgr.pdHTTP.cli = cli
}

// GetClusterVersion returns the current cluster version.
func (mgr *Mgr) GetClusterVersion() (string, error) {
	var err error
	for _, addr := range mgr.pdHTTP.addrs {
		v, e := mgr.PDHTTPGet(addr, clusterVersionPrefix, mgr.pdHTTP.cli)
		if e != nil {
			err = e
			continue
		}
		return string(v), nil
	}

	return "", err
}

// GetRegionCount returns the total region count in the cluster
func (mgr *Mgr) GetRegionCount() (int, error) {
	var err error
	for _, addr := range mgr.pdHTTP.addrs {
		v, e := mgr.PDHTTPGet(addr, regionCountPrefix, mgr.pdHTTP.cli)
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
	store, err := mgr.PDClient.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithInsecure()
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	keepAlive := 10
	keepAliveTimeout := 3
	conn, err := grpc.DialContext(
		ctx,
		store.GetAddress(),
		opt,
		grpc.WithBackoffMaxDelay(time.Second*3),
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

// GetTikvClient get or create a coprocessor client.
func (mgr *Mgr) GetTikvClient(ctx context.Context, storeID uint64) (tikvpb.TikvClient, error) {
	mgr.grpcClis.mu.Lock()
	defer mgr.grpcClis.mu.Unlock()

	if conn, ok := mgr.grpcClis.clis[storeID]; ok {
		// Find a cached backup client.
		return tikvpb.NewTikvClient(conn), nil
	}

	conn, err := mgr.getGrpcConnLocked(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tikvpb.NewTikvClient(conn), nil
}

// GetPDClient returns a pd client.
func (mgr *Mgr) GetPDClient() pd.Client {
	return mgr.PDClient
}

// GetTiKV returns a tikv storage.
func (mgr *Mgr) GetTiKV() tikv.Storage {
	return mgr.tikvCli
}

// GetLockResolver gets the LockResolver.
func (mgr *Mgr) GetLockResolver() *tikv.LockResolver {
	return mgr.tikvCli.GetLockResolver()
}
