// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package conn

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/codec"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/utils"
)

const (
	dialTimeout          = 30 * time.Second
	clusterVersionPrefix = "pd/api/v1/config/cluster-version"
	regionCountPrefix    = "pd/api/v1/stats/region"
	schdulerPrefix       = "pd/api/v1/schedulers"
	maxMsgSize           = int(128 * utils.MB) // pd.ScanRegion may return a large response
	scheduleConfigPrefix = "pd/api/v1/config/schedule"

	resetRetryTimes = 3
)

// Mgr manages connections to a TiDB cluster.
type Mgr struct {
	PdMgr    *PdController
	tlsConf  *tls.Config
	dom      *domain.Domain
	storage  tikv.Storage
	grpcClis struct {
		mu   sync.Mutex
		clis map[uint64]*grpc.ClientConn
	}
	ownsStorage bool
}

// StoreBehavior is the action to do in GetAllTiKVStores when a non-TiKV
// store (e.g. TiFlash store) is found.
type StoreBehavior uint8

const (
	// ErrorOnTiFlash causes GetAllTiKVStores to return error when the store is
	// found to be a TiFlash node.
	ErrorOnTiFlash StoreBehavior = 0
	// SkipTiFlash causes GetAllTiKVStores to skip the store when it is found to
	// be a TiFlash node.
	SkipTiFlash StoreBehavior = 1
	// TiFlashOnly caused GetAllTiKVStores to skip the store which is not a
	// TiFlash node.
	TiFlashOnly StoreBehavior = 2
)

// GetAllTiKVStores returns all TiKV stores registered to the PD client. The
// stores must not be a tombstone and must never contain a label `engine=tiflash`.
func GetAllTiKVStores(
	ctx context.Context,
	pdClient pd.Client,
	storeBehavior StoreBehavior,
) ([]*metapb.Store, error) {
	// get all live stores.
	stores, err := pdClient.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return nil, err
	}

	// filter out all stores which are TiFlash.
	j := 0
skipStore:
	for _, store := range stores {
		var isTiFlash bool
		for _, label := range store.Labels {
			if label.Key == "engine" && label.Value == "tiflash" {
				if storeBehavior == SkipTiFlash {
					continue skipStore
				} else if storeBehavior == ErrorOnTiFlash {
					return nil, errors.Errorf(
						"cannot restore to a cluster with active TiFlash stores (store %d at %s)", store.Id, store.Address)
				}
				isTiFlash = true
			}
		}
		if !isTiFlash && storeBehavior == TiFlashOnly {
			continue skipStore
		}
		stores[j] = store
		j++
	}
	return stores[:j], nil
}

// NewMgr creates a new Mgr.
func NewMgr(
	ctx context.Context,
	g glue.Glue,
	pdAddrs string,
	storage tikv.Storage,
	tlsConf *tls.Config,
	securityOption pd.SecurityOption,
	storeBehavior StoreBehavior,
	checkRequirements bool,
) (*Mgr, error) {
	addrs := strings.Split(pdAddrs, ",")

	failure := errors.Errorf("pd address (%s) has wrong format", pdAddrs)
	cli := &http.Client{Timeout: 30 * time.Second}
	if tlsConf != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConf
		cli.Transport = transport
	}

	processedAddrs := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if addr != "" && !strings.HasPrefix("http", addr) {
			if tlsConf != nil {
				addr = "https://" + addr
			} else {
				addr = "http://" + addr
			}
		}
		processedAddrs = append(processedAddrs, addr)
		_, failure = pdRequest(ctx, addr, clusterVersionPrefix, cli, http.MethodGet, nil)
		if failure == nil {
			break
		}
	}
	if failure != nil {
		return nil, errors.Annotatef(failure, "pd address (%s) not available, please check network", pdAddrs)
	}

	maxCallMsgSize := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgSize)),
	}
	pdClient, err := pd.NewClientWithContext(
		ctx, addrs, securityOption,
		pd.WithGRPCDialOptions(maxCallMsgSize...),
		pd.WithCustomTimeoutOption(10*time.Second),
	)
	if err != nil {
		log.Error("fail to create pd client", zap.Error(err))
		return nil, err
	}
	if checkRequirements {
		err = utils.CheckClusterVersion(ctx, pdClient)
		if err != nil {
			errMsg := "running BR in incompatible version of cluster, " +
				"if you believe it's OK, use --check-requirements=false to skip."
			return nil, errors.Annotate(err, errMsg)
		}
	}
	log.Info("new mgr", zap.String("pdAddrs", pdAddrs))

	// Check live tikv.
	stores, err := GetAllTiKVStores(ctx, pdClient, storeBehavior)
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

	dom, err := g.GetDomain(storage)
	if err != nil {
		return nil, errors.Trace(err)
	}

	mgr := &Mgr{
		PdMgr: &PdController{
			pdClient: pdClient,
		},
		storage:     storage,
		dom:         dom,
		tlsConf:     tlsConf,
		ownsStorage: g.OwnsStorage(),
	}
	mgr.PdMgr.addrs = processedAddrs
	mgr.PdMgr.cli = cli
	mgr.grpcClis.clis = make(map[uint64]*grpc.ClientConn)
	return mgr, nil
}

// SetPDHTTP set pd addrs and cli for test.
func (mgr *Mgr) SetPDHTTP(addrs []string, cli *http.Client) {
	mgr.PdMgr.addrs = addrs
	mgr.PdMgr.cli = cli
}

// SetPDClient set pd addrs and cli for test.
func (mgr *Mgr) SetPDClient(pdClient pd.Client) {
	mgr.PdMgr.pdClient = pdClient
}

// GetClusterVersion returns the current cluster version.
func (mgr *Mgr) GetClusterVersion(ctx context.Context) (string, error) {
	return mgr.getClusterVersionWith(ctx, pdRequest)
}

func (mgr *Mgr) getClusterVersionWith(ctx context.Context, get pdHTTPRequest) (string, error) {
	var err error
	for _, addr := range mgr.PdMgr.addrs {
		v, e := get(ctx, addr, clusterVersionPrefix, mgr.PdMgr.cli, http.MethodGet, nil)
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
	return mgr.getRegionCountWith(ctx, pdRequest, startKey, endKey)
}

func (mgr *Mgr) getRegionCountWith(
	ctx context.Context, get pdHTTPRequest, startKey, endKey []byte,
) (int, error) {
	// TiKV reports region start/end keys to PD in memcomparable-format.
	var start, end string
	start = url.QueryEscape(string(codec.EncodeBytes(nil, startKey)))
	if len(endKey) != 0 { // Empty end key means the max.
		end = url.QueryEscape(string(codec.EncodeBytes(nil, endKey)))
	}
	var err error
	for _, addr := range mgr.PdMgr.addrs {
		query := fmt.Sprintf(
			"%s?start_key=%s&end_key=%s",
			regionCountPrefix, start, end)
		v, e := get(ctx, addr, query, mgr.PdMgr.cli, http.MethodGet, nil)
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
	store, err := mgr.PdMgr.pdClient.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithInsecure()
	if mgr.tlsConf != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(mgr.tlsConf))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	keepAlive := 10
	keepAliveTimeout := 3
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = time.Second * 3
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	conn, err := grpc.DialContext(
		ctx,
		addr,
		opt,
		grpc.WithBlock(),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Duration(keepAlive) * time.Second,
			Timeout: time.Duration(keepAliveTimeout) * time.Second,
		}),
	)
	cancel()
	if err != nil {
		return nil, errors.WithStack(err)
	}
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
	// Cache the conn.
	mgr.grpcClis.clis[storeID] = conn
	return backup.NewBackupClient(conn), nil
}

// ResetBackupClient reset the connection for backup client.
func (mgr *Mgr) ResetBackupClient(ctx context.Context, storeID uint64) (backup.BackupClient, error) {
	mgr.grpcClis.mu.Lock()
	defer mgr.grpcClis.mu.Unlock()

	if conn, ok := mgr.grpcClis.clis[storeID]; ok {
		// Find a cached backup client.
		log.Info("Reset backup client", zap.Uint64("storeID", storeID))
		err := conn.Close()
		if err != nil {
			log.Warn("close backup connection failed, ignore it", zap.Uint64("storeID", storeID))
		}
		delete(mgr.grpcClis.clis, storeID)
	}
	var (
		conn *grpc.ClientConn
		err  error
	)
	for retry := 0; retry < resetRetryTimes; retry++ {
		conn, err = mgr.getGrpcConnLocked(ctx, storeID)
		if err != nil {
			log.Warn("failed to reset grpc connection, retry it",
				zap.Int("retry time", retry), zap.Error(err))
			time.Sleep(time.Duration(retry+3) * time.Second)
			continue
		}
		mgr.grpcClis.clis[storeID] = conn
		break
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return backup.NewBackupClient(conn), nil
}

// GetPDClient returns a pd client.
func (mgr *Mgr) GetPDClient() pd.Client {
	return mgr.PdMgr.pdClient
}

// GetTiKV returns a tikv storage.
func (mgr *Mgr) GetTiKV() tikv.Storage {
	return mgr.storage
}

// GetTLSConfig returns the tls config.
func (mgr *Mgr) GetTLSConfig() *tls.Config {
	return mgr.tlsConf
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
	if mgr.ownsStorage {
		if mgr.dom != nil {
			mgr.dom.Close()
		}

		atomic.StoreUint32(&tikv.ShuttingDown, 1)
		mgr.storage.Close()
	}

	mgr.PdMgr.Close()
}
