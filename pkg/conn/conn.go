// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package conn

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
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
	pd "github.com/pingcap/pd/v3/client"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/utils"
)

const (
	dialTimeout          = 5 * time.Second
	clusterVersionPrefix = "pd/api/v1/config/cluster-version"
	regionCountPrefix    = "pd/api/v1/stats/region"
	schdulerPrefix       = "pd/api/v1/schedulers"
	maxMsgSize           = int(128 * utils.MB) // pd.ScanRegion may return a large response
	scheduleConfigPrefix = "pd/api/v1/config/schedule"
)

// Mgr manages connections to a TiDB cluster.
type Mgr struct {
	pdClient pd.Client
	pdHTTP   struct {
		addrs []string
		cli   *http.Client
	}
	tlsConf  *tls.Config
	dom      *domain.Domain
	storage  tikv.Storage
	grpcClis struct {
		mu   sync.Mutex
		clis map[uint64]*grpc.ClientConn
	}
	ownsStorage bool
}

type pdHTTPRequest func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error)

func pdRequest(
	ctx context.Context,
	addr string, prefix string,
	cli *http.Client, method string, body io.Reader) ([]byte, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	url := fmt.Sprintf("%s/%s", u, prefix)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
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
	pdClient, err := pd.NewClient(
		addrs, securityOption, pd.WithGRPCDialOptions(maxCallMsgSize...))
	if err != nil {
		log.Error("fail to create pd client", zap.Error(err))
		return nil, err
	}
	if checkRequirements {
		err = utils.CheckClusterVersion(ctx, pdClient)
		if err != nil {
			errMsg := "running BR in incompatible version of cluster, " +
				"error: (%s). " +
				"if you believe it's OK, use --check-requirements=false to skip."
			return nil, errors.Errorf(fmt.Sprintf(errMsg, err.Error()))
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
		pdClient:    pdClient,
		storage:     storage,
		dom:         dom,
		tlsConf:     tlsConf,
		ownsStorage: g.OwnsStorage(),
	}
	mgr.pdHTTP.addrs = processedAddrs
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
	return mgr.getClusterVersionWith(ctx, pdRequest)
}

func (mgr *Mgr) getClusterVersionWith(ctx context.Context, get pdHTTPRequest) (string, error) {
	var err error
	for _, addr := range mgr.pdHTTP.addrs {
		v, e := get(ctx, addr, clusterVersionPrefix, mgr.pdHTTP.cli, http.MethodGet, nil)
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
	for _, addr := range mgr.pdHTTP.addrs {
		query := fmt.Sprintf(
			"%s?start_key=%s&end_key=%s",
			regionCountPrefix, start, end)
		v, e := get(ctx, addr, query, mgr.pdHTTP.cli, http.MethodGet, nil)
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
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  time.Second,     // Default was 1s.
				Multiplier: 1.6,             // Default
				Jitter:     0.2,             // Default
				MaxDelay:   3 * time.Second, // Default was 120s.
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Duration(keepAlive) * time.Second,
			Timeout: time.Duration(keepAliveTimeout) * time.Second,
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

// GetTLSConfig returns the tls config
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

// RemoveScheduler remove pd scheduler
func (mgr *Mgr) RemoveScheduler(ctx context.Context, scheduler string) error {
	return mgr.removeSchedulerWith(ctx, scheduler, pdRequest)
}

func (mgr *Mgr) removeSchedulerWith(ctx context.Context, scheduler string, delete pdHTTPRequest) (err error) {
	for _, addr := range mgr.pdHTTP.addrs {
		prefix := fmt.Sprintf("%s/%s", schdulerPrefix, scheduler)
		_, err = delete(ctx, addr, prefix, mgr.pdHTTP.cli, http.MethodDelete, nil)
		if err != nil {
			continue
		}
		return nil
	}
	return err
}

// AddScheduler add pd scheduler
func (mgr *Mgr) AddScheduler(ctx context.Context, scheduler string) error {
	return mgr.addSchedulerWith(ctx, scheduler, pdRequest)
}

func (mgr *Mgr) addSchedulerWith(ctx context.Context, scheduler string, post pdHTTPRequest) (err error) {
	for _, addr := range mgr.pdHTTP.addrs {
		body := bytes.NewBuffer([]byte(`{"name":"` + scheduler + `"}`))
		_, err = post(ctx, addr, schdulerPrefix, mgr.pdHTTP.cli, http.MethodPost, body)
		if err != nil {
			continue
		}
		return nil
	}
	return err
}

// ListSchedulers list all pd scheduler
func (mgr *Mgr) ListSchedulers(ctx context.Context) ([]string, error) {
	return mgr.listSchedulersWith(ctx, pdRequest)
}

func (mgr *Mgr) listSchedulersWith(ctx context.Context, get pdHTTPRequest) ([]string, error) {
	var err error
	for _, addr := range mgr.pdHTTP.addrs {
		v, e := get(ctx, addr, schdulerPrefix, mgr.pdHTTP.cli, http.MethodGet, nil)
		if e != nil {
			err = e
			continue
		}
		d := make([]string, 0)
		err = json.Unmarshal(v, &d)
		if err != nil {
			return nil, err
		}
		return d, nil
	}
	return nil, err
}

// GetPDScheduleConfig returns PD schedule config value associated with the key.
// It returns nil if there is no such config item.
func (mgr *Mgr) GetPDScheduleConfig(
	ctx context.Context,
) (map[string]interface{}, error) {
	var err error
	for _, addr := range mgr.pdHTTP.addrs {
		v, e := pdRequest(
			ctx, addr, scheduleConfigPrefix, mgr.pdHTTP.cli, http.MethodGet, nil)
		if e != nil {
			err = e
			continue
		}
		cfg := make(map[string]interface{})
		err = json.Unmarshal(v, &cfg)
		if err != nil {
			return nil, err
		}
		return cfg, nil
	}
	return nil, err
}

// UpdatePDScheduleConfig updates PD schedule config value associated with the key.
func (mgr *Mgr) UpdatePDScheduleConfig(
	ctx context.Context, cfg map[string]interface{},
) error {
	for _, addr := range mgr.pdHTTP.addrs {
		reqData, err := json.Marshal(cfg)
		if err != nil {
			return err
		}
		_, e := pdRequest(ctx, addr, scheduleConfigPrefix,
			mgr.pdHTTP.cli, http.MethodPost, bytes.NewBuffer(reqData))
		if e == nil {
			return nil
		}
	}
	return errors.New("update PD schedule config failed")
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

	mgr.pdClient.Close()
}
