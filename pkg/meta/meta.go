package meta

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	dialTimeout = 5 * time.Second
)

// Backer backups a TiDB/TiKV cluster.
type Backer struct {
	ctx      context.Context
	pdClient pd.Client
	pdHTTP   struct {
		addrs []string
		cli   *http.Client
	}
	tikvCli    tikv.Storage
	backupClis struct {
		mu   sync.Mutex
		clis map[uint64]*grpc.ClientConn
	}
}

// NewBacker creates a new Backer.
func NewBacker(ctx context.Context, pdAddrs string) (*Backer, error) {
	addrs := strings.Split(pdAddrs, ",")
	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("new backer", zap.String("pdAddrs", pdAddrs))
	tikvCli, err := tikv.Driver{}.Open(
		// Disable GC because TiDB enables GC already.
		fmt.Sprintf("tikv://%s?disableGC=true", pdAddrs))
	if err != nil {
		return nil, errors.Trace(err)
	}

	backer := &Backer{
		ctx:      ctx,
		pdClient: pdClient,
		tikvCli:  tikvCli.(tikv.Storage),
	}
	backer.pdHTTP.addrs = addrs
	backer.pdHTTP.cli = &http.Client{Timeout: 30 * time.Second}
	backer.backupClis.clis = make(map[uint64]*grpc.ClientConn)
	return backer, nil
}

// GetClusterVersion returns the current cluster version.
func (backer *Backer) GetClusterVersion() (string, error) {
	var clusterVersionPrefix = "pd/api/v1/config/cluster-version"

	get := func(addr string) (string, error) {
		if addr != "" && !strings.HasPrefix("http", addr) {
			addr = "http://" + addr
		}
		u, err := url.Parse(addr)
		if err != nil {
			return "", errors.Trace(err)
		}
		url := fmt.Sprintf("%s/%s", u, clusterVersionPrefix)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return "", errors.Trace(err)
		}
		resp, err := backer.pdHTTP.cli.Do(req)
		if err != nil {
			return "", errors.Trace(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			res, _ := ioutil.ReadAll(resp.Body)
			return "", errors.Errorf("[%d] %s", resp.StatusCode, res)
		}

		r, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(r), nil
	}

	var err error
	for _, addr := range backer.pdHTTP.addrs {
		var v string
		var e error
		if v, e = get(addr); e != nil {
			err = errors.Trace(err)
			continue
		}
		return v, nil
	}

	return "", err
}

// GetGCSaftPoint returns the current gc safe point.
// TODO: Some cluster may not enable distributed GC.
func (backer *Backer) GetGCSaftPoint() (Timestamp, error) {
	safePoint, err := backer.pdClient.UpdateGCSafePoint(backer.ctx, 0)
	println(safePoint)
	if err != nil {
		return Timestamp{}, errors.Trace(err)
	}
	return DecodeTs(safePoint), nil
}

// Context returns Backer's context.
func (backer *Backer) Context() context.Context {
	return backer.ctx
}

// GetBackupClient get or create a backup client.
func (backer *Backer) GetBackupClient(storeID uint64) (backup.BackupClient, error) {
	backer.backupClis.mu.Lock()
	defer backer.backupClis.mu.Unlock()

	if conn, ok := backer.backupClis.clis[storeID]; ok {
		// Find a cached backup client.
		return backup.NewBackupClient(conn), nil
	}

	store, err := backer.pdClient.GetStore(backer.ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithInsecure()
	ctx, cancel := context.WithTimeout(backer.ctx, dialTimeout)
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
	backer.backupClis.clis[storeID] = conn

	client := backup.NewBackupClient(conn)
	return client, nil
}

// ResetBackupClient reset and close cached backup client.
func (backer *Backer) ResetBackupClient(storeID uint64) error {
	backer.backupClis.mu.Lock()
	defer backer.backupClis.mu.Unlock()

	if conn, ok := backer.backupClis.clis[storeID]; ok {
		delete(backer.backupClis.clis, storeID)
		if err := conn.Close(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// SendBackup send backup request to the given store.
// Stop receiving response if respFn returns error.
func (backer *Backer) SendBackup(
	ctx context.Context,
	storeID uint64,
	req backup.BackupRequest,
	respFn func(*backup.BackupResponse) error,
) error {
	log.Info("try backup", zap.Any("backup request", req))
	client, err := backer.GetBackupClient(storeID)
	if err != nil {
		log.Warn("fail to connect store", zap.Uint64("StoreID", storeID))
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	bcli, err := client.Backup(ctx, &req)
	if err != nil {
		log.Warn("fail to create backup", zap.Uint64("StoreID", storeID))
		if err1 := backer.ResetBackupClient(storeID); err1 != nil {
			log.Warn("fail to reset backup client",
				zap.Uint64("StoreID", storeID),
				zap.Error(err1))
		}
		return nil
	}
	for {
		resp, err := bcli.Recv()
		if err != nil {
			if err == io.EOF {
				log.Info("backup streaming finish",
					zap.Uint64("StoreID", storeID))
				break
			}
			return errors.Trace(err)
		}
		// TODO: handle errors in the resp.
		log.Info("range backuped",
			zap.Any("StartKey", resp.GetStartKey()),
			zap.Any("EndKey", resp.GetEndKey()))
		err = respFn(resp)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetPDClient returns a pd client.
func (backer *Backer) GetPDClient() pd.Client {
	return backer.pdClient
}

// GetTiKV returns a tikv storage.
func (backer *Backer) GetTiKV() tikv.Storage {
	return backer.tikvCli
}

// GetLockResolver gets the LockResolver.
func (backer *Backer) GetLockResolver() *tikv.LockResolver {
	return backer.tikvCli.GetLockResolver()
}

// Timestamp is composed by a physical unix timestamp and a logical timestamp.
type Timestamp struct {
	Physical int64
	Logical  int64
}

const physicalShiftBits = 18

// DecodeTs decodes Timestamp from a uint64
func DecodeTs(ts uint64) Timestamp {
	physical := oracle.ExtractPhysical(ts)
	logical := ts - (uint64(physical) << physicalShiftBits)
	return Timestamp{
		Physical: physical,
		Logical:  int64(logical),
	}
}

// EncodeTs encodes Timestamp into a uint64
func EncodeTs(tp Timestamp) uint64 {
	return uint64((tp.Physical << physicalShiftBits) + tp.Logical)
}
