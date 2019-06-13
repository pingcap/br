package meta

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/kvproto/pkg/backup"
)

// Backer backups a TiDB/TiKV cluster.
type Backer struct {
	ctx      context.Context
	pdClient pd.Client
	pdHTTP   struct {
		addrs []string
		cli   *http.Client
	}
	kvClients  map[uint64]backup.BackupClient
}

// NewBacker creates a new Backer.
func NewBacker(ctx context.Context, pdAddrs string) (*Backer, error) {
	addrs := strings.Split(pdAddrs, ",")
	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Backer{
		ctx:      ctx,
		pdClient: pdClient,
		pdHTTP: struct {
			addrs []string
			cli   *http.Client
		}{
			addrs: addrs,
			cli:   &http.Client{Timeout: 30 * time.Second},
		},
	}, nil
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

func (backer *Backer) Context() context.Context {
	return backer.ctx
}

const physicalShiftBits = 18

func DecodeTs(ts uint64) Timestamp {
	physical := oracle.ExtractPhysical(ts)
	logical := ts - (uint64(physical) << physicalShiftBits)
	return Timestamp{
		Physical: physical,
		Logical:  int64(logical),
	}
}

func EncodeTs(tp Timestamp) uint64 {
	return uint64((tp.Physical << physicalShiftBits) + tp.Logical)
}
