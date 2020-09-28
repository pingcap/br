// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package pdutil

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/codec"
	pd "github.com/tikv/pd/client"

	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/utils"
)

const (
	clusterVersionPrefix = "pd/api/v1/config/cluster-version"
	regionCountPrefix    = "pd/api/v1/stats/region"
	schedulerPrefix      = "pd/api/v1/schedulers"
	maxMsgSize           = int(128 * utils.MB) // pd.ScanRegion may return a large response
	scheduleConfigPrefix = "pd/api/v1/config/schedule"
)

// clusterConfig represents a set of scheduler whose config have been modified
// along with their original config.
type clusterConfig struct {
	// Enable PD schedulers before restore
	scheduler []string
	// Original scheudle configuration
	scheduleCfg map[string]interface{}
}

var (
	// Schedulers represent region/leader schedulers which can impact on performance.
	Schedulers = map[string]struct{}{
		"balance-leader-scheduler":     {},
		"balance-hot-region-scheduler": {},
		"balance-region-scheduler":     {},

		"shuffle-leader-scheduler":     {},
		"shuffle-region-scheduler":     {},
		"shuffle-hot-region-scheduler": {},
	}
	pdRegionMergeCfg = []string{
		"max-merge-region-keys",
		"max-merge-region-size",
	}
	pdScheduleLimitCfg = []string{
		"leader-schedule-limit",
		"region-schedule-limit",
		"max-snapshot-count",
	}

	// DefaultPDCfg find by https://github.com/tikv/pd/blob/master/conf/config.toml.
	DefaultPDCfg = map[string]interface{}{
		"max-merge-region-keys": 200000,
		"max-merge-region-size": 20,
		"leader-schedule-limit": 4,
		"region-schedule-limit": 2048,
		"max-snapshot-count":    3,
	}
)

// pdHTTPRequest defines the interface to send a request to pd and return the result in bytes.
type pdHTTPRequest func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error)

// pdRequest is a func to send a HTTP to pd and return the result bytes.
func pdRequest(
	ctx context.Context,
	addr string, prefix string,
	cli *http.Client, method string, body io.Reader) ([]byte, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	reqURL := fmt.Sprintf("%s/%s", u, prefix)
	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
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
		return nil, errors.Annotatef(berrors.ErrPDInvalidResponse, "[%d] %s %s", resp.StatusCode, res, reqURL)
	}

	r, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return r, nil
}

// PdController manage get/update config from pd.
type PdController struct {
	addrs    []string
	cli      *http.Client
	pdClient pd.Client
}

// NewPdController creates a new PdController.
func NewPdController(
	ctx context.Context,
	pdAddrs string,
	tlsConf *tls.Config,
	securityOption pd.SecurityOption,
) (*PdController, error) {
	cli := &http.Client{Timeout: 30 * time.Second}
	if tlsConf != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConf
		cli.Transport = transport
	}

	addrs := strings.Split(pdAddrs, ",")
	processedAddrs := make([]string, 0, len(addrs))
	var failure error
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

	return &PdController{
		addrs:    processedAddrs,
		cli:      cli,
		pdClient: pdClient,
	}, nil
}

// SetHTTP set pd addrs and cli for test.
func (p *PdController) SetHTTP(addrs []string, cli *http.Client) {
	p.addrs = addrs
	p.cli = cli
}

// SetPDClient set pd addrs and cli for test.
func (p *PdController) SetPDClient(pdClient pd.Client) {
	p.pdClient = pdClient
}

// GetPDClient set pd addrs and cli for test.
func (p *PdController) GetPDClient() pd.Client {
	return p.pdClient
}

// GetClusterVersion returns the current cluster version.
func (p *PdController) GetClusterVersion(ctx context.Context) (string, error) {
	return p.getClusterVersionWith(ctx, pdRequest)
}

func (p *PdController) getClusterVersionWith(ctx context.Context, get pdHTTPRequest) (string, error) {
	var err error
	for _, addr := range p.addrs {
		v, e := get(ctx, addr, clusterVersionPrefix, p.cli, http.MethodGet, nil)
		if e != nil {
			err = e
			continue
		}
		return string(v), nil
	}

	return "", err
}

// GetRegionCount returns the region count in the specified range.
func (p *PdController) GetRegionCount(ctx context.Context, startKey, endKey []byte) (int, error) {
	return p.getRegionCountWith(ctx, pdRequest, startKey, endKey)
}

func (p *PdController) getRegionCountWith(
	ctx context.Context, get pdHTTPRequest, startKey, endKey []byte,
) (int, error) {
	// TiKV reports region start/end keys to PD in memcomparable-format.
	var start, end string
	start = url.QueryEscape(string(codec.EncodeBytes(nil, startKey)))
	if len(endKey) != 0 { // Empty end key means the max.
		end = url.QueryEscape(string(codec.EncodeBytes(nil, endKey)))
	}
	var err error
	for _, addr := range p.addrs {
		query := fmt.Sprintf(
			"%s?start_key=%s&end_key=%s",
			regionCountPrefix, start, end)
		v, e := get(ctx, addr, query, p.cli, http.MethodGet, nil)
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

// RemoveScheduler remove pd scheduler.
func (p *PdController) RemoveScheduler(ctx context.Context, scheduler string) error {
	return p.removeSchedulerWith(ctx, scheduler, pdRequest)
}

func (p *PdController) removeSchedulerWith(ctx context.Context, scheduler string, delete pdHTTPRequest) (err error) {
	for _, addr := range p.addrs {
		prefix := fmt.Sprintf("%s/%s", schedulerPrefix, scheduler)
		_, err = delete(ctx, addr, prefix, p.cli, http.MethodDelete, nil)
		if err != nil {
			continue
		}
		return nil
	}
	return err
}

// AddScheduler add pd scheduler.
func (p *PdController) AddScheduler(ctx context.Context, scheduler string) error {
	return p.addSchedulerWith(ctx, scheduler, pdRequest)
}

func (p *PdController) addSchedulerWith(ctx context.Context, scheduler string, post pdHTTPRequest) (err error) {
	for _, addr := range p.addrs {
		body := bytes.NewBuffer([]byte(`{"name":"` + scheduler + `"}`))
		_, err = post(ctx, addr, schedulerPrefix, p.cli, http.MethodPost, body)
		if err != nil {
			continue
		}
		return nil
	}
	return err
}

// ListSchedulers list all pd scheduler.
func (p *PdController) ListSchedulers(ctx context.Context) ([]string, error) {
	return p.listSchedulersWith(ctx, pdRequest)
}

func (p *PdController) listSchedulersWith(ctx context.Context, get pdHTTPRequest) ([]string, error) {
	var err error
	for _, addr := range p.addrs {
		v, e := get(ctx, addr, schedulerPrefix, p.cli, http.MethodGet, nil)
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
func (p *PdController) GetPDScheduleConfig(
	ctx context.Context,
) (map[string]interface{}, error) {
	var err error
	for _, addr := range p.addrs {
		v, e := pdRequest(
			ctx, addr, scheduleConfigPrefix, p.cli, http.MethodGet, nil)
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
func (p *PdController) UpdatePDScheduleConfig(
	ctx context.Context, cfg map[string]interface{},
) error {
	for _, addr := range p.addrs {
		reqData, err := json.Marshal(cfg)
		if err != nil {
			return err
		}
		_, e := pdRequest(ctx, addr, scheduleConfigPrefix,
			p.cli, http.MethodPost, bytes.NewBuffer(reqData))
		if e == nil {
			return nil
		}
	}
	return errors.Annotate(berrors.ErrPDUpdateFailed, "failed to update PD schedule config")
}

func addPDLeaderScheduler(ctx context.Context, pd *PdController, removedSchedulers []string) error {
	for _, scheduler := range removedSchedulers {
		err := pd.AddScheduler(ctx, scheduler)
		if err != nil {
			return err
		}
	}
	return nil
}

func restoreSchedulers(ctx context.Context, pd *PdController, clusterCfg clusterConfig) error {
	if err := addPDLeaderScheduler(ctx, pd, clusterCfg.scheduler); err != nil {
		return errors.Annotate(err, "fail to add PD schedulers")
	}
	mergeCfg := make(map[string]interface{})
	for _, cfgKey := range pdRegionMergeCfg {
		value := clusterCfg.scheduleCfg[cfgKey]
		if value == nil {
			// Ignore non-exist config.
			continue
		}
		mergeCfg[cfgKey] = value
	}
	if err := pd.UpdatePDScheduleConfig(ctx, mergeCfg); err != nil {
		return errors.Annotate(err, "fail to update PD merge config")
	}

	scheduleLimitCfg := make(map[string]interface{})
	for _, cfgKey := range pdScheduleLimitCfg {
		value := clusterCfg.scheduleCfg[cfgKey]
		if value == nil {
			// Ignore non-exist config.
			continue
		}
		scheduleLimitCfg[cfgKey] = value
	}
	if err := pd.UpdatePDScheduleConfig(ctx, scheduleLimitCfg); err != nil {
		return errors.Annotate(err, "fail to update PD schedule config")
	}
	return nil
}

func (p *PdController) makeUndoFunctionByConfig(config clusterConfig) utils.UndoFunc {
	restore := func(ctx context.Context) error {
		return restoreSchedulers(ctx, p, config)
	}
	return restore
}

// RemoveSchedulers removes the schedulers that may slow down BR speed.
func (p *PdController) RemoveSchedulers(ctx context.Context) (undo utils.UndoFunc, err error) {
	undo = utils.Nop

	// Remove default PD scheduler that may affect restore process.
	existSchedulers, err := p.ListSchedulers(ctx)
	if err != nil {
		return
	}
	needRemoveSchedulers := make([]string, 0, len(existSchedulers))
	for _, s := range existSchedulers {
		if _, ok := Schedulers[s]; ok {
			needRemoveSchedulers = append(needRemoveSchedulers, s)
		}
	}
	scheduler, err := removePDLeaderScheduler(ctx, p, needRemoveSchedulers)
	if err != nil {
		return
	}

	undo = p.makeUndoFunctionByConfig(clusterConfig{scheduler: scheduler})

	stores, err := p.pdClient.GetAllStores(ctx)
	if err != nil {
		return
	}
	scheduleCfg, err := p.GetPDScheduleConfig(ctx)
	if err != nil {
		return
	}

	undo = p.makeUndoFunctionByConfig(clusterConfig{scheduler: scheduler, scheduleCfg: scheduleCfg})

	disableMergeCfg := make(map[string]interface{})
	for _, cfgKey := range pdRegionMergeCfg {
		value := scheduleCfg[cfgKey]
		if value == nil {
			// Ignore non-exist config.
			continue
		}
		// Disable region merge by setting config to 0.
		disableMergeCfg[cfgKey] = 0
	}
	err = p.UpdatePDScheduleConfig(ctx, disableMergeCfg)
	if err != nil {
		return
	}

	scheduleLimitCfg := make(map[string]interface{})
	for _, cfgKey := range pdScheduleLimitCfg {
		value := scheduleCfg[cfgKey]
		if value == nil {
			// Ignore non-exist config.
			continue
		}

		// Speed update PD scheduler by enlarging scheduling limits.
		// Multiply limits by store count but no more than 40.
		// Larger limit may make cluster unstable.
		limit := int(value.(float64))
		scheduleLimitCfg[cfgKey] = math.Min(40, float64(limit*len(stores)))
	}
	return undo, p.UpdatePDScheduleConfig(ctx, scheduleLimitCfg)
}

// Close close the connection to pd.
func (p *PdController) Close() {
	p.pdClient.Close()
}

func removePDLeaderScheduler(ctx context.Context, pd *PdController, existSchedulers []string) ([]string, error) {
	removedSchedulers := make([]string, 0, len(existSchedulers))
	for _, scheduler := range existSchedulers {
		err := pd.RemoveScheduler(ctx, scheduler)
		if err != nil {
			return nil, err
		}
		removedSchedulers = append(removedSchedulers, scheduler)
	}
	return removedSchedulers, nil
}
