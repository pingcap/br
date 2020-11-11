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

	"github.com/coreos/go-semver/semver"
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
	pauseTimeout         = 5 * time.Minute
)

var (
	// in v4.0.8 version we can use pause configs
	// see https://github.com/tikv/pd/pull/3088
	pauseConfigVersion = semver.New("4.0.7")
)

// clusterConfig represents a set of scheduler whose config have been modified
// along with their original config.
type clusterConfig struct {
	// Enable PD schedulers before restore
	scheduler []string
	// Original scheudle configuration
	scheduleCfg map[string]interface{}
}

type pauseSchedulerBody struct {
	Delay int64 `json:"delay"`
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
	expectPDCfg = map[string]interface{}{
		"max-merge-region-keys": 0,
		"max-merge-region-size": 0,
		// TODO remove this schedule-limits, see https://github.com/pingcap/br/pull/555#discussion_r509855972
		"leader-schedule-limit":       1,
		"region-schedule-limit":       1,
		"max-snapshot-count":          1,
		"enable-location-replacement": false,
	}

	// DefaultPDCfg find by https://github.com/tikv/pd/blob/master/conf/config.toml.
	DefaultPDCfg = map[string]interface{}{
		"max-merge-region-keys":       200000,
		"max-merge-region-size":       20,
		"leader-schedule-limit":       4,
		"region-schedule-limit":       2048,
		"max-snapshot-count":          3,
		"enable-location-replacement": "true",
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
	version  *semver.Version

	// control the pause schedulers goroutine
	schedulerPauseCh chan struct{}
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
	var versionBytes []byte
	for _, addr := range addrs {
		if addr != "" && !strings.HasPrefix("http", addr) {
			if tlsConf != nil {
				addr = "https://" + addr
			} else {
				addr = "http://" + addr
			}
		}
		processedAddrs = append(processedAddrs, addr)
		versionBytes, failure = pdRequest(ctx, addr, clusterVersionPrefix, cli, http.MethodGet, nil)
		if failure == nil {
			break
		}
	}
	if failure != nil {
		return nil, errors.Annotatef(berrors.ErrPDUpdateFailed, "pd address (%s) not available, please check network", pdAddrs)
	}

	version, err := semver.NewVersion(string(versionBytes))
	if err != nil {
		log.Warn("fail back to old version",
			zap.Binary("version", versionBytes), zap.Error(err))
		version = pauseConfigVersion
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
		version:  version,
		// We should make a buffered channel here otherwise when context canceled,
		// gracefully shutdown will stick at resuming schedulers.
		schedulerPauseCh: make(chan struct{}, 1),
	}, nil
}

func (p *PdController) isPauseConfigEnabled() bool {
	return p.version.Compare(*pauseConfigVersion) > 0
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

// PauseSchedulers remove pd scheduler temporarily.
func (p *PdController) PauseSchedulers(ctx context.Context, schedulers []string) ([]string, error) {
	return p.pauseSchedulersAndConfigWith(ctx, schedulers, nil, pdRequest)
}

func (p *PdController) doPauseSchedulers(ctx context.Context, schedulers []string, post pdHTTPRequest) ([]string, error) {
	// pause this scheduler with 300 seconds
	body, err := json.Marshal(pauseSchedulerBody{Delay: int64(pauseTimeout)})
	if err != nil {
		return nil, err
	}
	// PauseSchedulers remove pd scheduler temporarily.
	removedSchedulers := make([]string, 0, len(schedulers))
	for _, scheduler := range schedulers {
		prefix := fmt.Sprintf("%s/%s", schedulerPrefix, scheduler)
		for _, addr := range p.addrs {
			_, err = post(ctx, addr, prefix, p.cli, http.MethodPost, bytes.NewBuffer(body))
			if err == nil {
				removedSchedulers = append(removedSchedulers, scheduler)
				break
			}
		}
		if err != nil {
			return removedSchedulers, err
		}
	}
	return removedSchedulers, nil
}

func (p *PdController) pauseSchedulersAndConfigWith(
	ctx context.Context, schedulers []string,
	schedulerCfg map[string]interface{}, post pdHTTPRequest,
) ([]string, error) {
	// first pause this scheduler, if the first time failed. we should return the error
	// so put first time out of for loop. and in for loop we could ignore other failed pause.
	removedSchedulers, err := p.doPauseSchedulers(ctx, schedulers, post)
	if err != nil {
		log.Error("failed to pause scheduler at beginning",
			zap.Strings("name", schedulers), zap.Error(err))
		return nil, err
	}
	log.Info("pause scheduler successful at beginning", zap.Strings("name", schedulers))
	if schedulerCfg != nil {
		err = p.doPauseConfigs(ctx, schedulerCfg)
		if err != nil {
			log.Error("failed to pause config at beginning",
				zap.Any("cfg", schedulerCfg), zap.Error(err))
			return nil, err
		}
		log.Info("pause configs successful at beginning", zap.Any("cfg", schedulerCfg))
	}

	go func() {
		tick := time.NewTicker(pauseTimeout / 3)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				_, err := p.doPauseSchedulers(ctx, schedulers, post)
				if err != nil {
					log.Warn("pause scheduler failed, ignore it and wait next time pause", zap.Error(err))
				}
				if schedulerCfg != nil {
					err = p.doPauseConfigs(ctx, schedulerCfg)
					if err != nil {
						log.Warn("pause configs failed, ignore it and wait next time pause", zap.Error(err))
					}
				}
				log.Info("pause scheduler(configs)", zap.Strings("name", removedSchedulers))
			case <-p.schedulerPauseCh:
				log.Info("exit pause scheduler and configs successful")
				return
			}
		}
	}()
	return removedSchedulers, nil
}

// ResumeSchedulers resume pd scheduler.
func (p *PdController) ResumeSchedulers(ctx context.Context, schedulers []string) error {
	return p.resumeSchedulerWith(ctx, schedulers, pdRequest)
}

func (p *PdController) resumeSchedulerWith(ctx context.Context, schedulers []string, post pdHTTPRequest) (err error) {
	log.Info("resume scheduler", zap.Strings("schedulers", schedulers))
	p.schedulerPauseCh <- struct{}{}

	// 0 means stop pause.
	body, err := json.Marshal(pauseSchedulerBody{Delay: 0})
	if err != nil {
		return err
	}
	for _, scheduler := range schedulers {
		prefix := fmt.Sprintf("%s/%s", schedulerPrefix, scheduler)
		for _, addr := range p.addrs {
			_, err = post(ctx, addr, prefix, p.cli, http.MethodPost, bytes.NewBuffer(body))
			if err == nil {
				break
			}
		}
		if err != nil {
			log.Error("failed to resume scheduler after retry, you may reset this scheduler manually"+
				"or just wait this scheduler pause timeout", zap.String("scheduler", scheduler))
		} else {
			log.Info("resume scheduler successful", zap.String("scheduler", scheduler))
		}
	}
	// no need to return error, because the pause will timeout.
	return nil
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
	ctx context.Context, cfg map[string]interface{}, prefixs ...string,
) error {
	prefix := scheduleConfigPrefix
	if len(prefixs) != 0 {
		prefix = prefixs[0]
	}
	for _, addr := range p.addrs {
		reqData, err := json.Marshal(cfg)
		if err != nil {
			return err
		}
		_, e := pdRequest(ctx, addr, prefix,
			p.cli, http.MethodPost, bytes.NewBuffer(reqData))
		if e == nil {
			return nil
		}
		log.Warn("failed to update PD config, will try next", zap.Error(e), zap.String("pd", addr))
	}
	return errors.Annotate(berrors.ErrPDUpdateFailed, "failed to update PD schedule config")
}

func (p *PdController) pauseSchedulersAndConfig(
	ctx context.Context, schedulers []string, cfg map[string]interface{},
) ([]string, error) {
	return p.pauseSchedulersAndConfigWith(ctx, schedulers, cfg, pdRequest)
}

func (p *PdController) doPauseConfigs(ctx context.Context, cfg map[string]interface{}) error {
	// pause this scheduler with 300 seconds
	prefix := fmt.Sprintf("%s?ttlSecond=%d", schedulerPrefix, 300)
	return p.UpdatePDScheduleConfig(ctx, cfg, prefix)
}

func restoreSchedulers(ctx context.Context, pd *PdController, clusterCfg clusterConfig) error {
	if err := pd.ResumeSchedulers(ctx, clusterCfg.scheduler); err != nil {
		return errors.Annotate(err, "fail to add PD schedulers")
	}
	log.Info("restoring config", zap.Any("config", clusterCfg.scheduleCfg))
	mergeCfg := make(map[string]interface{})
	for cfgKey := range expectPDCfg {
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
	stores, err := p.pdClient.GetAllStores(ctx)
	if err != nil {
		return
	}
	scheduleCfg, err := p.GetPDScheduleConfig(ctx)
	if err != nil {
		return
	}
	disablePDCfg := make(map[string]interface{})
	for cfgKey, cfgVal := range expectPDCfg {
		value, ok := scheduleCfg[cfgKey]
		if !ok {
			// Ignore non-exist config.
			continue
		}
		switch v := cfgVal.(type) {
		case bool:
			disablePDCfg[cfgKey] = "false"
		case int:
			limit := int(value.(float64))
			disablePDCfg[cfgKey] = int(math.Min(40, float64(limit*len(stores)))) * v
		}
	}
	undo = p.makeUndoFunctionByConfig(clusterConfig{scheduleCfg: scheduleCfg})
	log.Debug("saved PD config", zap.Any("config", scheduleCfg))

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

	var removedSchedulers []string
	if p.isPauseConfigEnabled() {
		// after 4.0.8 we can set these config with TTL
		removedSchedulers, err = p.pauseSchedulersAndConfig(ctx, needRemoveSchedulers, disablePDCfg)
	} else {
		// adapt to earlier version (before 4.0.8) of pd cluster
		// which doesn't have temporary config setting.
		err = p.UpdatePDScheduleConfig(ctx, disablePDCfg)
		if err != nil {
			return
		}
		removedSchedulers, err = p.PauseSchedulers(ctx, needRemoveSchedulers)
	}
	undo = p.makeUndoFunctionByConfig(clusterConfig{scheduler: removedSchedulers, scheduleCfg: scheduleCfg})
	return undo, err
}

// Close close the connection to pd.
func (p *PdController) Close() {
	p.pdClient.Close()
	close(p.schedulerPauseCh)
}
