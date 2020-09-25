// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package conn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"

	"github.com/pingcap/errors"
	pd "github.com/tikv/pd/client"

	"github.com/pingcap/br/pkg/utils"
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
	schedulers = map[string]struct{}{
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
)

// PdController manage get/update config from pd.
type PdController struct {
	addrs    []string
	cli      *http.Client
	pdClient pd.Client
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

// RemoveScheduler remove pd scheduler.
func (p *PdController) RemoveScheduler(ctx context.Context, scheduler string) error {
	return p.removeSchedulerWith(ctx, scheduler, pdRequest)
}

func (p *PdController) removeSchedulerWith(ctx context.Context, scheduler string, delete pdHTTPRequest) (err error) {
	for _, addr := range p.addrs {
		prefix := fmt.Sprintf("%s/%s", schdulerPrefix, scheduler)
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
		_, err = post(ctx, addr, schdulerPrefix, p.cli, http.MethodPost, body)
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
		v, e := get(ctx, addr, schdulerPrefix, p.cli, http.MethodGet, nil)
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
	return errors.New("update PD schedule config failed")
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
		if _, ok := schedulers[s]; ok {
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
