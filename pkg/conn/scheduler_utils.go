// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package conn

import (
	"context"
	"math"

	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/errors"
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

func addPDLeaderScheduler(ctx context.Context, mgr *Mgr, removedSchedulers []string) error {
	for _, scheduler := range removedSchedulers {
		err := mgr.AddScheduler(ctx, scheduler)
		if err != nil {
			return err
		}
	}
	return nil
}

func restoreSchedulers(ctx context.Context, mgr *Mgr, clusterCfg clusterConfig) error {
	if err := addPDLeaderScheduler(ctx, mgr, clusterCfg.scheduler); err != nil {
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
	if err := mgr.UpdatePDScheduleConfig(ctx, mergeCfg); err != nil {
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
	if err := mgr.UpdatePDScheduleConfig(ctx, scheduleLimitCfg); err != nil {
		return errors.Annotate(err, "fail to update PD schedule config")
	}
	return nil
}

// RemoveSchedulers removes the schedulers that may slow down BR speed.
func RemoveSchedulers(ctx context.Context, mgr *Mgr) (utils.UndoFunc, error) {
	// Remove default PD scheduler that may affect restore process.
	existSchedulers, err := mgr.ListSchedulers(ctx)
	if err != nil {
		return utils.Nop, nil
	}
	needRemoveSchedulers := make([]string, 0, len(existSchedulers))
	for _, s := range existSchedulers {
		if _, ok := schedulers[s]; ok {
			needRemoveSchedulers = append(needRemoveSchedulers, s)
		}
	}
	scheduler, err := removePDLeaderScheduler(ctx, mgr, needRemoveSchedulers)
	if err != nil {
		return utils.Nop, nil
	}

	stores, err := mgr.GetPDClient().GetAllStores(ctx)
	if err != nil {
		return utils.Nop, err
	}

	scheduleCfg, err := mgr.GetPDScheduleConfig(ctx)
	if err != nil {
		return utils.Nop, err
	}

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
	err = mgr.UpdatePDScheduleConfig(ctx, disableMergeCfg)
	if err != nil {
		return utils.Nop, err
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
	err = mgr.UpdatePDScheduleConfig(ctx, scheduleLimitCfg)
	if err != nil {
		return utils.Nop, err
	}

	cluster := clusterConfig{
		scheduler:   scheduler,
		scheduleCfg: scheduleCfg,
	}
	restore := func(ctx context.Context) error {
		return restoreSchedulers(ctx, mgr, cluster)
	}
	return restore, nil
}

func removePDLeaderScheduler(ctx context.Context, mgr *Mgr, existSchedulers []string) ([]string, error) {
	removedSchedulers := make([]string, 0, len(existSchedulers))
	for _, scheduler := range existSchedulers {
		err := mgr.RemoveScheduler(ctx, scheduler)
		if err != nil {
			return nil, err
		}
		removedSchedulers = append(removedSchedulers, scheduler)
	}
	return removedSchedulers, nil
}
