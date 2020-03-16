// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package summary

import (
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	// BackupUnit tells summary in backup
	BackupUnit = "backup"
	// RestoreUnit tells summary in restore
	RestoreUnit = "restore"

	// TotalKV is a field we collect during backup/restore
	TotalKV = "total kv"
	// TotalBytes is a field we collect during backup/restore
	TotalBytes = "total bytes"
)

// LogCollector collects infos into summary log
type LogCollector interface {
	SetUnit(unit string)

	CollectSuccessUnit(name string, unitCount int, arg interface{})

	CollectFailureUnit(name string, reason error)

	CollectDuration(name string, t time.Duration)

	CollectInt(name string, t int)

	Summary(name string)
}

type logFunc func(msg string, fields ...zap.Field)

var collector = newLogCollector(log.Info)

type logCollector struct {
	mu               sync.Mutex
	unit             string
	successUnitCount int
	failureUnitCount int
	successCosts     map[string]time.Duration
	successData      map[string]uint64
	failureReasons   map[string]error
	durations        map[string]time.Duration
	ints             map[string]int

	log logFunc
}

func newLogCollector(log logFunc) LogCollector {
	return &logCollector{
		successUnitCount: 0,
		failureUnitCount: 0,
		successCosts:     make(map[string]time.Duration),
		successData:      make(map[string]uint64),
		failureReasons:   make(map[string]error),
		durations:        make(map[string]time.Duration),
		ints:             make(map[string]int),
		log:              log,
	}
}

func (tc *logCollector) SetUnit(unit string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.unit = unit
}

func (tc *logCollector) CollectSuccessUnit(name string, unitCount int, arg interface{}) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	switch v := arg.(type) {
	case time.Duration:
		if _, ok := tc.successCosts[name]; !ok {
			tc.successCosts[name] = v
			tc.successUnitCount += unitCount
		} else {
			tc.successCosts[name] += v
		}
	case uint64:
		if _, ok := tc.successData[name]; !ok {
			tc.successData[name] = v
		} else {
			tc.successData[name] += v
		}
	}
}

func (tc *logCollector) CollectFailureUnit(name string, reason error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if _, ok := tc.failureReasons[name]; !ok {
		tc.failureReasons[name] = reason
		tc.failureUnitCount++
	}
}

func (tc *logCollector) CollectDuration(name string, t time.Duration) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.durations[name] += t
}

func (tc *logCollector) CollectInt(name string, t int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.ints[name] += t
}

func (tc *logCollector) Summary(name string) {
	tc.mu.Lock()
	defer func() {
		tc.durations = make(map[string]time.Duration)
		tc.ints = make(map[string]int)
		tc.successCosts = make(map[string]time.Duration)
		tc.failureReasons = make(map[string]error)
		tc.mu.Unlock()
	}()

	var msg string
	switch tc.unit {
	case BackupUnit:
		msg = fmt.Sprintf("total backup ranges: %d, total success: %d, total failed: %d",
			tc.failureUnitCount+tc.successUnitCount, tc.successUnitCount, tc.failureUnitCount)
		if len(tc.failureReasons) != 0 {
			msg += ", failed ranges"
		}
	case RestoreUnit:
		msg = fmt.Sprintf("total restore files: %d, total success: %d, total failed: %d",
			tc.failureUnitCount+tc.successUnitCount, tc.successUnitCount, tc.failureUnitCount)
		if len(tc.failureReasons) != 0 {
			msg += ", failed files"
		}
	}

	logFields := make([]zap.Field, 0, len(tc.durations)+len(tc.ints))
	for key, val := range tc.durations {
		logFields = append(logFields, zap.Duration(key, val))
	}
	for key, val := range tc.ints {
		logFields = append(logFields, zap.Int(key, val))
	}

	if len(tc.failureReasons) != 0 {
		for unitName, reason:= range tc.failureReasons {
			logFields = append(logFields, zap.String("unitName", unitName), zap.Error(reason))
		}
		log.Info(name+" summary"+msg, logFields...)
		return
	}
	totalCost := time.Duration(0)
	for _, cost := range tc.successCosts {
		totalCost += cost
	}
	msg += fmt.Sprintf(", total take(s): %.2f", totalCost.Seconds())
	for name, data := range tc.successData {
		if name == TotalBytes {
			fData := float64(data) / 1024 / 1024
			if fData > 1 {
				msg += fmt.Sprintf(", total size(MB): %.2f", fData)
				msg += fmt.Sprintf(", avg speed(MB/s): %.2f", fData/totalCost.Seconds())
			} else {
				msg += fmt.Sprintf(", total size(Byte): %d", data)
				msg += fmt.Sprintf(", avg speed(Byte/s): %.2f", float64(data)/totalCost.Seconds())
			}
			continue
		}
		msg += fmt.Sprintf(", %s: %d", name, data)
	}

	tc.log(name+" summary: "+msg, logFields...)
}

// SetLogCollector allow pass LogCollector outside
func SetLogCollector(l LogCollector) {
	collector = l
}
