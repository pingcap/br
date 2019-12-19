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

	CollectSuccessUnit(name string, arg interface{})

	CollectFailureUnit(name string, reason error)

	CollectDuration(name string, t time.Duration)

	CollectInt(name string, t int)

	Summary(name string)
}

var collector = newLogCollector()

type logCollector struct {
	mu             sync.Mutex
	unit           string
	unitCount      int
	successCosts   map[string]time.Duration
	successData    map[string]uint64
	failureReasons map[string]error
	fields         []zap.Field
}

func newLogCollector() LogCollector {
	return &logCollector{
		unitCount:      0,
		fields:         make([]zap.Field, 0),
		successCosts:   make(map[string]time.Duration),
		successData:    make(map[string]uint64),
		failureReasons: make(map[string]error),
	}
}

func (tc *logCollector) SetUnit(unit string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.unit = unit
}

func (tc *logCollector) CollectSuccessUnit(name string, arg interface{}) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	switch v := arg.(type) {
	case time.Duration:
		if _, ok := tc.successCosts[name]; !ok {
			tc.successCosts[name] = v
			tc.unitCount++
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
		tc.unitCount++
	}
}

func (tc *logCollector) CollectDuration(name string, t time.Duration) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.fields = append(tc.fields, zap.Duration(name, t))
}

func (tc *logCollector) CollectInt(name string, t int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.fields = append(tc.fields, zap.Int(name, t))
}

func (tc *logCollector) Summary(name string) {
	tc.mu.Lock()
	defer func() {
		tc.fields = tc.fields[:0]
		tc.successCosts = make(map[string]time.Duration)
		tc.failureReasons = make(map[string]error)
		tc.mu.Unlock()
	}()

	var msg string
	switch tc.unit {
	case BackupUnit:
		msg = fmt.Sprintf("total backup ranges: %d, total success: %d, total failed: %d",
			tc.unitCount, len(tc.successCosts), len(tc.failureReasons))
		if len(tc.failureReasons) != 0 {
			msg += ", failed ranges"
		}
	case RestoreUnit:
		msg = fmt.Sprintf("total restore tables: %d, total success: %d, total failed: %d",
			tc.unitCount, len(tc.successCosts), len(tc.failureReasons))
		if len(tc.failureReasons) != 0 {
			msg += ", failed tables"
		}
	}

	logFields := tc.fields
	if len(tc.failureReasons) != 0 {
		names := make([]string, 0, len(tc.failureReasons))
		for name := range tc.failureReasons {
			// logFields = append(logFields, zap.NamedError(name, reason))
			names = append(names, name)
		}
		logFields = append(logFields, zap.Strings(msg, names))
		log.Info(name+" summary", logFields...)
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

	log.Info(name+" summary: "+msg, logFields...)
}

// SetLogCollector allow pass LogCollector outside
func SetLogCollector(l LogCollector) {
	collector = l
}
