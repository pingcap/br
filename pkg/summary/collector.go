package summary

import (
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	BackupUnit  = "backup"
	RestoreUnit = "restore"
)

// LogCollector collects infos into summary log
type LogCollector interface {
	SetUnit(unit string)

	CollectSuccessUnit(name string, t time.Duration)

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
	successUnits   map[string]time.Duration
	failureReasons map[string]error
	fields         []zap.Field
}

func newLogCollector() LogCollector {
	return &logCollector{
		unitCount:      0,
		fields:         make([]zap.Field, 0),
		successUnits:   make(map[string]time.Duration),
		failureReasons: make(map[string]error),
	}
}

func (tc *logCollector) SetUnit(unit string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.unit = unit
}

func (tc *logCollector) CollectSuccessUnit(name string, t time.Duration) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if _, ok := tc.successUnits[name]; !ok {
		tc.successUnits[name] = t
		tc.unitCount++
	} else {
		tc.successUnits[name] += t
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
		tc.successUnits = make(map[string]time.Duration)
		tc.failureReasons = make(map[string]error)
		tc.mu.Unlock()
	}()

	var msg string
	switch tc.unit {
	case BackupUnit:
		msg = fmt.Sprintf("total backup ranges: %d, success ranges: %d, failed ranges: %d",
			tc.unitCount, len(tc.successUnits), len(tc.failureReasons))
	case RestoreUnit:
		msg = fmt.Sprintf("total restore tables: %d, success tables: %d, failed tables: %d",
			tc.unitCount, len(tc.successUnits), len(tc.failureReasons))
	}

	logFields := tc.fields
	if len(tc.failureReasons) != 0 {
		names := make([]string, 0, len(tc.failureReasons))
		for name := range tc.failureReasons {
			// logFields = append(logFields, zap.NamedError(name, reason))
			names = append(names, name)
		}
		logFields = append(logFields, zap.Strings(msg+" failed units", names))
		log.Info(name+" summary", logFields...)
		return
	}
	totalCost := time.Duration(0)
	for _, cost := range tc.successUnits {
		totalCost += cost
	}

	logFields = append(logFields, zap.Duration(msg+" total take", totalCost))

	log.Info(name+" summary", logFields...)
}

// SetLogCollector allow pass LogCollector outside
func SetLogCollector(l LogCollector) {
	collector = l
}
