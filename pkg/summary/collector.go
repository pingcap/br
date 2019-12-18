package summary

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// LogCollector collects infos into summary log
type LogCollector interface {
	CollectDuration(name string, t time.Duration)

	CollectInt(name string, t int)

	Summary(name string)
}

// Collector collects infos into summary log
var Collector = newLogCollector()

type logCollector struct {
	mu     sync.Mutex
	fields []zap.Field
}

func newLogCollector() LogCollector {
	return &logCollector{
		fields: make([]zap.Field, 0),
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
	defer tc.mu.Unlock()
	log.Info(name+" summary", tc.fields...)
	tc.fields = tc.fields[:0]
}

// SetLogCollector allow pass LogCollector outside
func SetLogCollector(l LogCollector) {
	Collector = l
}
