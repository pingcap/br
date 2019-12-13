package utils

import (
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// unit of storage
const (
	B = 1 << (iota * 10)
	KB
	MB
	GB
	TB
)

type timeCollector struct {
	mu    sync.Mutex
	times map[string]time.Duration
}

// TimeCollector collects time into summary log
var TimeCollector *timeCollector

func init() {
	TimeCollector = &timeCollector{
		times: make(map[string]time.Duration),
	}
}

func (tc *timeCollector) CollectDuration(name string, t time.Duration) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.times[name] = t
}

func (tc *timeCollector) SummaryLog(name string) {
	z := make([]zap.Field, 0, len(tc.times))
	for k, v := range tc.times {
		z = append(z, zap.Duration(k, v))
	}
	log.Info(fmt.Sprintf("%s time summary", name), z...)
}
