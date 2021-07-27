// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package logutil

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// TrivialRater is a trivial rate tracer.
// It doesn't record any time sequence, and always
// return the average speed over all the time.
// TODO: replace it with Prometheus.
type TrivialRater struct {
	start   time.Time
	current uint64
}

// NewTrivialRater make a trivial rater.
func NewTrivialRater() TrivialRater {
	return TrivialRater{
		start:   time.Now(),
		current: 0,
	}
}

// Success adds n success units for the rater.
func (r *TrivialRater) Success(n uint64) {
	atomic.AddUint64(&r.current, n)
}

// Rate returns the rate over all time, in the given unit.
func (r *TrivialRater) Rate(unit time.Duration) float64 {
	return r.RateAt(time.Now(), unit)
}

// RateAt returns the rate until some instant.
func (r *TrivialRater) RateAt(instant time.Time, unit time.Duration) float64 {
	return float64(atomic.LoadUint64(&r.current)) / float64(instant.Sub(r.start)) * float64(unit)
}

// L make a logger with the current speed.
func (r *TrivialRater) L() *zap.Logger {
	return log.With(zap.String("speed", fmt.Sprintf("%.2f ops/s", r.Rate(time.Second))))
}
