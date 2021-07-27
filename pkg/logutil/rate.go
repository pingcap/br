// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package logutil

import (
	"fmt"
	"time"

	"github.com/pingcap/br/pkg/lightning/metric"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// TrivialRater is a trivial rate tracer.
// It doesn't record any time sequence, and always
// return the average speed over all the time.
type TrivialRater struct {
	start   time.Time
	current prometheus.Counter
}

// NewTrivialRater make a trivial rater.
func NewTrivialRater() TrivialRater {
	return TrivialRater{
		start: time.Now(),
		// TODO: when br decided to introduce Prometheus, move this to its metric package.
		current: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "BR",
			Name:      "table_created",
			Help:      "The count of tables have been created.",
		}),
	}
}

// Success adds n success units for the rater.
func (r *TrivialRater) Success(n float64) {
	r.current.Add(n)
}

// Rate returns the rate over all time, in the given unit.
func (r *TrivialRater) Rate() float64 {
	return r.RateAt(time.Now())
}

// RateAt returns the rate until some instant.
func (r *TrivialRater) RateAt(instant time.Time) float64 {
	return metric.ReadCounter(r.current) / float64(instant.Sub(r.start)) * float64(time.Second)
}

// L make a logger with the current speed.
func (r *TrivialRater) L() *zap.Logger {
	return log.With(zap.String("speed", fmt.Sprintf("%.2f ops/s", r.Rate())))
}
