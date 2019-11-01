package restore

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	restoreFileCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "br",
			Subsystem: "restore",
			Name:      "restore_file",
			Help:      "Restore file statistic.",
		}, []string{"type"})

	restoreFileHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "br",
			Subsystem: "restore",
			Name:      "region_file_seconds",
			Help:      "Restore file latency distributions.",
			Buckets:   prometheus.ExponentialBuckets(0.05, 2, 16),
		})
)

func init() {
	prometheus.MustRegister(restoreFileCounter)
	prometheus.MustRegister(restoreFileHistogram)
}
