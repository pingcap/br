package utils

import (
	"time"

	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

// PushPrometheus pushes metrics to Prometheus Pushgateway.
func PushPrometheus(job, addr string, interval time.Duration) {
	for {
		err := push.New(addr, job).Gatherer(prometheus.DefaultGatherer).Push()
		if err != nil {
			log.Warn("could not push metrics to Prometheus Pushgateway")
		}
		time.Sleep(interval)
	}
}
