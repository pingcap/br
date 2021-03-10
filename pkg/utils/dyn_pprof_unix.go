// +build linux darwin freebsd unix
// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/lightning/common"
)

const startPProfSignal = syscall.SIGUSR1

// StartDynamicPProfListener starts the listener that will enable pprof when received `startPProfSignal`.
func StartDynamicPProfListener(tls *common.TLS) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, startPProfSignal)
	go func() {
		for sig := range signalChan {
			if sig == startPProfSignal {
				log.Info("signal received, starting pprof...", zap.Stringer("signal", sig))
				if err := StartPProfListener("0.0.0.0:0", tls); err != nil {
					log.Warn("failed to start pprof", zap.Error(err))
					return
				}
			}
		}
	}()
}
