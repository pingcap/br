// +build linux darwin freebsd unix
// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const startPProfSignal = syscall.SIGUSR1

var signalChan = make(chan os.Signal, 1)

// StartDynamicPProfListener starts the listener that will enable pprof when received `startPProfSignal`.
func StartDynamicPProfListener() {
	signal.Notify(signalChan, startPProfSignal)
	go onSignalStartPProf(signalChan)
}

func onSignalStartPProf(signals <-chan os.Signal) {
	for sig := range signals {
		if sig == startPProfSignal {
			log.Info("signal received, starting pprof...", zap.Stringer("signal", sig))
			StartPProfListener("0.0.0.0:0")
		}
	}
}
