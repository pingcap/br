// +build linux darwin freebsd unix
// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	startPProfSignal = syscall.SIGUSR1
	listenAddrFormat = "0.0.0.0:%d"
)

var (
	pprofPort  = 6060
	signalChan = make(chan os.Signal, 1)
)

// StartDynamicPProfListener starts the listener that will enable pprof when received `startPProfSignal`
func StartDynamicPProfListener() {
	signal.Notify(signalChan, startPProfSignal)
	go onSignalStartPProf(signalChan)
	log.Info(fmt.Sprintf("dynamic pprof started, you can enable pprof by `kill -s %d %d`)",
		startPProfSignal,
		os.Getpid()))
}

func onSignalStartPProf(signals <-chan os.Signal) {
	for sig := range signals {
		if sig == startPProfSignal {
			log.Info("signal received, starting pprof...", zap.Stringer("signal", sig))
			StartPProfListener(fmt.Sprintf(listenAddrFormat, pprofPort))
			// We use different port each time so that we won't stick on a port that already has been used.
			pprofPort++
		}
	}
}
