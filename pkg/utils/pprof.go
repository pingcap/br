// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"net/http"
	"net/http/pprof"
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	startedPProf = ""
	mu           = new(sync.Mutex)
)

// StartPProfListener forks a new goroutine listening on specified port and provide pprof info.
func StartPProfListener(statusAddr string) {
	mu.Lock()
	defer mu.Unlock()

	if startedPProf != "" {
		log.Warn("Try to start pprof when it has been started, nothing will happen", zap.String("address", startedPProf))
		return
	}

	go func() {
		_ = pprof.Handler
		if len(statusAddr) != 0 {
			log.Info("start pprof", zap.String("addr", statusAddr))
			if e := http.ListenAndServe(statusAddr, nil); e != nil {
				log.Warn("fail to start pprof, if port is in used and you are starting pprof by signal, please retry to select a new port",
					zap.String("addr", statusAddr), zap.Error(e))
				return
			}
			startedPProf = statusAddr
		}
	}()
}
