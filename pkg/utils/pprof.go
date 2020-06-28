// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"fmt"
	"net"
	"net/http"
	"os"

	// #nosec
	// register HTTP handler for /debug/pprof
	_ "net/http/pprof"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	startedPProf = ""
	mu           sync.Mutex
)

// StartPProfListener forks a new goroutine listening on specified port and provide pprof info.
func StartPProfListener(statusAddr string) {
	mu.Lock()
	defer mu.Unlock()
	if startedPProf != "" {
		log.Warn("Try to start pprof when it has been started, nothing will happen", zap.String("address", startedPProf))
		return
	}
	failpoint.Inject("determined-pprof-port", func(v failpoint.Value) {
		port := v.(int)
		statusAddr = fmt.Sprintf(":%d", port)
		log.Info("injecting failpoint, pprof will start at determined port", zap.Int("port", port))
	})
	listener, err := net.Listen("tcp", statusAddr)
	log.Info("bind pprof to addr", zap.Stringer("addr", listener.Addr()))
	_, _ = fmt.Fprintf(os.Stderr, "bind pprof to addr %s\n", listener.Addr().String())
	if err != nil {
		log.Warn("failed to start pprof", zap.String("addr", statusAddr), zap.Error(err))
	}
	startedPProf = listener.Addr().String()

	go func() {
		if e := http.Serve(listener, nil); e != nil {
			log.Warn("failed to serve pprof", zap.String("addr", startedPProf), zap.Error(e))
			mu.Lock()
			startedPProf = ""
			mu.Unlock()
			return
		}
	}()
}
