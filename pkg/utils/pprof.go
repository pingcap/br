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

func listen(statusAddr string) net.Listener {
	mu.Lock()
	defer mu.Unlock()
	if startedPProf != "" {
		log.Warn("Try to start pprof when it has been started, nothing will happen", zap.String("address", startedPProf))
		return nil
	}
	failpoint.Inject("determined-pprof-port", func(v failpoint.Value) {
		port := v.(int)
		statusAddr = fmt.Sprintf(":%d", port)
		log.Info("injecting failpoint, pprof will start at determined port", zap.Int("port", port))
	})
	listener, err := net.Listen("tcp", statusAddr)
	if err != nil {
		log.Warn("failed to start pprof", zap.String("addr", statusAddr), zap.Error(err))
		return nil
	}
	startedPProf = listener.Addr().String()
	log.Info("bound pprof to addr", zap.String("addr", startedPProf))
	_, _ = fmt.Fprintf(os.Stderr, "bound pprof to addr %s\n", startedPProf)
	return listener
}

func startPProfWith(statusAddr string, serve func(net.Listener) error) {
	if listener := listen(statusAddr); listener != nil {
		go func() {
			if e := serve(listener); e != nil {
				log.Warn("failed to serve pprof", zap.String("addr", startedPProf), zap.Error(e))
				mu.Lock()
				startedPProf = ""
				mu.Unlock()
				return
			}
		}()
	}
}

// StartPProfListenerTLS forks a new goroutine listening on specified port and provide pprof info, with TLS.
func StartPProfListenerTLS(statusAddr, certFile, keyFile string) {
	startPProfWith(statusAddr, func(listener net.Listener) error {
		return http.ServeTLS(listener, nil, certFile, keyFile)
	})
}

// StartPProfListener forks a new goroutine listening on specified port and provide pprof info.
func StartPProfListener(statusAddr string) {
	startPProfWith(statusAddr, func(listener net.Listener) error {
		return http.Serve(listener, nil)
	})
}
