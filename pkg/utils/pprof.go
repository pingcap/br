// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"

	// #nosec
	// register HTTP handler for /debug/pprof
	_ "net/http/pprof"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sourcegraph.com/sourcegraph/appdash"
	appdashot "sourcegraph.com/sourcegraph/appdash/opentracing"
	"sourcegraph.com/sourcegraph/appdash/traceapp"
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
	if err != nil {
		log.Warn("failed to start pprof", zap.String("addr", statusAddr), zap.Error(err))
		return
	}
	startedPProf = listener.Addr().String()
	log.Info("bound pprof to addr", zap.String("addr", startedPProf))
	_, _ = fmt.Fprintf(os.Stderr, "bound pprof to addr %s\n", startedPProf)

	store := appdash.NewMemoryStore()
	cs := appdash.NewServer(listener, appdash.NewLocalCollector(store))
	go cs.Start()
	// Print the URL at which the web UI will be running.
	appdashURL, err := url.Parse("http://" + statusAddr)
	if err != nil {
		log.L().Error("Error parsing", zap.String("statusAddr", statusAddr), zap.Error(err))
	}
	fmt.Printf("To see your traces, go to %s/traces\n", appdashURL)

	// Start the web UI in a separate goroutine.
	tapp, err := traceapp.New(nil, appdashURL)
	if err != nil {
		log.L().Error("Error new trace app", zap.Error(err))
	}
	tapp.Store = store
	tapp.Queryer = store

	tracer := appdashot.NewTracer(appdash.NewRemoteCollector(listener.Addr().String()))
	opentracing.InitGlobalTracer(tracer)

	go func() {
		if e := http.Serve(listener, tapp); e != nil {
			log.Warn("failed to serve pprof", zap.String("addr", startedPProf), zap.Error(e))
			mu.Lock()
			startedPProf = ""
			mu.Unlock()
			return
		}
	}()
}
