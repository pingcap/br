package cmd

import (
	"context"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/pingcap/br/pkg/meta"
	"github.com/pingcap/br/pkg/raw"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

var (
	initOnce         = sync.Once{}
	defaultBacker    *meta.Backer
	defaultContext   context.Context
	defaultRawClient *raw.BackupClient
)

const (
	// FlagPD is the name of url flag.
	FlagPD = "pd"
	// FlagCA is the name of CA flag.
	FlagCA = "ca"
	// FlagCert is the name of cert flag.
	FlagCert = "cert"
	// FlagKey is the name of key flag.
	FlagKey = "key"
	// FlagStorage is the name of storage flag.
	FlagStorage = "storage"
	// FlagLogLevel is the name of log-level flag.
	FlagLogLevel = "log-level"
	// FlagLogFile is the name of log-file flag.
	FlagLogFile = "log-file"
	// FlagStatusAddr is the name of status-addr flag.
	FlagStatusAddr = "status-addr"
	// FlagPrometheusAddr is the name of prometheus-addr flag
	FlagPrometheusAddr = "prometheus-addr"
)

// AddFlags adds flags to the given cmd.
func AddFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringP(FlagPD, "u", "127.0.0.1:2379", "PD address")
	cmd.PersistentFlags().String(FlagCA, "", "CA certificate path for TLS connection")
	cmd.PersistentFlags().String(FlagCert, "", "Certificate path for TLS connection")
	cmd.PersistentFlags().String(FlagKey, "", "Private key path for TLS connection")
	cmd.PersistentFlags().StringP(FlagStorage, "s", "",
		`specify the url where backup storage, eg, "local:///path/to/save"`)
	cmd.PersistentFlags().StringP(FlagLogLevel, "L", "info",
		"Set the log level")
	cmd.PersistentFlags().String(FlagLogFile, "",
		"Set the log file path. If not set, logs will output to stdout")
	cmd.PersistentFlags().String(FlagStatusAddr, "localhost:6060",
		"Set the HTTP listening address for the status report service. Set to empty string to disable")
	cmd.PersistentFlags().String(FlagPrometheusAddr, "",
		"Set the HTTP address for the prometheus metrics service. Set to empty string to disable")
	cmd.MarkFlagRequired(FlagPD)
	cmd.MarkFlagRequired(FlagStorage)
}

// Init ...
func Init(ctx context.Context, cmd *cobra.Command) (err error) {
	initOnce.Do(func() {
		defaultContext = ctx

		// Initialize the logger.
		conf := new(log.Config)
		conf.Level, err = cmd.Flags().GetString(FlagLogLevel)
		if err != nil {
			return
		}
		conf.File.Filename, err = cmd.Flags().GetString(FlagLogFile)
		if err != nil {
			return
		}
		lg, p, e := log.InitLogger(conf)
		if e != nil {
			err = e
			return
		}
		log.ReplaceGlobals(lg, p)

		// Initialize the pprof server.
		statusAddr, e := cmd.Flags().GetString(FlagStatusAddr)
		if e != nil {
			err = e
			return
		}
		if len(statusAddr) != 0 {
			runServe(statusAddr)
		}

		prometheusAddr, e := cmd.Flags().GetString(FlagPrometheusAddr)
		if e != nil {
			err = e
			return
		}
		if len(prometheusAddr) == 0 {
			go func() {
				utils.PushPrometheus("br", prometheusAddr, time.Second*10)
			}()
		}

		// Initialize the main program.
		var addr string
		addr, err = cmd.Flags().GetString(FlagPD)
		if err != nil {
			return
		}
		if addr == "" {
			err = errors.New("pd address can not be empty")
			return
		}
		defaultBacker, err = meta.NewBacker(defaultContext, addr)
		if err != nil {
			return
		}
		defaultRawClient, err = raw.NewBackupClient(defaultBacker)
	})
	return
}

// GetDefaultBacker returns the default backer for command line usage.
func GetDefaultBacker() *meta.Backer {
	return defaultBacker
}

// GetDefaultRawClient returns the default back client for command line usage.
func GetDefaultRawClient() *raw.BackupClient {
	return defaultRawClient
}

// GetDefaultContext returns the default context for command line usage.
func GetDefaultContext() context.Context {
	return defaultContext
}

func runServe(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Error("start server failed")
		} else {
			log.Info("start server")
		}
	}()
}
