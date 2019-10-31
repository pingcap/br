package cmd

import (
	"context"
	"net/http"
	"sync"

	"github.com/pingcap/br/pkg/meta"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	initOnce       = sync.Once{}
	defaultContext context.Context
	pdAddress      string

	backerOnce    = sync.Once{}
	defaultBacker *meta.Backer
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
	cmd.PersistentFlags().String(FlagStatusAddr, "",
		"Set the HTTP listening address for the status report service. Set to empty string to disable")
	cmd.MarkFlagRequired(FlagPD)
	cmd.MarkFlagRequired(FlagStorage)
}

// Init ...
func Init(cmd *cobra.Command) (err error) {
	initOnce.Do(func() {
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
			go func() {
				if e := http.ListenAndServe(statusAddr, nil); e != nil {
					log.Warn("fail to start pprof", zap.String("addr", statusAddr), zap.Error(e))
				} else {
					log.Info("start pprof", zap.String("addr", statusAddr))
				}
			}()
		}
		// Set the PD server address.
		pdAddress, e = cmd.Flags().GetString(FlagPD)
		if e != nil {
			err = e
			return
		}
	})
	return
}

// GetDefaultBacker returns the default backer for command line usage.
func GetDefaultBacker() (*meta.Backer, error) {
	if pdAddress == "" {
		return nil, errors.New("pd address can not be empty")
	}

	// Lazy initialize and defaultBacker
	var err error
	backerOnce.Do(func() {
		defaultBacker, err = meta.NewBacker(defaultContext, pdAddress)
	})
	if err != nil {
		return nil, err
	}
	return defaultBacker, nil
}

// SetDefaultContext sets the default context for command line usage.
func SetDefaultContext(ctx context.Context) {
	defaultContext = ctx
}

// GetDefaultContext returns the default context for command line usage.
func GetDefaultContext() context.Context {
	return defaultContext
}
