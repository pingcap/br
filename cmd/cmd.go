package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/utils"
)

var (
	initOnce       = sync.Once{}
	defaultContext context.Context
	pdAddress      string
	hasLogFile     uint64

	connOnce   = sync.Once{}
	defaultMgr *conn.Mgr
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
	// FlagSlowLogFile is the name of slow-log-file flag.
	FlagSlowLogFile = "slow-log-file"

	flagDatabase = "db"
	flagTable    = "table"

	flagVersion      = "version"
	flagVersionShort = "V"
)

// AddFlags adds flags to the given cmd.
func AddFlags(cmd *cobra.Command) {
	cmd.Version = utils.BRInfo()
	cmd.Flags().BoolP(flagVersion, flagVersionShort, false, "Display version information about BR")
	cmd.SetVersionTemplate("{{printf \"%s\" .Version}}\n")

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
	storage.DefineFlags(cmd.PersistentFlags())

	cmd.PersistentFlags().StringP(FlagSlowLogFile, "", "",
		"Set the slow log file path. If not set, discard slow logs")
	_ = cmd.PersistentFlags().MarkHidden(FlagSlowLogFile)
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
		if len(conf.File.Filename) != 0 {
			atomic.StoreUint64(&hasLogFile, 1)
		}
		lg, p, e := log.InitLogger(conf)
		if e != nil {
			err = e
			return
		}
		log.ReplaceGlobals(lg, p)

		slowLogFilename, e := cmd.Flags().GetString(FlagSlowLogFile)
		if e != nil {
			err = e
			return
		}
		if len(slowLogFilename) != 0 {
			slowCfg := logutil.LogConfig{SlowQueryFile: slowLogFilename}
			e = logutil.InitLogger(&slowCfg)
			if e != nil {
				err = e
				return
			}
		} else {
			// Hack! Discard slow log by setting log level to PanicLevel
			logutil.SlowQueryLogger.SetLevel(logrus.PanicLevel)
		}

		// Initialize the pprof server.
		statusAddr, e := cmd.Flags().GetString(FlagStatusAddr)
		if e != nil {
			err = e
			return
		}
		go func() {
			// Make sure pprof is registered.
			_ = pprof.Handler
			if len(statusAddr) != 0 {
				log.Info("start pprof", zap.String("addr", statusAddr))
				if e := http.ListenAndServe(statusAddr, nil); e != nil {
					log.Warn("fail to start pprof", zap.String("addr", statusAddr), zap.Error(e))
				}
			}
		}()
		// Set the PD server address.
		pdAddress, e = cmd.Flags().GetString(FlagPD)
		if e != nil {
			err = e
			return
		}
	})
	return err
}

// HasLogFile returns whether we set a log file
func HasLogFile() bool {
	return atomic.LoadUint64(&hasLogFile) != uint64(0)
}

// GetDefaultMgr returns the default mgr for command line usage.
func GetDefaultMgr() (*conn.Mgr, error) {
	if pdAddress == "" {
		return nil, errors.New("pd address can not be empty")
	}

	// Lazy initialize and defaultMgr
	var err error
	connOnce.Do(func() {
		var storage kv.Storage
		storage, err = tikv.Driver{}.Open(
			// Disable GC because TiDB enables GC already.
			fmt.Sprintf("tikv://%s?disableGC=true", pdAddress))
		if err != nil {
			return
		}
		defaultMgr, err = conn.NewMgr(defaultContext, pdAddress, storage.(tikv.Storage))
	})
	if err != nil {
		return nil, err
	}
	return defaultMgr, nil
}

// SetDefaultContext sets the default context for command line usage.
func SetDefaultContext(ctx context.Context) {
	defaultContext = ctx
}

// GetDefaultContext returns the default context for command line usage.
func GetDefaultContext() context.Context {
	return defaultContext
}
