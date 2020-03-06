package cmd

import (
	"context"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/gluetidb"
	"github.com/pingcap/br/pkg/task"
	"github.com/pingcap/br/pkg/utils"
)

var (
	initOnce       = sync.Once{}
	defaultContext context.Context
	hasLogFile     uint64
	tidbGlue       = gluetidb.Glue{}
)

const (
	// FlagLogLevel is the name of log-level flag.
	FlagLogLevel = "log-level"
	// FlagLogFile is the name of log-file flag.
	FlagLogFile = "log-file"
	// FlagStatusAddr is the name of status-addr flag.
	FlagStatusAddr = "status-addr"
	// FlagSlowLogFile is the name of slow-log-file flag.
	FlagSlowLogFile = "slow-log-file"

	flagVersion      = "version"
	flagVersionShort = "V"
)

// AddFlags adds flags to the given cmd.
func AddFlags(cmd *cobra.Command) {
	cmd.Version = utils.BRInfo()
	cmd.Flags().BoolP(flagVersion, flagVersionShort, false, "Display version information about BR")
	cmd.SetVersionTemplate("{{printf \"%s\" .Version}}\n")

	cmd.PersistentFlags().StringP(FlagLogLevel, "L", "info",
		"Set the log level")
	cmd.PersistentFlags().String(FlagLogFile, "",
		"Set the log file path. If not set, logs will output to stdout")
	cmd.PersistentFlags().String(FlagStatusAddr, "",
		"Set the HTTP listening address for the status report service. Set to empty string to disable")
	task.DefineCommonFlags(cmd.PersistentFlags())

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
		tidbLogCfg := logutil.LogConfig{}
		if len(slowLogFilename) != 0 {
			tidbLogCfg.SlowQueryFile = slowLogFilename
		} else {
			// Hack! Discard slow log by setting log level to PanicLevel
			logutil.SlowQueryLogger.SetLevel(logrus.PanicLevel)
			// Disable annoying TiDB Log.
			tidbLogCfg.Level = "fatal"
		}
		e = logutil.InitLogger(&tidbLogCfg)
		if e != nil {
			err = e
			return
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
	})
	return err
}

// HasLogFile returns whether we set a log file
func HasLogFile() bool {
	return atomic.LoadUint64(&hasLogFile) != uint64(0)
}

// SetDefaultContext sets the default context for command line usage.
func SetDefaultContext(ctx context.Context) {
	defaultContext = ctx
}

// GetDefaultContext returns the default context for command line usage.
func GetDefaultContext() context.Context {
	return defaultContext
}
