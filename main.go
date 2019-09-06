package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/br/cmd"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func main() {
	conf := &log.Config{Level: "info", File: log.FileLogConfig{}}
	lg, p, _ := log.InitLogger(conf)
	log.ReplaceGlobals(lg, p)

	gCtx := context.Background()
	ctx, cancel := context.WithCancel(gCtx)

	go func() {
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Warn("fail to start pprof", zap.Error(err))
		} else {
			log.Info("start pprof at localhost:6060")
		}
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		switch sig {
		case syscall.SIGTERM:
			cancel()
			os.Exit(0)
		default:
			cancel()
			os.Exit(1)
		}
	}()

	rootCmd := &cobra.Command{
		Use:              "br",
		Short:            "br is a TiDB/TiKV cluster backup tool.",
		TraverseChildren: true,
		SilenceUsage:     true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			// c.DebugFlags()
			return cmd.Init(ctx, c)
		},
	}
	cmd.AddFlags(rootCmd)
	rootCmd.AddCommand(
		cmd.NewMetaCommand(),
		cmd.NewBackupCommand(),
		// cmd.NewTxnCommand(),
		cmd.NewRestoreCommand(),
	)
	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(errors.ErrorStack(err))
		cancel()
		os.Exit(1)
	}
	cancel()
}
