package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/overvenus/br/cmd"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
)

func main() {
	conf := &log.Config{Level: "info", File: log.FileLogConfig{}}
	lg, p, _ := log.InitLogger(conf)
	log.ReplaceGlobals(lg, p)

	gCtx := context.Background()
	ctx, cancel := context.WithCancel(gCtx)

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
		cmd.NewFullBackupCommand(),
		cmd.NewRegionCommand(),
		cmd.NewTxnCommand(),
	)
	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(errors.ErrorStack(err))
		cancel()
		os.Exit(1)
	}
	cancel()
}
