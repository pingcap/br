package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/pingcap/br/cmd"
)

func main() {
	gCtx := context.Background()
	ctx, cancel := context.WithCancel(gCtx)
	defer cancel()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		log.Warn("received signal to exit", zap.Stringer("signal", sig))
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
		Short:            "br is a TiDB/TiKV cluster backup restore tool.",
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.AddFlags(rootCmd)
	cmd.SetDefaultContext(ctx)
	rootCmd.AddCommand(
		cmd.NewDebugCommand(),
		cmd.NewBackupCommand(),
		cmd.NewRestoreCommand(),
	)
	// Ouputs cmd.Print to stdout.
	rootCmd.SetOut(os.Stdout)

	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
