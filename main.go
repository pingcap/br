package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/br/cmd"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
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
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			// c.DebugFlags()
			return cmd.Init(ctx, c)
		},
	}
	cmd.AddFlags(rootCmd)
	rootCmd.AddCommand(
		cmd.NewVersionCommand(),
		cmd.NewMetaCommand(),
		cmd.NewBackupCommand(),
		cmd.NewRestoreCommand(),
	)
	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(errors.ErrorStack(err))
		os.Exit(1)
	}
}
