package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/overvenus/br/cmd"
	"github.com/spf13/cobra"
)

func main() {
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
	}
	cmd.AddFlags(rootCmd)
	rootCmd.AddCommand(cmd.NewMetaCommand())
	rootCmd.AddCommand(cmd.NewBackupCommand())
	if err := rootCmd.ParseFlags(os.Args[1:]); err != nil {
		rootCmd.Println(err)
	}

	cmd.InitDefaultContext(ctx)
	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(rootCmd.UsageString())
	}
}
