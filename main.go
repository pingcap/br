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

// CommandFlags are flags that used in all Commands
type CommandFlags struct {
	URL      string
	CAPath   string
	CertPath string
	KeyPath  string
}

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

	commandFlags := CommandFlags{}
	rootCmd := &cobra.Command{
		Use:   "br",
		Short: "br is a TiDB/TiKV cluster backup tool.",
	}
	rootCmd.PersistentFlags().StringVarP(&commandFlags.URL, "pd", "u", "http://127.0.0.1:2379", "pd address")
	rootCmd.AddCommand(cmd.NewMetaCommand())
	if err := rootCmd.ParseFlags(os.Args[1:]); err != nil {
		rootCmd.Println(err)
	}
	// TODO: support https

	cmd.SetDefaultBacker(ctx, commandFlags.URL)
	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(rootCmd.UsageString())
	}
}
