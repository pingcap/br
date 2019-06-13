package cmd

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/overvenus/br/pkg/meta"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
)

var (
	defaultBacker     *meta.Backer
	defaultBackerOnce = sync.Once{}

	defaultContext     context.Context
	defaultContextOnce = sync.Once{}
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
)

// AddFlags adds flags to the given cmd.
func AddFlags(cmd *cobra.Command) {
	flags := flag.FlagSet{}
	flags.StringP(FlagPD, "u", "127.0.0.1:2379", "PD address")
	flags.String(FlagCA, "", "CA path")
	flags.String(FlagCert, "", "Cert path")
	flags.String(FlagKey, "", "Key path")
	cmd.PersistentFlags().AddFlagSet(&flags)
}

// InitDefaultContext sets the default backer for command line usage.
func InitDefaultContext(ctx context.Context) {
	defaultContextOnce.Do(func() {
		defaultContext = ctx
	})
}

// InitDefaultBacker sets the default backer for command line usage.
// TODO: support https
func InitDefaultBacker(pdAddrs string) {
	defaultBackerOnce.Do(func() {
		var err error
		defaultBacker, err = meta.NewBacker(defaultContext, pdAddrs)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	})
}

// GetDefaultBacker returns the default backer for command line usage.
func GetDefaultBacker() *meta.Backer {
	return defaultBacker
}
