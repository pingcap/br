package cmd

import (
	"context"
	"sync"

	"github.com/overvenus/br/pkg/meta"
	"github.com/overvenus/br/pkg/raw"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

var (
	initOnce         = sync.Once{}
	defaultBacker    *meta.Backer
	defaultContext   context.Context
	defaultRawClient *raw.BackupClient
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
	// FlagStore is the name of key flag.
	FlagStore = "store"
)

// AddFlags adds flags to the given cmd.
func AddFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringP(FlagPD, "u", "127.0.0.1:2379", "PD address")
	cmd.PersistentFlags().String(FlagCA, "", "CA path")
	cmd.PersistentFlags().String(FlagCert, "", "Cert path")
	cmd.PersistentFlags().String(FlagKey, "", "Key path")
	cmd.PersistentFlags().Uint64P(FlagStore, "s", 0, "backup at the specific store")
	cmd.MarkFlagRequired(FlagPD)
}

// Init ...
func Init(ctx context.Context, cmd *cobra.Command) (err error) {
	initOnce.Do(func() {
		defaultContext = ctx
		var addr string
		addr, err = cmd.Flags().GetString(FlagPD)
		if err != nil {
			return
		}
		if addr == "" {
			err = errors.Errorf("pd address can not be empty")
			return
		}
		defaultBacker, err = meta.NewBacker(defaultContext, addr)
		var storeID uint64
		storeID, err = cmd.Flags().GetUint64("store")
		if err != nil {
			return
		}
		defaultRawClient, err = raw.NewBackupClient(defaultBacker, storeID)
	})
	return
}

// GetDefaultBacker returns the default backer for command line usage.
func GetDefaultBacker() *meta.Backer {
	return defaultBacker
}

// GetDefaultRawClient returns the default back client for command line usage.
func GetDefaultRawClient() *raw.BackupClient {
	return defaultRawClient
}
