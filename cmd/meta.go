package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// NewMetaCommand return a meta subcommand.
func NewMetaCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:   "meta <subcommand>",
		Short: "show meta data of a cluster",
		PersistentPreRunE: func(cmd *cobra.Command, arg []string) error {
			addr, err := cmd.InheritedFlags().GetString(FlagPD)
			if err != nil {
				return err
			}
			InitDefaultBacker(addr)
			return nil
		},
	}
	meta.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "show cluster version",
		Run: func(cmd *cobra.Command, _ []string) {
			backer := GetDefaultBacker()
			v, err := backer.GetClusterVersion()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			cmd.Println(v)
		},
	})
	meta.AddCommand(&cobra.Command{
		Use:   "safepoint",
		Short: "show the current GC safepoint of cluster",
		Run: func(cmd *cobra.Command, _ []string) {
			backer := GetDefaultBacker()
			sp, err := backer.GetGCSaftPoint()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			cmd.Printf("Timestamp { Physical: %d, Logical: %d }\n",
				sp.Physical, sp.Logical)
		},
	})
	return meta
}
