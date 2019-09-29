package cmd

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewMetaCommand return a meta subcommand.
func NewMetaCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:   "meta <subcommand>",
		Short: "show meta data of a cluster",
	}
	meta.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "show cluster version",
		RunE: func(cmd *cobra.Command, _ []string) error {
			backer := GetDefaultBacker()
			v, err := backer.GetClusterVersion()
			if err != nil {
				fmt.Println(errors.ErrorStack(err))
				return err
			}
			cmd.Println(v)
			return nil
		},
	})
	meta.AddCommand(&cobra.Command{
		Use:   "safepoint",
		Short: "show the current GC safepoint of cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			backer := GetDefaultBacker()
			sp, err := backer.GetGCSafePoint()
			if err != nil {
				fmt.Println(errors.ErrorStack(err))
				return err
			}
			cmd.Printf("Timestamp { Physical: %d, Logical: %d }\n",
				sp.Physical, sp.Logical)
			return nil
		},
	})
	return meta
}
