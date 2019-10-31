package cmd

import (
	"github.com/pingcap/br/pkg/utils"
	"github.com/spf13/cobra"
)

// NewVersionCommand returns a restore subcommand
func NewVersionCommand() *cobra.Command {
	bp := &cobra.Command{
		Use:   "version",
		Short: "output version information",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.PrintBRInfo()
		},
	}
	return bp
}
