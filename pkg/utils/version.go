package utils

import (
	"fmt"

	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

// Version information.
var (
	BRReleaseVersion = "None"
	BRBuildTS        = "None"
	BRGitHash        = "None"
	BRGitBranch      = "None"
)

// LogBRInfo prints the BR version information.
func LogBRInfo() {
	log.Info("Welcome to Backup & Restore (BR)")
	log.Info("BR", zap.String("release-version", BRReleaseVersion))
	log.Info("BR", zap.String("git-hash", BRGitHash))
	log.Info("BR", zap.String("git-branch", BRGitBranch))
	log.Info("BR", zap.String("utc-build-time", BRBuildTS))
}

// PrintBRInfo prints the BR version information without log info.
func PrintBRInfo() {
	fmt.Println("Release Version:", BRReleaseVersion)
	fmt.Println("Git Commit Hash:", BRGitHash)
	fmt.Println("Git Branch:", BRGitBranch)
	fmt.Println("UTC Build Time: ", BRBuildTS)
}

// LogArguments prints origin command arguments
func LogArguments(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		log.Info("argument", zap.Stringer(f.Name, f.Value))
	})
}
