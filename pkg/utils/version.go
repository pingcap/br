// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"bytes"
	"fmt"
	"runtime"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/israce"
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
	goVersion        = runtime.Version()
)

// LogBRInfo logs version information about BR.
func LogBRInfo() {
	log.Info("Welcome to Backup & Restore (BR)")
	log.Info("BR", zap.String("release-version", BRReleaseVersion))
	log.Info("BR", zap.String("git-hash", BRGitHash))
	log.Info("BR", zap.String("git-branch", BRGitBranch))
	log.Info("BR", zap.String("go-version", goVersion))
	log.Info("BR", zap.String("utc-build-time", BRBuildTS))
	log.Info("BR", zap.Bool("race-enabled", israce.RaceEnabled))
}

// BRInfo returns version information about BR.
func BRInfo() string {
	buf := bytes.Buffer{}
	fmt.Fprintf(&buf, "Release Version: %s\n", BRReleaseVersion)
	fmt.Fprintf(&buf, "Git Commit Hash: %s\n", BRGitHash)
	fmt.Fprintf(&buf, "Git Branch: %s\n", BRGitBranch)
	fmt.Fprintf(&buf, "Go Version: %s\n", goVersion)
	fmt.Fprintf(&buf, "UTC Build Time: %s\n", BRBuildTS)
	fmt.Fprintf(&buf, "Race Enabled: %t", israce.RaceEnabled)
	return buf.String()
}

// LogArguments prints origin command arguments
func LogArguments(cmd *cobra.Command) {
	var fields []zap.Field
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		fields = append(fields, zap.Stringer(f.Name, f.Value))
	})
	log.Info("arguments", fields...)
}
