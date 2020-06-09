// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"runtime"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/v3/client"
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

var minTiKVVersion *semver.Version = semver.New("3.1.0-beta.2")
var incompatibleTiKVMajor3 *semver.Version = semver.New("3.1.0")
var incompatibleTiKVMajor4 *semver.Version = semver.New("4.0.0-rc.1")

func removeVAndHash(v string) string {
	hash := regexp.MustCompile("-[0-9]+-g[0-9a-f]{8,}")
	if hash.Match([]byte(v)) {
		v = hash.ReplaceAllLiteralString(v, "")
	}
	return strings.TrimPrefix(v, "v")
}

// CheckClusterVersion check TiKV version.
func CheckClusterVersion(ctx context.Context, client pd.Client) error {
	BRVersion, err := semver.NewVersion(removeVAndHash(BRReleaseVersion))
	if err != nil {
		return err
	}
	stores, err := client.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return err
	}
	for _, s := range stores {
		tikvVersion, err := semver.NewVersion(removeVAndHash(s.Version))
		if err != nil {
			return err
		}

		if tikvVersion.Compare(*minTiKVVersion) < 0 {
			return errors.Errorf("TiKV node %s version %s don't support BR, please upgrade cluster to %s",
				s.Address, removeVAndHash(s.Version), BRReleaseVersion)
		}

		if tikvVersion.Major != BRVersion.Major {
			return errors.Errorf("TiKV node %s version %s and BR %s major version mismatch, please use the same version of BR",
				s.Address, removeVAndHash(s.Version), BRReleaseVersion)
		}

		// BR(https://github.com/pingcap/br/pull/233) and TiKV(https://github.com/tikv/tikv/pull/7241) have breaking changes
		// if BR include #233 and TiKV not include #7241, BR will panic TiKV during restore
		// These incompatible version is 3.1.0 and 4.0.0-rc.1
		if tikvVersion.Major == 3 {
			if tikvVersion.Compare(*incompatibleTiKVMajor3) < 0 && BRVersion.Compare(*incompatibleTiKVMajor3) >= 0 {
				return errors.Errorf("TiKV node %s version %s and BR %s version mismatch, please use the same version of BR",
					s.Address, removeVAndHash(s.Version), BRReleaseVersion)
			}
		}

		if tikvVersion.Major == 4 {
			if tikvVersion.Compare(*incompatibleTiKVMajor4) < 0 && BRVersion.Compare(*incompatibleTiKVMajor4) >= 0 {
				return errors.Errorf("TiKV node %s version %s and BR %s version mismatch, please use the same version of BR",
					s.Address, removeVAndHash(s.Version), BRReleaseVersion)
			}
		}

		if tikvVersion.Compare(*BRVersion) > 0 {
			log.Warn(fmt.Sprintf("BR version is too old, please consider use version %s of BR", removeVAndHash(s.Version)))
			break
		}
	}
	return nil
}
