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
	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/util/israce"
	"go.uber.org/zap"
)

// Version information.
var (
	BRReleaseVersion = "None"
	BRBuildTS        = "None"
	BRGitHash        = "None"
	BRGitBranch      = "None"
	goVersion        = runtime.Version()
	VersionHash      = regexp.MustCompile("-[0-9]+-g[0-9a-f]{7,}")
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

var minTiKVVersion *semver.Version = semver.New("3.1.0-beta.2")
var incompatibleTiKVMajor3 *semver.Version = semver.New("3.1.0")
var incompatibleTiKVMajor4 *semver.Version = semver.New("4.0.0-rc.1")

func removeVAndHash(v string) string {
	v = VersionHash.ReplaceAllLiteralString(v, "")
	v = strings.TrimSuffix(v, "-dirty")
	return strings.TrimPrefix(v, "v")
}

// CheckClusterVersion check TiKV version.
func CheckClusterVersion(ctx context.Context, client pd.Client) error {
	BRVersion, err := semver.NewVersion(removeVAndHash(BRReleaseVersion))
	if err != nil {
		return berrors.ErrVersionMismatch.FastGen("%s: invalid BR version, please recompile using `git fetch origin --tags && make build`", err)
	}
	stores, err := client.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return errors.Trace(err)
	}
	for _, s := range stores {
		tikvVersionString := removeVAndHash(s.Version)
		tikvVersion, err := semver.NewVersion(tikvVersionString)
		if err != nil {
			return berrors.ErrVersionMismatch.FastGen("%s: TiKV node %s version %s is invalid", err, s.Address, tikvVersionString)
		}

		if tikvVersion.Compare(*minTiKVVersion) < 0 {
			return berrors.ErrVersionMismatch.FastGen("TiKV node %s version %s don't support BR, please upgrade cluster to %s",
				s.Address, tikvVersionString, BRReleaseVersion)
		}

		if tikvVersion.Major != BRVersion.Major {
			return berrors.ErrVersionMismatch.FastGen("TiKV node %s version %s and BR %s major version mismatch, please use the same version of BR",
				s.Address, tikvVersionString, BRReleaseVersion)
		}

		// BR(https://github.com/pingcap/br/pull/233) and TiKV(https://github.com/tikv/tikv/pull/7241) have breaking changes
		// if BR include #233 and TiKV not include #7241, BR will panic TiKV during restore
		// These incompatible version is 3.1.0 and 4.0.0-rc.1
		if tikvVersion.Major == 3 {
			if tikvVersion.Compare(*incompatibleTiKVMajor3) < 0 && BRVersion.Compare(*incompatibleTiKVMajor3) >= 0 {
				return berrors.ErrVersionMismatch.FastGen("TiKV node %s version %s and BR %s version mismatch, please use the same version of BR",
					s.Address, tikvVersionString, BRReleaseVersion)
			}
		}

		if tikvVersion.Major == 4 {
			if tikvVersion.Compare(*incompatibleTiKVMajor4) < 0 && BRVersion.Compare(*incompatibleTiKVMajor4) >= 0 {
				return berrors.ErrVersionMismatch.FastGen("TiKV node %s version %s and BR %s version mismatch, please use the same version of BR",
					s.Address, tikvVersionString, BRReleaseVersion)
			}
		}

		// don't warn if we are the master build, which always have the version v4.0.0-beta.2-*
		if BRGitBranch != "master" && tikvVersion.Compare(*BRVersion) > 0 {
			log.Warn(fmt.Sprintf("BR version is too old, please consider use version %s of BR", tikvVersionString))
			break
		}
	}
	return nil
}
