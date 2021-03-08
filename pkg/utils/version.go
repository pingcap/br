// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"bytes"
	"context"
	"fmt"
	"golang.org/x/net/http/httpproxy"
	"regexp"
	"runtime"
	"strings"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/israce"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"

	berrors "github.com/pingcap/br/pkg/errors"
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

var (
	minTiKVVersion          = semver.New("3.1.0-beta.2")
	incompatibleTiKVMajor3  = semver.New("3.1.0")
	incompatibleTiKVMajor4  = semver.New("4.0.0-rc.1")
	compatibleTiFlashMajor3 = semver.New("3.1.0")
	compatibleTiFlashMajor4 = semver.New("4.0.0")
)

func removeVAndHash(v string) string {
	v = VersionHash.ReplaceAllLiteralString(v, "")
	v = strings.TrimSuffix(v, "-dirty")
	return strings.TrimPrefix(v, "v")
}

func checkTiFlashVersion(store *metapb.Store) error {
	flash, err := semver.NewVersion(removeVAndHash(store.Version))
	if err != nil {
		return errors.Annotatef(berrors.ErrVersionMismatch, "failed to parse TiFlash %s version %s, err %s",
			store.GetPeerAddress(), store.Version, err)
	}

	if flash.Major == 3 && flash.LessThan(*compatibleTiFlashMajor3) {
		return errors.Annotatef(berrors.ErrVersionMismatch, "incompatible TiFlash %s version %s, try update it to %s",
			store.GetPeerAddress(), store.Version, compatibleTiFlashMajor3)
	}

	if flash.Major == 4 && flash.LessThan(*compatibleTiFlashMajor4) {
		return errors.Annotatef(berrors.ErrVersionMismatch, "incompatible TiFlash %s version %s, try update it to %s",
			store.GetPeerAddress(), store.Version, compatibleTiFlashMajor4)
	}

	return nil
}

// IsTiFlash tests whether the store is based on tiflash engine.
func IsTiFlash(store *metapb.Store) bool {
	for _, label := range store.Labels {
		if label.Key == "engine" && label.Value == "tiflash" {
			return true
		}
	}
	return false
}

// CheckClusterVersion check TiKV version.
func CheckClusterVersion(ctx context.Context, client pd.Client) error {
	BRVersion, err := semver.NewVersion(removeVAndHash(BRReleaseVersion))
	if err != nil {
		return errors.Annotatef(berrors.ErrVersionMismatch, "%s: invalid BR version, please recompile using `git fetch origin --tags && make build`", err)
	}
	stores, err := client.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return errors.Trace(err)
	}
	for _, s := range stores {
		isTiFlash := IsTiFlash(s)
		log.Debug("checking compatibility of store in cluster",
			zap.Uint64("ID", s.GetId()),
			zap.Bool("TiFlash?", isTiFlash),
			zap.String("address", s.GetAddress()),
			zap.String("version", s.GetVersion()),
		)
		if isTiFlash {
			if err := checkTiFlashVersion(s); err != nil {
				return errors.Trace(err)
			}
		}

		tikvVersionString := removeVAndHash(s.Version)
		tikvVersion, err := semver.NewVersion(tikvVersionString)
		if err != nil {
			return errors.Annotatef(berrors.ErrVersionMismatch, "%s: TiKV node %s version %s is invalid", err, s.Address, tikvVersionString)
		}

		if tikvVersion.Compare(*minTiKVVersion) < 0 {
			return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s don't support BR, please upgrade cluster to %s",
				s.Address, tikvVersionString, BRReleaseVersion)
		}

		if tikvVersion.Major != BRVersion.Major {
			return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s and BR %s major version mismatch, please use the same version of BR",
				s.Address, tikvVersionString, BRReleaseVersion)
		}

		// BR(https://github.com/pingcap/br/pull/233) and TiKV(https://github.com/tikv/tikv/pull/7241) have breaking changes
		// if BR include #233 and TiKV not include #7241, BR will panic TiKV during restore
		// These incompatible version is 3.1.0 and 4.0.0-rc.1
		if tikvVersion.Major == 3 {
			if tikvVersion.Compare(*incompatibleTiKVMajor3) < 0 && BRVersion.Compare(*incompatibleTiKVMajor3) >= 0 {
				return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s and BR %s version mismatch, please use the same version of BR",
					s.Address, tikvVersionString, BRReleaseVersion)
			}
		}

		if tikvVersion.Major == 4 {
			if tikvVersion.Compare(*incompatibleTiKVMajor4) < 0 && BRVersion.Compare(*incompatibleTiKVMajor4) >= 0 {
				return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s and BR %s version mismatch, please use the same version of BR",
					s.Address, tikvVersionString, BRReleaseVersion)
			}
		}

		// don't warn if we are the master build, which always have the version v4.0.0-beta.2-*
		if BRGitBranch != "master" && tikvVersion.Compare(*BRVersion) > 0 {
			log.Warn(fmt.Sprintf("BR version is outdated, please consider use version %s of BR", tikvVersionString))
			break
		}
	}
	return nil
}

// LogEnvVariables logs related environment variables.
func LogEnvVariables() {
	// log http proxy settings, it will be used in gRPC connection by default
	proxyCfg := httpproxy.FromEnvironment()
	fields := make([]zap.Field, 0, 3)
	if proxyCfg.HTTPProxy != "" {
		fields = append(fields, zap.String("http_proxy", proxyCfg.HTTPProxy))
	}
	if proxyCfg.HTTPSProxy != "" {
		fields = append(fields, zap.String("https_proxy", proxyCfg.HTTPSProxy))
	}
	if proxyCfg.NoProxy != "" {
		fields = append(fields, zap.String("no_proxy", proxyCfg.NoProxy))
	}
	if len(fields) > 0 {
		log.Info("using proxy config", fields...)
	}
}
