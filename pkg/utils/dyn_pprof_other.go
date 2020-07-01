// +build !linux,!darwin,!freebsd,!unix
// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

// StartDynamicPProfListener starts the listener that will enable pprof when received `startPProfSignal`
func StartDynamicPProfListener() {
	// nothing to do on no posix signal supporting systems.
}
