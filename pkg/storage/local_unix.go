// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

// +build !windows

package storage

import (
	"os"
	"syscall"
)

func mkdirAll(base string) error {
	mask := syscall.Umask(0)
	err := os.MkdirAll(base, 0755)
	syscall.Umask(mask)
	return err
}
