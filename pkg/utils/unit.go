// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

const (
	// B is number of bytes in one byte.
	B = uint64(1) << (iota * 10)
	// KB is number of bytes in one kibibyte.
	KB
	// MB is number of bytes in one mebibyte.
	MB
	// GB is number of bytes in one gibibyte.
	GB
	// TB is number of bytes in one tebibyte.
	TB
)
