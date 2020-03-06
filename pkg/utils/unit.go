// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

// unit of storage
const (
	B = uint64(1) << (iota * 10)
	KB
	MB
	GB
	TB
)
