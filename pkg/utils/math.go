// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

// MinInt choice smaller integer from its two arguments.
func MinInt(x, y int) int {
	if x < y {
		return x
	}

	return y
}
