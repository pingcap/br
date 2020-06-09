// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// MinInt choice smallest integer from its arguments.
func MinInt(x int, xs ...int) int {
	min := x
	for _, n := range xs {
		if n < min {
			min = n
		}
	}
	return min
}

// MaxInt choice biggest integer from its arguments.
func MaxInt(x int, xs ...int) int {
	max := x
	for _, n := range xs {
		if n > max {
			max = n
		}
	}
	return max
}

// ClampInt restrict a value to a certain interval.
func ClampInt(n, min, max int) int {
	if min > max {
		log.Error("clamping integer with min > max", zap.Int("min", min), zap.Int("max", max))
	}

	return MinInt(max, MaxInt(min, n))
}
