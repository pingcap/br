// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package rtree

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/br/pkg/redact"
)

// String formats a range to a string.
func (rg *Range) String() string {
	return fmt.Sprintf("[%s %s)", redact.Key(rg.StartKey), redact.Key(rg.EndKey))
}

// ZapRanges make zap fields for logging Range slice.
func ZapRanges(ranges []Range) zapcore.Field {
	return zap.Object("ranges", rangesMarshaler(ranges))
}

type rangesMarshaler []Range

func (rs rangesMarshaler) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, r := range rs {
		encoder.AppendString(r.String())
	}
	return nil
}

func (rs rangesMarshaler) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	total := len(rs)
	encoder.AddInt("total", total)
	if total <= 4 {
		_ = encoder.AddArray("ranges", rs)
	} else {
		encoder.AddString("ranges", fmt.Sprintf("%s ...(skip %d)... %s",
			&rs[0], total-2, &rs[total-1]))
	}

	totalKV := uint64(0)
	totalSize := uint64(0)
	totalFile := 0
	for _, r := range rs {
		for _, f := range r.Files {
			totalKV += f.GetTotalKvs()
			totalSize += f.GetTotalBytes()
		}
		totalFile += len(r.Files)
	}

	encoder.AddInt("total file", totalFile)
	encoder.AddUint64("total kv", totalKV)
	encoder.AddUint64("total size", totalSize)
	return nil
}
