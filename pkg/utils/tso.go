package utils

import (
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// Timestamp is composed by a physical unix timestamp and a logical timestamp.
type Timestamp struct {
	Physical int64
	Logical  int64
}

const physicalShiftBits = 18

// DecodeTs decodes Timestamp from a uint64
func DecodeTs(ts uint64) Timestamp {
	physical := oracle.ExtractPhysical(ts)
	logical := ts - (uint64(physical) << physicalShiftBits)
	return Timestamp{
		Physical: physical,
		Logical:  int64(logical),
	}
}

// EncodeTs encodes Timestamp into a uint64
func EncodeTs(tp Timestamp) uint64 {
	return uint64((tp.Physical << physicalShiftBits) + tp.Logical)
}
