package backup

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// Backer backups a TiDB/TiKV cluster.
type Backer struct {
	ctx      context.Context
	pdClient pd.Client
}

// NewBacker creates a new Backer.
func NewBacker(ctx context.Context, pdAddrs string) (*Backer, error) {
	addrs := strings.Split(pdAddrs, ",")
	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Backer{
		ctx:      ctx,
		pdClient: pdClient,
	}, nil
}

// GetClusterVersion returns the current cluster version.
func (backer *Backer) GetClusterVersion() string {
	// TODO return the actual cluster version.
	return "v3.0.0"
}

// GetGCSaftPoint returns the current gc safe point.
func (backer *Backer) GetGCSaftPoint() (Timestamp, error) {
	safePoint, err := backer.pdClient.UpdateGCSafePoint(backer.ctx, 0)
	println(safePoint)
	if err != nil {
		return Timestamp{}, errors.Trace(err)
	}
	return decodeTs(safePoint), nil
}

const physicalShiftBits = 18

func decodeTs(ts uint64) Timestamp {
	physical := oracle.ExtractPhysical(ts)
	logical := ts - (uint64(physical) << physicalShiftBits)
	return Timestamp{
		Physical: physical,
		Logical:  int64(logical),
	}
}
