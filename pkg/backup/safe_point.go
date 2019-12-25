package backup

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/utils"
)

// GetGCSafePoint returns the current gc safe point.
// TODO: Some cluster may not enable distributed GC.
func GetGCSafePoint(ctx context.Context, pdClient pd.Client) (utils.Timestamp, error) {
	safePoint, err := pdClient.UpdateGCSafePoint(ctx, 0)
	if err != nil {
		return utils.Timestamp{}, err
	}
	return utils.DecodeTs(safePoint), nil
}

// CheckGCSafepoint checks whether the ts is older than GC safepoint.
// Note: It ignores errors other than exceed GC safepoint.
func CheckGCSafepoint(ctx context.Context, pdClient pd.Client, ts uint64) error {
	// TODO: use PDClient.GetGCSafePoint instead once PD client exports it.
	safePoint, err := GetGCSafePoint(ctx, pdClient)
	if err != nil {
		log.Warn("fail to get GC safe point", zap.Error(err))
		return nil
	}
	safePointTS := utils.EncodeTs(safePoint)
	if ts <= safePointTS {
		return errors.Errorf("GC safepoint %d exceed TS %d", safePointTS, ts)
	}
	return nil
}
