// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	splitRegionMaxRetryTime = 4
)

// SplitClient is an external client used by RegionSplitter.
type SplitClient interface {
	// GetStore gets a store by a store id.
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	// GetRegion gets a region which includes a specified key.
	GetRegion(ctx context.Context, key []byte) (*RegionInfo, error)
	// GetRegionByID gets a region by a region id.
	GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error)
	// SplitRegion splits a region from a key, if key is not included in the region, it will return nil.
	// note: the key should not be encoded
	SplitRegion(ctx context.Context, regionInfo *RegionInfo, key []byte) (*RegionInfo, error)
	// BatchSplitRegions splits a region from a batch of keys.
	// note: the keys should not be encoded
	BatchSplitRegions(ctx context.Context, regionInfo *RegionInfo, keys [][]byte) ([]*RegionInfo, error)
	// ScatterRegion scatters a specified region.
	ScatterRegion(ctx context.Context, regionInfo *RegionInfo) error
	// GetOperator gets the status of operator of the specified region.
	GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error)
	// ScanRegion gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned.
	ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error)
	// GetPlacementRule loads a placement rule from PD.
	GetPlacementRule(ctx context.Context, groupID, ruleID string) (placement.Rule, error)
	// SetPlacementRule insert or update a placement rule to PD.
	SetPlacementRule(ctx context.Context, rule placement.Rule) error
	// DeletePlacementRule removes a placement rule from PD.
	DeletePlacementRule(ctx context.Context, groupID, ruleID string) error
	// SetStoreLabel add or update specified label of stores. If labelValue
	// is empty, it clears the label.
	SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error
}

// pdClient is a wrapper of pd client, can be used by RegionSplitter.
type pdClient struct {
	mu         sync.Mutex
	client     pd.Client
	tlsConf    *tls.Config
	storeCache map[uint64]*metapb.Store
}

// NewSplitClient returns a client used by RegionSplitter.
func NewSplitClient(client pd.Client, tlsConf *tls.Config) SplitClient {
	return &pdClient{
		client:     client,
		tlsConf:    tlsConf,
		storeCache: make(map[uint64]*metapb.Store),
	}
}

func (c *pdClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	store, ok := c.storeCache[storeID]
	if ok {
		return store, nil
	}
	store, err := c.client.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	c.storeCache[storeID] = store
	return store, nil
}

func (c *pdClient) GetRegion(ctx context.Context, key []byte) (*RegionInfo, error) {
	region, err := c.client.GetRegion(ctx, key)
	if err != nil {
		return nil, err
	}
	if region == nil {
		return nil, nil
	}
	return &RegionInfo{
		Region: region.Meta,
		Leader: region.Leader,
	}, nil
}

func (c *pdClient) GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error) {
	region, err := c.client.GetRegionByID(ctx, regionID)
	if err != nil {
		return nil, err
	}
	if region == nil {
		return nil, nil
	}
	return &RegionInfo{
		Region: region.Meta,
		Leader: region.Leader,
	}, nil
}

func (c *pdClient) SplitRegion(ctx context.Context, regionInfo *RegionInfo, key []byte) (*RegionInfo, error) {
	var peer *metapb.Peer
	if regionInfo.Leader != nil {
		peer = regionInfo.Leader
	} else {
		if len(regionInfo.Region.Peers) == 0 {
			return nil, errors.New("region does not have peer")
		}
		peer = regionInfo.Region.Peers[0]
	}
	storeID := peer.GetStoreId()
	store, err := c.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.Dial(store.GetAddress(), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := tikvpb.NewTikvClient(conn)
	resp, err := client.SplitRegion(ctx, &kvrpcpb.SplitRegionRequest{
		Context: &kvrpcpb.Context{
			RegionId:    regionInfo.Region.Id,
			RegionEpoch: regionInfo.Region.RegionEpoch,
			Peer:        peer,
		},
		SplitKey: key,
	})
	if err != nil {
		return nil, err
	}
	if resp.RegionError != nil {
		return nil, errors.Errorf("split region failed: region=%v, key=%x, err=%v", regionInfo.Region, key, resp.RegionError)
	}

	// BUG: Left is deprecated, it may be nil even if split is succeed!
	// Assume the new region is the left one.
	newRegion := resp.GetLeft()
	if newRegion == nil {
		regions := resp.GetRegions()
		for _, r := range regions {
			if bytes.Equal(r.GetStartKey(), regionInfo.Region.GetStartKey()) {
				newRegion = r
				break
			}
		}
	}
	if newRegion == nil {
		return nil, errors.New("split region failed: new region is nil")
	}
	var leader *metapb.Peer
	// Assume the leaders will be at the same store.
	if regionInfo.Leader != nil {
		for _, p := range newRegion.GetPeers() {
			if p.GetStoreId() == regionInfo.Leader.GetStoreId() {
				leader = p
				break
			}
		}
	}
	return &RegionInfo{
		Region: newRegion,
		Leader: leader,
	}, nil
}

func splitRegionWithFailpoint(
	ctx context.Context,
	regionInfo *RegionInfo,
	peer *metapb.Peer,
	client tikvpb.TikvClient,
	keys [][]byte,
) (*kvrpcpb.SplitRegionResponse, error) {
	failpoint.Inject("not-leader-error", func(injectNewLeader failpoint.Value) {
		log.Debug("failpoint not-leader-error injected.")
		resp := &kvrpcpb.SplitRegionResponse{
			RegionError: &errorpb.Error{
				NotLeader: &errorpb.NotLeader{
					RegionId: regionInfo.Region.Id,
				},
			},
		}
		if injectNewLeader.(bool) {
			resp.RegionError.NotLeader.Leader = regionInfo.Leader
		}
		failpoint.Return(resp, nil)
	})
	failpoint.Inject("somewhat-retryable-error", func() {
		log.Debug("failpoint somewhat-retryable-error injected.")
		failpoint.Return(&kvrpcpb.SplitRegionResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil)
	})
	return client.SplitRegion(ctx, &kvrpcpb.SplitRegionRequest{
		Context: &kvrpcpb.Context{
			RegionId:    regionInfo.Region.Id,
			RegionEpoch: regionInfo.Region.RegionEpoch,
			Peer:        peer,
		},
		SplitKeys: keys,
	})
}

func (c *pdClient) sendSplitRegionRequest(
	ctx context.Context, regionInfo *RegionInfo, keys [][]byte,
) (*kvrpcpb.SplitRegionResponse, error) {
	var splitErrors error
	for i := 0; i < splitRegionMaxRetryTime; i++ {
		var peer *metapb.Peer
		if regionInfo.Leader != nil {
			peer = regionInfo.Leader
		} else {
			if len(regionInfo.Region.Peers) == 0 {
				return nil, multierr.Append(splitErrors,
					errors.Errorf("region[%d] doesn't have any peer", regionInfo.Region.GetId()))
			}
			peer = regionInfo.Region.Peers[0]
		}
		storeID := peer.GetStoreId()
		store, err := c.GetStore(ctx, storeID)
		if err != nil {
			return nil, multierr.Append(splitErrors, err)
		}
		opt := grpc.WithInsecure()
		if c.tlsConf != nil {
			opt = grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConf))
		}
		conn, err := grpc.Dial(store.GetAddress(), opt)
		if err != nil {
			return nil, multierr.Append(splitErrors, err)
		}
		defer conn.Close()
		client := tikvpb.NewTikvClient(conn)
		resp, err := splitRegionWithFailpoint(ctx, regionInfo, peer, client, keys)
		if err != nil {
			return nil, multierr.Append(splitErrors, err)
		}
		if resp.RegionError != nil {
			splitErrors = multierr.Append(splitErrors,
				errors.Errorf("split region failed: region=%v, err=%v",
					regionInfo.Region, resp.RegionError))
			if nl := resp.RegionError.NotLeader; nl != nil {
				if leader := nl.GetLeader(); leader != nil {
					regionInfo.Leader = leader
				} else {
					newRegionInfo, findLeaderErr := c.GetRegionByID(ctx, nl.RegionId)
					if findLeaderErr != nil {
						return nil, multierr.Append(splitErrors, findLeaderErr)
					}
					if !checkRegionEpoch(newRegionInfo, regionInfo) {
						return nil, multierr.Append(splitErrors, ErrEpochNotMatch)
					}
					log.Info("find new leader", zap.Uint64("new leader", newRegionInfo.Leader.Id))
					regionInfo = newRegionInfo
				}
				log.Info("split region meet not leader error, retrying",
					zap.Int("retry times", i),
					zap.Uint64("regionID", regionInfo.Region.Id),
					zap.Any("new leader", regionInfo.Leader),
				)
				continue
			}
			// TODO: we don't handle RegionNotMatch and RegionNotFound here,
			// because I think we don't have enough information to retry.
			// But maybe we can handle them here by some information the error itself provides.
			if resp.RegionError.ServerIsBusy != nil ||
				resp.RegionError.StaleCommand != nil {
				log.Warn("a error occurs on split region",
					zap.Int("retry times", i),
					zap.Uint64("regionID", regionInfo.Region.Id),
					zap.String("error", resp.RegionError.Message),
					zap.Any("error verbose", resp.RegionError),
				)
				continue
			}
			return nil, splitErrors
		}
		return resp, nil
	}
	return nil, splitErrors
}

func (c *pdClient) BatchSplitRegions(
	ctx context.Context, regionInfo *RegionInfo, keys [][]byte,
) ([]*RegionInfo, error) {
	resp, err := c.sendSplitRegionRequest(ctx, regionInfo, keys)
	if err != nil {
		return nil, err
	}

	regions := resp.GetRegions()
	newRegionInfos := make([]*RegionInfo, 0, len(regions))
	for _, region := range regions {
		// Skip the original region
		if region.GetId() == regionInfo.Region.GetId() {
			continue
		}
		var leader *metapb.Peer
		// Assume the leaders will be at the same store.
		if regionInfo.Leader != nil {
			for _, p := range region.GetPeers() {
				if p.GetStoreId() == regionInfo.Leader.GetStoreId() {
					leader = p
					break
				}
			}
		}
		newRegionInfos = append(newRegionInfos, &RegionInfo{
			Region: region,
			Leader: leader,
		})
	}
	return newRegionInfos, nil
}

func (c *pdClient) ScatterRegion(ctx context.Context, regionInfo *RegionInfo) error {
	return c.client.ScatterRegion(ctx, regionInfo.Region.GetId())
}

func (c *pdClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return c.client.GetOperator(ctx, regionID)
}

func (c *pdClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error) {
	regions, err := c.client.ScanRegions(ctx, key, endKey, limit)
	if err != nil {
		return nil, err
	}
	regionInfos := make([]*RegionInfo, 0, len(regions))
	for i := range regions {
		regionInfos = append(regionInfos, &RegionInfo{
			Region: regions[i].Meta,
			Leader: regions[i].Leader,
		})
	}
	return regionInfos, nil
}

func (c *pdClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (placement.Rule, error) {
	var rule placement.Rule
	addr := c.getPDAPIAddr()
	if addr == "" {
		return rule, errors.New("failed to add stores labels: no leader")
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", addr+path.Join("/pd/api/v1/config/rule", groupID, ruleID), nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return rule, errors.WithStack(err)
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return rule, errors.WithStack(err)
	}
	res.Body.Close()
	err = json.Unmarshal(b, &rule)
	if err != nil {
		return rule, errors.WithStack(err)
	}
	return rule, nil
}

func (c *pdClient) SetPlacementRule(ctx context.Context, rule placement.Rule) error {
	addr := c.getPDAPIAddr()
	if addr == "" {
		return errors.New("failed to add stores labels: no leader")
	}
	m, _ := json.Marshal(rule)
	req, _ := http.NewRequestWithContext(ctx, "POST", addr+path.Join("/pd/api/v1/config/rule"), bytes.NewReader(m))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.Trace(res.Body.Close())
}

func (c *pdClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	addr := c.getPDAPIAddr()
	if addr == "" {
		return errors.New("failed to add stores labels: no leader")
	}
	req, _ := http.NewRequestWithContext(ctx, "DELETE", addr+path.Join("/pd/api/v1/config/rule", groupID, ruleID), nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.Trace(res.Body.Close())
}

func (c *pdClient) SetStoresLabel(
	ctx context.Context, stores []uint64, labelKey, labelValue string,
) error {
	b := []byte(fmt.Sprintf(`{"%s": "%s"}`, labelKey, labelValue))
	addr := c.getPDAPIAddr()
	if addr == "" {
		return errors.New("failed to add stores labels: no leader")
	}
	for _, id := range stores {
		req, _ := http.NewRequestWithContext(
			ctx, "POST",
			addr+path.Join("/pd/api/v1/store", strconv.FormatUint(id, 10), "label"),
			bytes.NewReader(b),
		)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return errors.WithStack(err)
		}
		err = res.Body.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *pdClient) getPDAPIAddr() string {
	addr := c.client.GetLeaderAddr()
	if addr != "" && !strings.HasPrefix(addr, "http") {
		addr = "http://" + addr
	}
	return strings.TrimRight(addr, "/")
}
