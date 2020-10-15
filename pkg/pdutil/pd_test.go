// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package pdutil

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/util/codec"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/statistics"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testPDControllerSuite struct {
}

var _ = Suite(&testPDControllerSuite{})

func (s *testPDControllerSuite) TestScheduler(c *C) {
	ctx := context.Background()

	scheduler := "balance-leader-scheduler"
	mock := func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return nil, errors.New("failed")
	}
	schedulerPauseCh := make(chan struct{})
	pdController := &PdController{addrs: []string{"", ""}, schedulerPauseCh: schedulerPauseCh}
	_, err := pdController.pauseSchedulersWith(ctx, []string{scheduler}, mock)
	c.Assert(err, ErrorMatches, "failed")

	go func() {
		<-schedulerPauseCh
	}()
	err = pdController.resumeSchedulerWith(ctx, []string{scheduler}, mock)
	c.Assert(err, ErrorMatches, "failed")

	_, err = pdController.listSchedulersWith(ctx, mock)
	c.Assert(err, ErrorMatches, "failed")

	mock = func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return []byte(`["` + scheduler + `"]`), nil
	}
	_, err = pdController.pauseSchedulersWith(ctx, []string{scheduler}, mock)
	c.Assert(err, IsNil)

	go func() {
		<-schedulerPauseCh
	}()
	err = pdController.resumeSchedulerWith(ctx, []string{scheduler}, mock)
	c.Assert(err, IsNil)

	schedulers, err := pdController.listSchedulersWith(ctx, mock)
	c.Assert(err, IsNil)
	c.Assert(schedulers, HasLen, 1)
	c.Assert(schedulers[0], Equals, scheduler)
}

func (s *testPDControllerSuite) TestGetClusterVersion(c *C) {
	pdController := &PdController{addrs: []string{"", ""}} // two endpoints
	counter := 0
	mock := func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		counter++
		if counter <= 1 {
			return nil, errors.New("mock error")
		}
		return []byte(`test`), nil
	}

	ctx := context.Background()
	respString, err := pdController.getClusterVersionWith(ctx, mock)
	c.Assert(err, IsNil)
	c.Assert(respString, Equals, "test")

	mock = func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return nil, errors.New("mock error")
	}
	_, err = pdController.getClusterVersionWith(ctx, mock)
	c.Assert(err, NotNil)
}

func (s *testPDControllerSuite) TestRegionCount(c *C) {
	regions := core.NewRegionsInfo()
	regions.SetRegion(core.NewRegionInfo(&metapb.Region{
		Id:          1,
		StartKey:    codec.EncodeBytes(nil, []byte{1, 1}),
		EndKey:      codec.EncodeBytes(nil, []byte{1, 3}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	regions.SetRegion(core.NewRegionInfo(&metapb.Region{
		Id:          2,
		StartKey:    codec.EncodeBytes(nil, []byte{1, 3}),
		EndKey:      codec.EncodeBytes(nil, []byte{1, 5}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	regions.SetRegion(core.NewRegionInfo(&metapb.Region{
		Id:          3,
		StartKey:    codec.EncodeBytes(nil, []byte{2, 3}),
		EndKey:      codec.EncodeBytes(nil, []byte{3, 4}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	c.Assert(regions.Length(), Equals, 3)

	mock := func(
		_ context.Context, addr string, prefix string, _ *http.Client, _ string, _ io.Reader,
	) ([]byte, error) {
		query := fmt.Sprintf("%s/%s", addr, prefix)
		u, e := url.Parse(query)
		c.Assert(e, IsNil, Commentf("%s", query))
		start := u.Query().Get("start_key")
		end := u.Query().Get("end_key")
		c.Log(hex.EncodeToString([]byte(start)))
		c.Log(hex.EncodeToString([]byte(end)))
		scanRegions := regions.ScanRange([]byte(start), []byte(end), 0)
		stats := statistics.RegionStats{Count: len(scanRegions)}
		ret, err := json.Marshal(stats)
		c.Assert(err, IsNil)
		return ret, nil
	}

	pdController := &PdController{addrs: []string{"http://mock"}}
	ctx := context.Background()
	resp, err := pdController.getRegionCountWith(ctx, mock, []byte{}, []byte{})
	c.Assert(err, IsNil)
	c.Assert(resp, Equals, 3)

	resp, err = pdController.getRegionCountWith(ctx, mock, []byte{0}, []byte{0xff})
	c.Assert(err, IsNil)
	c.Assert(resp, Equals, 3)

	resp, err = pdController.getRegionCountWith(ctx, mock, []byte{1, 2}, []byte{1, 4})
	c.Assert(err, IsNil)
	c.Assert(resp, Equals, 2)
}
