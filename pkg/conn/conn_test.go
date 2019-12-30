package conn

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/statistics"
	"github.com/pingcap/tidb/util/codec"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

type testClientSuite struct {
	ctx    context.Context
	cancel context.CancelFunc

	mgr     *Mgr
	regions *core.RegionsInfo
}

func (s *testClientSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.mgr = &Mgr{}
	s.regions = core.NewRegionsInfo()
}

func (s *testClientSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testClientSuite) TestGetClusterVersion(c *C) {
	s.mgr.pdHTTP.addrs = []string{"", ""} // two endpoints
	counter := 0
	mock := func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		counter++
		if counter <= 1 {
			return nil, errors.New("mock error")
		}
		return []byte(`test`), nil
	}

	ctx := context.Background()
	respString, err := s.mgr.getClusterVersionWith(ctx, mock)
	c.Assert(err, IsNil)
	c.Assert(respString, Equals, "test")

	mock = func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return nil, errors.New("mock error")
	}
	_, err = s.mgr.getClusterVersionWith(ctx, mock)
	c.Assert(err, NotNil)
}

func (s *testClientSuite) TestScheduler(c *C) {
	ctx := context.Background()

	scheduler := "balance-leader-scheduler"
	mock := func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return nil, errors.New("failed")
	}
	err := s.mgr.removeSchedulerWith(ctx, scheduler, mock)
	c.Assert(err, ErrorMatches, "failed")

	err = s.mgr.addSchedulerWith(ctx, scheduler, mock)
	c.Assert(err, ErrorMatches, "failed")

	_, err = s.mgr.listSchedulersWith(ctx, mock)
	c.Assert(err, ErrorMatches, "failed")

	mock = func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return []byte(`["` + scheduler + `"]`), nil
	}
	err = s.mgr.removeSchedulerWith(ctx, scheduler, mock)
	c.Assert(err, IsNil)

	err = s.mgr.addSchedulerWith(ctx, scheduler, mock)
	c.Assert(err, IsNil)

	schedulers, err := s.mgr.listSchedulersWith(ctx, mock)
	c.Assert(err, IsNil)
	c.Assert(schedulers, HasLen, 1)
	c.Assert(schedulers[0], Equals, scheduler)
}

func (s *testClientSuite) TestRegionCount(c *C) {
	s.regions.SetRegion(core.NewRegionInfo(&metapb.Region{
		Id:          1,
		StartKey:    codec.EncodeBytes(nil, []byte{1, 1}),
		EndKey:      codec.EncodeBytes(nil, []byte{1, 3}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	s.regions.SetRegion(core.NewRegionInfo(&metapb.Region{
		Id:          2,
		StartKey:    codec.EncodeBytes(nil, []byte{1, 3}),
		EndKey:      codec.EncodeBytes(nil, []byte{1, 5}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	s.regions.SetRegion(core.NewRegionInfo(&metapb.Region{
		Id:          3,
		StartKey:    codec.EncodeBytes(nil, []byte{2, 3}),
		EndKey:      codec.EncodeBytes(nil, []byte{3, 4}),
		RegionEpoch: &metapb.RegionEpoch{},
	}, nil))
	c.Assert(s.regions.Length(), Equals, 3)

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
		regions := s.regions.ScanRange([]byte(start), []byte(end), 0)
		stats := statistics.RegionStats{Count: len(regions)}
		ret, err := json.Marshal(stats)
		c.Assert(err, IsNil)
		return ret, nil
	}
	s.mgr.pdHTTP.addrs = []string{"http://mock"}
	ctx := context.Background()
	resp, err := s.mgr.getRegionCountWith(ctx, mock, []byte{}, []byte{})
	c.Assert(err, IsNil)
	c.Assert(resp, Equals, 3)

	resp, err = s.mgr.getRegionCountWith(ctx, mock, []byte{0}, []byte{0xff})
	c.Assert(err, IsNil)
	c.Assert(resp, Equals, 3)

	resp, err = s.mgr.getRegionCountWith(ctx, mock, []byte{1, 2}, []byte{1, 4})
	c.Assert(err, IsNil)
	c.Assert(resp, Equals, 2)
}
