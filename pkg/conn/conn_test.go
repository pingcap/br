package conn

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/statistics"
	"github.com/pingcap/tidb/util/codec"
)

func TestT(t *testing.T) {
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
	ctx := context.Background()

	s.mgr.pdHTTP.addrs = []string{"", ""} // two endpoints
	counter := 0
	mock := func(context.Context, string, string, *http.Client) ([]byte, error) {
		counter++
		if counter <= 1 {
			return nil, errors.New("mock error")
		}
		return []byte(`test`), nil
	}
	respString, err := s.mgr.getClusterVersionWith(ctx, mock)
	c.Assert(err, IsNil)
	c.Assert(respString, Equals, "test")

	mock = func(context.Context, string, string, *http.Client) ([]byte, error) {
		return nil, errors.New("mock error")
	}
	_, err = s.mgr.getClusterVersionWith(ctx, mock)
	c.Assert(err, NotNil)
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

	ctx := context.Background()
	mock := func(_ context.Context, addr string, prefix string, _ *http.Client) ([]byte, error) {
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
