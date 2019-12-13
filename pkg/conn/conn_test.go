package conn

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server/statistics"
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

	mgr *Mgr
}

func (s *testClientSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.mgr = &Mgr{}
}

func (s *testClientSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testClientSuite) TestPDHTTP(c *C) {
	s.mgr.PDHTTPGet = func(string, string, *http.Client) ([]byte, error) {
		stats := statistics.RegionStats{Count: 6}
		ret, err := json.Marshal(stats)
		c.Assert(err, IsNil)
		return ret, nil
	}
	s.mgr.pdHTTP.addrs = []string{""}
	resp, err := s.mgr.GetRegionCount([]byte{}, []byte{})
	c.Assert(err, IsNil)
	c.Assert(resp, Equals, 6)

	s.mgr.PDHTTPGet = func(string, string, *http.Client) ([]byte, error) {
		return []byte(`test`), nil
	}
	respString, err := s.mgr.GetClusterVersion()
	c.Assert(err, IsNil)
	c.Assert(respString, Equals, "test")
}
