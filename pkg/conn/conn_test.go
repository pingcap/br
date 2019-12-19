package conn

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/pd/server/statistics"
)

func TestT(t *testing.T) {
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
	ctx := context.Background()
	mock := func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		stats := statistics.RegionStats{Count: 6}
		ret, err := json.Marshal(stats)
		c.Assert(err, IsNil)
		return ret, nil
	}
	s.mgr.pdHTTP.addrs = []string{""}
	resp, err := s.mgr.getRegionCountWith(ctx, mock, []byte{}, []byte{})
	c.Assert(err, IsNil)
	c.Assert(resp, Equals, 6)

	mock = func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return []byte(`test`), nil
	}
	respString, err := s.mgr.getClusterVersionWith(ctx, mock)
	c.Assert(err, IsNil)
	c.Assert(respString, Equals, "test")

	scheduler := "balance-leader-scheduler"
	mock = func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return nil, errors.New("failed")
	}
	err = s.mgr.removeSchedulerWith(ctx, scheduler, mock)
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
