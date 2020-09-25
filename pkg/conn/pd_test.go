package conn

import (
	"context"
	"errors"
	"io"
	"net/http"

	. "github.com/pingcap/check"
)

type testPdMgrSuite struct {
}

var _ = Suite(&testPdMgrSuite{})

func (s *testPdMgrSuite) TestScheduler(c *C) {
	ctx := context.Background()

	scheduler := "balance-leader-scheduler"
	mock := func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return nil, errors.New("failed")
	}
	pdMgr := &PdController{}
	err := pdMgr.removeSchedulerWith(ctx, scheduler, mock)
	c.Assert(err, ErrorMatches, "failed")

	err = pdMgr.addSchedulerWith(ctx, scheduler, mock)
	c.Assert(err, ErrorMatches, "failed")

	_, err = pdMgr.listSchedulersWith(ctx, mock)
	c.Assert(err, ErrorMatches, "failed")

	mock = func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return []byte(`["` + scheduler + `"]`), nil
	}
	err = pdMgr.removeSchedulerWith(ctx, scheduler, mock)
	c.Assert(err, IsNil)

	err = pdMgr.addSchedulerWith(ctx, scheduler, mock)
	c.Assert(err, IsNil)

	schedulers, err := pdMgr.listSchedulersWith(ctx, mock)
	c.Assert(err, IsNil)
	c.Assert(schedulers, HasLen, 1)
	c.Assert(schedulers[0], Equals, scheduler)
}
