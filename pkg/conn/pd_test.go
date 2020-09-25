package conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	. "github.com/pingcap/check"
)

type testPDControllerSuite struct {
}

var _ = Suite(&testPDControllerSuite{})

func (s *testPDControllerSuite) TestScheduler(c *C) {
	ctx := context.Background()

	scheduler := "balance-leader-scheduler"
	mock := func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return nil, errors.New("failed")
	}
	pdController := &PdController{addrs: []string{"", ""}}
	err := pdController.removeSchedulerWith(ctx, scheduler, mock)
	fmt.Printf("err: %v\n", err)
	c.Assert(err, ErrorMatches, "failed")

	err = pdController.addSchedulerWith(ctx, scheduler, mock)
	c.Assert(err, ErrorMatches, "failed")

	_, err = pdController.listSchedulersWith(ctx, mock)
	c.Assert(err, ErrorMatches, "failed")

	mock = func(context.Context, string, string, *http.Client, string, io.Reader) ([]byte, error) {
		return []byte(`["` + scheduler + `"]`), nil
	}
	err = pdController.removeSchedulerWith(ctx, scheduler, mock)
	c.Assert(err, IsNil)

	err = pdController.addSchedulerWith(ctx, scheduler, mock)
	c.Assert(err, IsNil)

	schedulers, err := pdController.listSchedulersWith(ctx, mock)
	c.Assert(err, IsNil)
	c.Assert(schedulers, HasLen, 1)
	c.Assert(schedulers[0], Equals, scheduler)
}
