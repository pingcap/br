// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"

	. "github.com/pingcap/check"
)

type testProgressSuite struct{}

var _ = Suite(&testProgressSuite{})

type testWriter struct {
	fn func(string)
}

func (t *testWriter) Write(p []byte) (int, error) {
	t.fn(string(p))
	return len(p), nil
}

func (r *testProgressSuite) TestProgress(c *C) {
	ctx, cancel := context.WithCancel(context.Background())

	var p string
	pCh2 := make(chan string, 2)
	progress2 := NewProgressPrinter("test", 2, false)
	progress2.goPrintProgress(ctx, &testWriter{
		fn: func(p string) { pCh2 <- p },
	})
	updateCh2 := progress2.UpdateCh()
	updateCh2 <- struct{}{}
	p = <-pCh2
	c.Assert(p, Matches, ".*50.*")
	updateCh2 <- struct{}{}
	p = <-pCh2
	c.Assert(p, Matches, ".*100.*")
	updateCh2 <- struct{}{}
	p = <-pCh2
	c.Assert(p, Matches, ".*100.*")

	pCh4 := make(chan string, 4)
	progress4 := NewProgressPrinter("test", 4, false)
	progress4.goPrintProgress(ctx, &testWriter{
		fn: func(p string) { pCh4 <- p },
	})
	updateCh4 := progress4.UpdateCh()
	updateCh4 <- struct{}{}
	p = <-pCh4
	c.Assert(p, Matches, ".*25.*")
	updateCh4 <- struct{}{}
	close(updateCh4)
	p = <-pCh4
	c.Assert(p, Matches, ".*100.*")

	pCh8 := make(chan string, 8)
	progress8 := NewProgressPrinter("test", 8, false)
	progress8.goPrintProgress(ctx, &testWriter{
		fn: func(p string) { pCh8 <- p },
	})
	updateCh8 := progress8.UpdateCh()
	updateCh8 <- struct{}{}
	updateCh8 <- struct{}{}
	<-pCh8
	p = <-pCh8
	c.Assert(p, Matches, ".*25.*")

	// Cancel should stop progress at the current position.
	cancel()
	p = <-pCh8
	c.Assert(p, Matches, ".*25.*")
}
