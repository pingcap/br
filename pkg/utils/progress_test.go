package utils

import (
	"context"
	"strings"

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
	defer cancel()

	var p string
	pCh := make(chan string, 2)
	progress := NewProgressPrinter("test", 2, false)
	progress.goPrintProgress(ctx, &testWriter{
		fn: func(p string) { pCh <- p },
	})
	updateCh := progress.UpdateCh()
	updateCh <- struct{}{}
	p = <-pCh
	c.Assert(strings.Contains(p, "50"), IsTrue, Commentf("%s", p))
	updateCh <- struct{}{}
	p = <-pCh
	c.Assert(strings.Contains(p, "100"), IsTrue, Commentf("%s", p))
	updateCh <- struct{}{}
	p = <-pCh
	c.Assert(strings.Contains(p, "100"), IsTrue, Commentf("%s", p))
}
