// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package rtree_test

import (
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	backuppb "github.com/pingcap/kvproto/pkg/backup"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/br/pkg/rtree"
)

var _ = Suite(&testLoggingSuite{})

type testLoggingSuite struct{}

func (s *testLoggingSuite) TestLogRanges(c *C) {
	cases := []struct {
		count  int
		expect string
	}{
		{0, `{"ranges": {"total": 0, "ranges": [], "file-count": 0, "kv-paris-count": 0, "data-size": "0B", "after-compress-size": "0B"}}`},
		{1, `{"ranges": {"total": 1, "ranges": ["[30, 31)"], "file-count": 1, "kv-paris-count": 0, "data-size": "0B", "after-compress-size": "0B"}}`},
		{2, `{"ranges": {"total": 2, "ranges": ["[30, 31)", "[31, 32)"], "file-count": 2, "kv-paris-count": 1, "data-size": "1B", "after-compress-size": "0B"}}`},
		{3, `{"ranges": {"total": 3, "ranges": ["[30, 31)", "[31, 32)", "[32, 33)"], "file-count": 3, "kv-paris-count": 3, "data-size": "3B", "after-compress-size": "0B"}}`},
		{4, `{"ranges": {"total": 4, "ranges": ["[30, 31)", "[31, 32)", "[32, 33)", "[33, 34)"], "file-count": 4, "kv-paris-count": 6, "data-size": "6B", "after-compress-size": "0B"}}`},
		{5, `{"ranges": {"total": 5, "ranges": ["[30, 31)", "(skip 3)", "[34, 35)"], "file-count": 5, "kv-paris-count": 10, "data-size": "10B", "after-compress-size": "0B"}}`},
		{6, `{"ranges": {"total": 6, "ranges": ["[30, 31)", "(skip 4)", "[35, 36)"], "file-count": 6, "kv-paris-count": 15, "data-size": "15B", "after-compress-size": "0B"}}`},
		{1024, `{"ranges": {"total": 1024, "ranges": ["[30, 31)", "(skip 1022)", "[31303233, 31303234)"], "file-count": 1024, "kv-paris-count": 523776, "data-size": "523.8kB", "after-compress-size": "0B"}}`}}

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	for _, cs := range cases {
		ranges := make([]rtree.Range, cs.count)
		for j := 0; j < cs.count; j++ {
			ranges[j] = *newRange([]byte(fmt.Sprintf("%d", j)), []byte(fmt.Sprintf("%d", j+1)))
			ranges[j].Files = append(ranges[j].Files, &backuppb.File{TotalKvs: uint64(j), TotalBytes: uint64(j)})
		}
		out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{rtree.ZapRanges(ranges)})
		c.Assert(err, IsNil)
		c.Assert(strings.TrimRight(out.String(), "\n"), Equals, cs.expect)
	}
}
