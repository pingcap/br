// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package log_test

import (
	. "github.com/pingcap/check"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/br/pkg/lightning/log"
)

var _ = Suite(&testFilterSuite{})

type testFilterSuite struct{}

func (s *testFilterSuite) TestFilter(c *C) {
	logger, buffer := log.MakeTestLogger()
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	c.Assert(
		buffer.Stripped(), Equals,
		`{"$lvl":"WARN","$msg":"the message","number":123456,"array":[7,8,9]}`,
	)

	logger, buffer = log.MakeTestLogger(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return log.NewFilterCore(c, "github.com/pingcap/br/")
	}), zap.AddCaller())
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	c.Assert(buffer.Stripped(), HasLen, 0)
}
