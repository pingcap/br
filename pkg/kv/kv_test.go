// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type rowSuite struct{}

var _ = Suite(&rowSuite{})

func TestRow(t *testing.T) {
	TestingT(t)
}

func (s *rowSuite) TestMarshal(c *C) {
	dats := make([]types.Datum, 4)
	dats[0].SetInt64(1)
	dats[1].SetNull()
	dats[2] = types.MaxValueDatum()
	dats[3] = types.MinNotNullDatum()

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{zapRow("row", dats)})
	c.Assert(err, IsNil)
	c.Assert(strings.TrimRight(out.String(), "\n"), Equals,
		`{"row": ["kind: int64, val: 1", "kind: null, val: NULL", "kind: max, val: +inf", "kind: min, val: -inf"]}`)
}
