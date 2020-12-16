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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
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

	// save coverage for MarshalLogArray
	log.Info("row marshal", zap.Array("row", rowArrayMarshaler(dats)))
}
