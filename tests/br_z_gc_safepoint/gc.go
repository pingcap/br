// Copyright 2019 PingCAP, Inc.
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

// Test backup with exceeding GC safe point.

package main

import (
	"context"
	"flag"
	"time"

	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
)

var (
	pdAddr   = flag.String("pd", "", "PD address")
	gcOffset = flag.Duration("gc-offset", time.Second*10,
		"Set GC safe point to current time - gc-offset, default: 10s")
	updateService = flag.Bool("update-service", false, "use new service to update min SafePoint")
)

func main() {
	flag.Parse()
	if *pdAddr == "" {
		log.Fatal("pd address is empty")
	}
	if *gcOffset == time.Duration(0) {
		log.Fatal("zero gc-offset is not allowed")
	}

	timeout := time.Second * 10
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	pdclient, err := pd.NewClientWithContext(ctx, []string{*pdAddr}, pd.SecurityOption{})
	if err != nil {
		log.Fatal("create pd client failed", zap.Error(err))
	}
	p, l, err := pdclient.GetTS(ctx)
	if err != nil {
		log.Fatal("get ts failed", zap.Error(err))
	}
	now := oracle.ComposeTS(p, l)
	nowMinusOffset := oracle.GetTimeFromTS(now).Add(-*gcOffset)
	newSP := oracle.ComposeTS(oracle.GetPhysical(nowMinusOffset), 0)
	if *updateService {
		_, err = pdclient.UpdateServiceGCSafePoint(ctx, "br", 300, newSP)
		if err != nil {
			log.Fatal("update service safe point failed", zap.Error(err))
		}
		log.Info("update service GC safe point", zap.Uint64("SP", newSP), zap.Uint64("now", now))
	} else {
		_, err = pdclient.UpdateGCSafePoint(ctx, newSP)
		if err != nil {
			log.Fatal("update safe point failed", zap.Error(err))
		}
		log.Info("update GC safe point", zap.Uint64("SP", newSP), zap.Uint64("now", now))
	}
}
