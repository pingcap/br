// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"context"
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	kvproto "github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	pd "github.com/tikv/pd/client"

	"github.com/pingcap/br/pkg/backup"
	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/pdutil"
)

type testBackup struct {
	ctx    context.Context
	cancel context.CancelFunc

	mockPDClient pd.Client
	backupClient *backup.Client
}

var _ = Suite(&testBackup{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (r *testBackup) SetUpSuite(c *C) {
	r.mockPDClient = mocktikv.NewPDClient(mocktikv.NewCluster())
	r.ctx, r.cancel = context.WithCancel(context.Background())
	mockMgr := &conn.Mgr{PdController: &pdutil.PdController{}}
	mockMgr.SetPDClient(r.mockPDClient)
	mockMgr.SetHTTP([]string{"test"}, nil)
	var err error
	r.backupClient, err = backup.NewBackupClient(r.ctx, mockMgr)
	c.Assert(err, IsNil)
}

func (r *testBackup) TestGetTS(c *C) {
	var (
		err error
		// mockPDClient' physical ts and current ts will have deviation
		// so make this deviation tolerance 100ms
		deviation = 100
	)

	// timeago not work
	expectedDuration := 0
	currentTS := time.Now().UnixNano() / int64(time.Millisecond)
	ts, err := r.backupClient.GetTS(r.ctx, 0, 0)
	c.Assert(err, IsNil)
	pdTS := oracle.ExtractPhysical(ts)
	duration := int(currentTS - pdTS)
	c.Assert(duration, Greater, expectedDuration-deviation)
	c.Assert(duration, Less, expectedDuration+deviation)

	// timeago = "1.5m"
	expectedDuration = 90000
	currentTS = time.Now().UnixNano() / int64(time.Millisecond)
	ts, err = r.backupClient.GetTS(r.ctx, 90*time.Second, 0)
	c.Assert(err, IsNil)
	pdTS = oracle.ExtractPhysical(ts)
	duration = int(currentTS - pdTS)
	c.Assert(duration, Greater, expectedDuration-deviation)
	c.Assert(duration, Less, expectedDuration+deviation)

	// timeago = "-1m"
	_, err = r.backupClient.GetTS(r.ctx, -time.Minute, 0)
	c.Assert(err, ErrorMatches, "negative timeago is not allowed.*")

	// timeago = "1000000h" overflows
	_, err = r.backupClient.GetTS(r.ctx, 1000000*time.Hour, 0)
	c.Assert(err, ErrorMatches, ".*backup ts overflow.*")

	// timeago = "10h" exceed GCSafePoint
	p, l, err := r.mockPDClient.GetTS(r.ctx)
	c.Assert(err, IsNil)
	now := oracle.ComposeTS(p, l)
	_, err = r.mockPDClient.UpdateGCSafePoint(r.ctx, now)
	c.Assert(err, IsNil)
	_, err = r.backupClient.GetTS(r.ctx, 10*time.Hour, 0)
	c.Assert(err, ErrorMatches, ".*GC safepoint [0-9]+ exceed TS [0-9]+.*")

	// timeago and backupts both exists, use backupts
	backupts := oracle.ComposeTS(p+10, l)
	ts, err = r.backupClient.GetTS(r.ctx, time.Minute, backupts)
	c.Assert(err, IsNil)
	c.Assert(ts, Equals, backupts)
}

func (r *testBackup) TestBuildTableRange(c *C) {
	type Case struct {
		ids []int64
		trs []kv.KeyRange
	}
	low := codec.EncodeInt(nil, math.MinInt64)
	high := kv.Key(codec.EncodeInt(nil, math.MaxInt64)).PrefixNext()
	cases := []Case{
		{ids: []int64{1}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
		}},
		{ids: []int64{1, 2, 3}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
			{StartKey: tablecodec.EncodeRowKey(2, low), EndKey: tablecodec.EncodeRowKey(2, high)},
			{StartKey: tablecodec.EncodeRowKey(3, low), EndKey: tablecodec.EncodeRowKey(3, high)},
		}},
		{ids: []int64{1, 3}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
			{StartKey: tablecodec.EncodeRowKey(3, low), EndKey: tablecodec.EncodeRowKey(3, high)},
		}},
	}
	for _, cs := range cases {
		c.Log(cs)
		tbl := &model.TableInfo{Partition: &model.PartitionInfo{Enable: true}}
		for _, id := range cs.ids {
			tbl.Partition.Definitions = append(tbl.Partition.Definitions,
				model.PartitionDefinition{ID: id})
		}
		ranges, err := backup.BuildTableRanges(tbl)
		c.Assert(err, IsNil)
		c.Assert(ranges, DeepEquals, cs.trs)
	}

	tbl := &model.TableInfo{ID: 7}
	ranges, err := backup.BuildTableRanges(tbl)
	c.Assert(err, IsNil)
	c.Assert(ranges, DeepEquals, []kv.KeyRange{
		{StartKey: tablecodec.EncodeRowKey(7, low), EndKey: tablecodec.EncodeRowKey(7, high)},
	})
}

func (r *testBackup) TestOnBackupRegionErrorResponse(c *C) {
	type Case struct {
		storeID           uint64
		bo                *tikv.Backoffer
		backupTS          uint64
		lockResolver      *tikv.LockResolver
		resp              *kvproto.BackupResponse
		exceptedBackoffMs int
		exceptedErr       bool
	}
	newBackupRegionErrorResp := func(regionError *errorpb.Error) *kvproto.BackupResponse {
		return &kvproto.BackupResponse{Error: &kvproto.Error{Detail: &kvproto.Error_RegionError{RegionError: regionError}}}
	}

	cases := []Case{
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{NotLeader: &errorpb.NotLeader{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{RegionNotFound: &errorpb.RegionNotFound{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{KeyNotInRegion: &errorpb.KeyNotInRegion{}}), exceptedBackoffMs: 0, exceptedErr: true},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{StaleCommand: &errorpb.StaleCommand{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{StoreNotMatch: &errorpb.StoreNotMatch{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{}}), exceptedBackoffMs: 0, exceptedErr: true},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{ReadIndexNotReady: &errorpb.ReadIndexNotReady{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{ProposalInMergingMode: &errorpb.ProposalInMergingMode{}}), exceptedBackoffMs: 1000, exceptedErr: false},
	}
	for _, cs := range cases {
		c.Log(cs)
		_, backoffMs, err := backup.OnBackupResponse(cs.storeID, cs.bo, cs.backupTS, cs.lockResolver, cs.resp)
		c.Assert(backoffMs, Equals, cs.exceptedBackoffMs)
		if cs.exceptedErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
	}
}
