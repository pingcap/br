package backup

import (
	"context"
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"

	"github.com/pingcap/br/pkg/conn"
)

type testBackup struct {
	ctx    context.Context
	cancel context.CancelFunc

	backupClient *Client
}

var _ = Suite(&testBackup{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (r *testBackup) SetUpSuite(c *C) {
	mockPDClient := mocktikv.NewPDClient(mocktikv.NewCluster())
	r.ctx, r.cancel = context.WithCancel(context.Background())
	mockMgr := &conn.Mgr{}
	mockMgr.SetPDClient(mockPDClient)
	mockMgr.SetPDHTTP([]string{"test"}, nil)
	r.backupClient = &Client{
		clusterID: mockPDClient.GetClusterID(r.ctx),
		mgr:       mockMgr,
	}
}

func (r *testBackup) TestGetTS(c *C) {
	var (
		err error
		// mockPDClient' physical ts and current ts will have deviation
		// so make this deviation tolerance 100ms
		deviation = 100
	)

	// timeago not valid
	timeAgo := "invalid"
	_, err = r.backupClient.GetTS(r.ctx, timeAgo)
	c.Assert(err, ErrorMatches, "time: invalid duration invalid")

	// timeago not work
	timeAgo = ""
	expectedDuration := 0
	currentTs := time.Now().UnixNano() / int64(time.Millisecond)
	ts, err := r.backupClient.GetTS(r.ctx, timeAgo)
	c.Assert(err, IsNil)
	pdTs := oracle.ExtractPhysical(ts)
	duration := int(currentTs - pdTs)
	c.Assert(duration, Greater, expectedDuration-deviation)
	c.Assert(duration, Less, expectedDuration+deviation)

	// timeago = "1.5m"
	timeAgo = "1.5m"
	expectedDuration = 90000
	currentTs = time.Now().UnixNano() / int64(time.Millisecond)
	ts, err = r.backupClient.GetTS(r.ctx, timeAgo)
	c.Assert(err, IsNil)
	pdTs = oracle.ExtractPhysical(ts)
	duration = int(currentTs - pdTs)
	c.Assert(duration, Greater, expectedDuration-deviation)
	c.Assert(duration, Less, expectedDuration+deviation)

	// timeago = "-1m"
	timeAgo = "-1m"
	_, err = r.backupClient.GetTS(r.ctx, timeAgo)
	c.Assert(err, ErrorMatches, "negative timeago is allowed")

	// timeago = "1000000h" overflows
	timeAgo = "1000000h"
	_, err = r.backupClient.GetTS(r.ctx, timeAgo)
	c.Assert(err, ErrorMatches, "backup ts overflow.*")

	// timeago = "10h" exceed GCSafePoint
	p, l, err := r.backupClient.mgr.GetPDClient().GetTS(r.ctx)
	c.Assert(err, IsNil)
	now := oracle.ComposeTS(p, l)
	_, err = r.backupClient.mgr.GetPDClient().UpdateGCSafePoint(r.ctx, now)
	c.Assert(err, IsNil)
	timeAgo = "10h"
	_, err = r.backupClient.GetTS(r.ctx, timeAgo)
	c.Assert(err, ErrorMatches, "given backup time exceed GCSafePoint")
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
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)}},
		},
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
		ranges, err := buildTableRanges(tbl)
		c.Assert(err, IsNil)
		c.Assert(ranges, DeepEquals, cs.trs)
	}

	tbl := &model.TableInfo{ID: 7}
	ranges, err := buildTableRanges(tbl)
	c.Assert(err, IsNil)
	c.Assert(ranges, DeepEquals, []kv.KeyRange{
		{StartKey: tablecodec.EncodeRowKey(7, low), EndKey: tablecodec.EncodeRowKey(7, high)},
	})

}
