// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package logutil_test

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	berrors "github.com/pingcap/br/pkg/errors"
	"github.com/pingcap/br/pkg/logutil"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testLoggingSuite{})

type testLoggingSuite struct{}

func newFile(j int) *backup.File {
	return &backup.File{
		Name:         fmt.Sprint(j),
		StartKey:     []byte(fmt.Sprint(j)),
		EndKey:       []byte(fmt.Sprint(j + 1)),
		TotalKvs:     uint64(j),
		TotalBytes:   uint64(j),
		StartVersion: uint64(j),
		EndVersion:   uint64(j + 1),
		Crc64Xor:     uint64(j),
		Sha256:       []byte(fmt.Sprint(j)),
		Cf:           "write",
	}
}

func (s *testLoggingSuite) TestFile(c *C) {
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.File(newFile(1))})
	c.Assert(err, IsNil)
	c.Assert(out.String(), Equals,
		"{\"file\": {\"name\": \"1\", \"CF\": \"write\", \"sha256\": \"31\", \"startKey\": \"31\", \"endKey\": \"32\", \"startVersion\": 1, \"endVersion\": 2, \"totalKvs\": 1, \"totalBytes\": 1, \"CRC64Xor\": 1}}\n")
}

func (s *testLoggingSuite) TestFiles(c *C) {
	cases := []struct {
		count  int
		expect string
	}{
		{0, "{\"files\": {\"total\": 0, \"files\": [], \"totalKVs\": 0, \"totalBytes\": 0, \"totalFileCount\": 0}}\n"},
		{1, "{\"files\": {\"total\": 1, \"files\": [\"0\"], \"totalKVs\": 0, \"totalBytes\": 0, \"totalFileCount\": 1}}\n"},
		{2, "{\"files\": {\"total\": 2, \"files\": [\"0\", \"1\"], \"totalKVs\": 1, \"totalBytes\": 1, \"totalFileCount\": 2}}\n"},
		{3, "{\"files\": {\"total\": 3, \"files\": [\"0\", \"1\", \"2\"], \"totalKVs\": 3, \"totalBytes\": 3, \"totalFileCount\": 3}}\n"},
		{4, "{\"files\": {\"total\": 4, \"files\": [\"0\", \"1\", \"2\", \"3\"], \"totalKVs\": 6, \"totalBytes\": 6, \"totalFileCount\": 4}}\n"},
		{5, "{\"files\": {\"total\": 5, \"files\": \"0 ...(skip 3)... 4\", \"totalKVs\": 10, \"totalBytes\": 10, \"totalFileCount\": 5}}\n"},
		{6, "{\"files\": {\"total\": 6, \"files\": \"0 ...(skip 4)... 5\", \"totalKVs\": 15, \"totalBytes\": 15, \"totalFileCount\": 6}}\n"},
		{1024, "{\"files\": {\"total\": 1024, \"files\": \"0 ...(skip 1022)... 1023\", \"totalKVs\": 523776, \"totalBytes\": 523776, \"totalFileCount\": 1024}}\n"},
	}

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	for _, cs := range cases {
		ranges := make([]*backup.File, cs.count)
		for j := 0; j < cs.count; j++ {
			ranges[j] = newFile(j)
		}
		out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.Files(ranges)})
		c.Assert(err, IsNil)
		c.Assert(out.String(), Equals, cs.expect)
	}
}

func (s *testLoggingSuite) TestKey(c *C) {
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.Key("test", []byte{0, 1, 2, 3})})
	c.Assert(err, IsNil)
	c.Assert(out.String(), Equals, "{\"test\": \"00010203\"}\n")
}

func (s *testLoggingSuite) TestKeys(c *C) {
	cases := []struct {
		count  int
		expect string
	}{
		{0, "{\"keys\": {\"total\": 0, \"keys\": []}}\n"},
		{1, "{\"keys\": {\"total\": 1, \"keys\": [\"30303030\"]}}\n"},
		{2, "{\"keys\": {\"total\": 2, \"keys\": [\"30303030\", \"30303031\"]}}\n"},
		{3, "{\"keys\": {\"total\": 3, \"keys\": [\"30303030\", \"30303031\", \"30303032\"]}}\n"},
		{4, "{\"keys\": {\"total\": 4, \"keys\": [\"30303030\", \"30303031\", \"30303032\", \"30303033\"]}}\n"},
		{5, "{\"keys\": {\"total\": 5, \"keys\": \"30303030 ...(skip 3)... 30303034\"}}\n"},
		{6, "{\"keys\": {\"total\": 6, \"keys\": \"30303030 ...(skip 4)... 30303035\"}}\n"},
		{1024, "{\"keys\": {\"total\": 1024, \"keys\": \"30303030 ...(skip 1022)... 31303233\"}}\n"},
	}

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	for _, cs := range cases {
		ranges := make([][]byte, cs.count)
		for j := 0; j < cs.count; j++ {
			ranges[j] = []byte(fmt.Sprintf("%04d", j))
		}
		out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.Keys(ranges)})
		c.Assert(err, IsNil)
		c.Assert(out.String(), Equals, cs.expect)
	}
}

func (s *testLoggingSuite) TestRewriteRule(c *C) {
	rule := &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("old"),
		NewKeyPrefix: []byte("new"),
		NewTimestamp: 0x555555,
	}

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.RewriteRule(rule)})
	c.Assert(err, IsNil)
	c.Assert(out.String(), Equals, "{\"rewriteRule\": {\"oldKeyPrefix\": \"6f6c64\", \"newKeyPrefix\": \"6e6577\", \"newTimestamp\": 5592405}}\n")
}

func (s *testLoggingSuite) TestRegion(c *C) {
	region := &metapb.Region{
		Id:          1,
		StartKey:    []byte{0x00, 0x01},
		EndKey:      []byte{0x00, 0x02},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{{Id: 2, StoreId: 3}, {Id: 4, StoreId: 5}},
	}

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.Region(region)})
	c.Assert(err, IsNil)
	c.Assert(out.String(), Equals,
		"{\"region\": {\"ID\": 1, \"startKey\": \"0001\", \"endKey\": \"0002\", \"epoch\": \"conf_ver:1 version:1 \", \"peers\": \"id:2 store_id:3 ,id:4 store_id:5 \"}}\n")
}

func (s *testLoggingSuite) TestLeader(c *C) {
	leader := &metapb.Peer{Id: 2, StoreId: 3}

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.Leader(leader)})
	c.Assert(err, IsNil)
	c.Assert(out.String(), Equals, "{\"leader\": \"id:2 store_id:3 \"}\n")
}

func (s *testLoggingSuite) TestSSTMeta(c *C) {
	meta := &import_sstpb.SSTMeta{
		Uuid: []byte("mock uuid"),
		Range: &import_sstpb.Range{
			Start: []byte{0x00, 0x01},
			End:   []byte{0x00, 0x02},
		},
		Crc32:       uint32(0x555555),
		Length:      1,
		CfName:      "default",
		RegionId:    1,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.SSTMeta(meta)})
	c.Assert(err, IsNil)
	c.Assert(out.String(), Equals,
		"{\"sstMeta\": {\"CF\": \"default\", \"endKeyExclusive\": false, \"CRC32\": 5592405, \"length\": 1, \"regionID\": 1, \"regionEpoch\": \"conf_ver:1 version:1 \", \"startKey\": \"0001\", \"endKey\": \"0002\", \"UUID\": \"invalid UUID 6d6f636b2075756964\"}}\n")
}

func (s *testLoggingSuite) TestShortError(c *C) {
	err := errors.Annotate(berrors.ErrInvalidArgument, "test")

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{logutil.ShortError(err)})
	c.Assert(err, IsNil)
	c.Assert(out.String(), Equals, "{\"error\": \"test: [BR:Common:ErrInvalidArgument]invalid argument\"}\n")
}
