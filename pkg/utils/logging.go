// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"encoding/hex"
	"strings"

	"github.com/google/uuid"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type zapMarshalFileMixIn struct{ *backup.File }

func (file zapMarshalFileMixIn) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("name", file.GetName())
	enc.AddString("CF", file.GetCf())
	enc.AddString("sha256", hex.EncodeToString(file.GetSha256()))
	enc.AddString("start key", EncodeKey(file.GetStartKey()))
	enc.AddString("end key", EncodeKey(file.GetEndKey()))
	enc.AddUint64("start version", file.GetStartVersion())
	enc.AddUint64("end version", file.GetEndVersion())
	enc.AddUint64("total kvs", file.GetTotalKvs())
	enc.AddUint64("total bytes", file.GetTotalBytes())
	enc.AddUint64("CRC64 xor", file.GetCrc64Xor())
	return nil
}

type zapMarshalRewriteRuleMixIn struct{ *import_sstpb.RewriteRule }

func (rewriteRule zapMarshalRewriteRuleMixIn) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("old key prefix", hex.EncodeToString(rewriteRule.GetOldKeyPrefix()))
	enc.AddString("new key prefix", hex.EncodeToString(rewriteRule.GetNewKeyPrefix()))
	enc.AddUint64("new timestamp", rewriteRule.GetNewTimestamp())
	return nil
}

type zapMarshalRegionMixIn struct{ *metapb.Region }

func (region zapMarshalRegionMixIn) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	peers := make([]string, 0, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		peers = append(peers, peer.String())
	}
	enc.AddUint64("ID", region.Id)
	enc.AddString("start key", EncodeKey(region.GetStartKey()))
	enc.AddString("end key", EncodeKey(region.GetEndKey()))
	enc.AddString("epoch", region.GetRegionEpoch().String())
	enc.AddString("peers", strings.Join(peers, ","))
	return nil
}

type zapMarshalSSTMetaMixIn struct{ *import_sstpb.SSTMeta }

func (sstMeta zapMarshalSSTMetaMixIn) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("CF", sstMeta.GetCfName())
	enc.AddBool("end key exclusive", sstMeta.EndKeyExclusive)
	enc.AddUint32("CRC32", sstMeta.Crc32)
	enc.AddUint64("length", sstMeta.Length)
	enc.AddUint64("region ID", sstMeta.RegionId)
	enc.AddString("region Epoch", sstMeta.RegionEpoch.String())
	enc.AddString("range start", EncodeKey(sstMeta.GetRange().GetStart()))
	enc.AddString("range end", EncodeKey(sstMeta.GetRange().GetEnd()))

	sstUUID, err := uuid.FromBytes(sstMeta.GetUuid())
	if err != nil {
		return err
	}
	enc.AddString("UUID", sstUUID.String())
	return nil
}

// EncodeKey encodes some byte-presenting key into readable form.
func EncodeKey(key []byte) string {
	return hex.EncodeToString(key)
}

// EncodeKeys encodes some byte-presenting keys into readable form.
func EncodeKeys(keys [][]byte) []string {
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, EncodeKey(key))
	}
	return result
}

// ZapRewriteRule make the zap fields for a rewrite rule.
func ZapRewriteRule(rewriteRule *import_sstpb.RewriteRule) zapcore.Field {
	return zap.Object("rewrite rule", zapMarshalRewriteRuleMixIn{rewriteRule})
}

// ZapRegion make the zap fields for a region.
func ZapRegion(region *metapb.Region) zapcore.Field {
	return zap.Object("region", zapMarshalRegionMixIn{region})
}

// ZapFile make the zap fields for a file.
func ZapFile(file *backup.File) zapcore.Field {
	return zap.Object("file", zapMarshalFileMixIn{file})
}

func ZapSSTMeta(sstMeta *import_sstpb.SSTMeta) zapcore.Field {
	return zap.Object("sst meta", zapMarshalSSTMetaMixIn{sstMeta})
}
