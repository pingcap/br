// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type zapMarshalFileMixIn struct{ *backup.File }

func (file zapMarshalFileMixIn) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("name", file.GetName())
	enc.AddString("CF", file.GetCf())
	enc.AddString("sha256", hex.EncodeToString(file.GetSha256()))
	enc.AddString("start key", WrapKey(file.GetStartKey()).String())
	enc.AddString("end key", WrapKey(file.GetEndKey()).String())
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
	enc.AddString("start key", WrapKey(region.GetStartKey()).String())
	enc.AddString("end key", WrapKey(region.GetEndKey()).String())
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
	enc.AddString("range start", WrapKey(sstMeta.GetRange().GetStart()).String())
	enc.AddString("range end", WrapKey(sstMeta.GetRange().GetEnd()).String())

	sstUUID, err := uuid.FromBytes(sstMeta.GetUuid())
	if err != nil {
		return err
	}
	enc.AddString("UUID", sstUUID.String())
	return nil
}

type zapArrayMarshalKeysMixIn [][]byte

func (keys zapArrayMarshalKeysMixIn) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, key := range keys {
		enc.AppendString(WrapKey(key).String())
	}
	return nil
}

// WrapKey wrap a key as a Stringer that can print proper upper hex format.
func WrapKey(key []byte) fmt.Stringer {
	return kv.Key(key)
}

// WrapKeys wrap keys as a ArrayMarshaler that can print proper upper hex format.
func WrapKeys(keys [][]byte) zapcore.ArrayMarshaler {
	return zapArrayMarshalKeysMixIn(keys)
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

// ZapSSTMeta make the zap fields for a SST meta.
func ZapSSTMeta(sstMeta *import_sstpb.SSTMeta) zapcore.Field {
	return zap.Object("sst meta", zapMarshalSSTMetaMixIn{sstMeta})
}
