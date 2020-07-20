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
	enc.AddString("startKey", WrapKey(file.GetStartKey()).String())
	enc.AddString("endKey", WrapKey(file.GetEndKey()).String())
	enc.AddUint64("startVersion", file.GetStartVersion())
	enc.AddUint64("endVersion", file.GetEndVersion())
	enc.AddUint64("totalKvs", file.GetTotalKvs())
	enc.AddUint64("totalBytes", file.GetTotalBytes())
	enc.AddUint64("CRC64Xor", file.GetCrc64Xor())
	return nil
}

type zapMarshalRewriteRuleMixIn struct{ *import_sstpb.RewriteRule }

func (rewriteRule zapMarshalRewriteRuleMixIn) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("oldKeyPrefix", hex.EncodeToString(rewriteRule.GetOldKeyPrefix()))
	enc.AddString("newKeyPrefix", hex.EncodeToString(rewriteRule.GetNewKeyPrefix()))
	enc.AddUint64("newTimestamp", rewriteRule.GetNewTimestamp())
	return nil
}

type zapMarshalRegionMixIn struct{ *metapb.Region }

func (region zapMarshalRegionMixIn) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	peers := make([]string, 0, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		peers = append(peers, peer.String())
	}
	enc.AddUint64("ID", region.Id)
	enc.AddString("startKey", WrapKey(region.GetStartKey()).String())
	enc.AddString("endKey", WrapKey(region.GetEndKey()).String())
	enc.AddString("epoch", region.GetRegionEpoch().String())
	enc.AddString("peers", strings.Join(peers, ","))
	return nil
}

type zapMarshalSSTMetaMixIn struct{ *import_sstpb.SSTMeta }

func (sstMeta zapMarshalSSTMetaMixIn) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("CF", sstMeta.GetCfName())
	enc.AddBool("endKeyExclusive", sstMeta.EndKeyExclusive)
	enc.AddUint32("CRC32", sstMeta.Crc32)
	enc.AddUint64("length", sstMeta.Length)
	enc.AddUint64("regionID", sstMeta.RegionId)
	enc.AddString("regionEpoch", sstMeta.RegionEpoch.String())
	enc.AddString("rangeStart", WrapKey(sstMeta.GetRange().GetStart()).String())
	enc.AddString("rangeEnd", WrapKey(sstMeta.GetRange().GetEnd()).String())

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

type Files []*backup.File

func (files Files) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	totalKVs := uint64(0)
	totalSize := uint64(0)
	for _, file := range files {
		totalKVs += file.GetTotalKvs()
		totalSize += file.GetTotalBytes()
	}
	encoder.AddUint64("totalKVs", totalKVs)
	encoder.AddUint64("totalBytes", totalSize)
	return nil
}

// WrapKey wrap a key as a Stringer that can print proper upper hex format.
func WrapKey(key []byte) fmt.Stringer {
	return kv.Key(key)
}

// WrapKeys wrap keys as an ArrayMarshaler that can print proper upper hex format.
func WrapKeys(keys [][]byte) zapcore.ArrayMarshaler {
	return zapArrayMarshalKeysMixIn(keys)
}

// ZapRewriteRule make the zap fields for a rewrite rule.
func ZapRewriteRule(rewriteRule *import_sstpb.RewriteRule) zapcore.Field {
	return zap.Object("rewriteRule", zapMarshalRewriteRuleMixIn{rewriteRule})
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
	return zap.Object("sstMeta", zapMarshalSSTMetaMixIn{sstMeta})
}

// ZapFiles make the zap field for a set of file.
func ZapFiles(files []*backup.File) zapcore.Field {
	return zap.Object("files", Files(files))
}
