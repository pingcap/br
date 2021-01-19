// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package logutil

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type zapFileMarshaler struct{ *backup.File }

func (file zapFileMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("name", file.GetName())
	enc.AddString("CF", file.GetCf())
	enc.AddString("sha256", hex.EncodeToString(file.GetSha256()))
	enc.AddString("startKey", RedactKey(file.GetStartKey()))
	enc.AddString("endKey", RedactKey(file.GetEndKey()))
	enc.AddUint64("startVersion", file.GetStartVersion())
	enc.AddUint64("endVersion", file.GetEndVersion())
	enc.AddUint64("totalKvs", file.GetTotalKvs())
	enc.AddUint64("totalBytes", file.GetTotalBytes())
	enc.AddUint64("CRC64Xor", file.GetCrc64Xor())
	return nil
}

type zapRewriteRuleMarshaler struct{ *import_sstpb.RewriteRule }

func (rewriteRule zapRewriteRuleMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("oldKeyPrefix", hex.EncodeToString(rewriteRule.GetOldKeyPrefix()))
	enc.AddString("newKeyPrefix", hex.EncodeToString(rewriteRule.GetNewKeyPrefix()))
	enc.AddUint64("newTimestamp", rewriteRule.GetNewTimestamp())
	return nil
}

type zapMarshalRegionMarshaler struct{ *metapb.Region }

func (region zapMarshalRegionMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	peers := make([]string, 0, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		peers = append(peers, peer.String())
	}
	enc.AddUint64("ID", region.Id)
	enc.AddString("startKey", RedactKey(region.GetStartKey()))
	enc.AddString("endKey", RedactKey(region.GetEndKey()))
	enc.AddString("epoch", region.GetRegionEpoch().String())
	enc.AddString("peers", strings.Join(peers, ","))
	return nil
}

type zapSSTMetaMarshaler struct{ *import_sstpb.SSTMeta }

func (sstMeta zapSSTMetaMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("CF", sstMeta.GetCfName())
	enc.AddBool("endKeyExclusive", sstMeta.EndKeyExclusive)
	enc.AddUint32("CRC32", sstMeta.Crc32)
	enc.AddUint64("length", sstMeta.Length)
	enc.AddUint64("regionID", sstMeta.RegionId)
	enc.AddString("regionEpoch", sstMeta.RegionEpoch.String())
	enc.AddString("startKey", RedactKey(sstMeta.GetRange().GetStart()))
	enc.AddString("endKey", RedactKey(sstMeta.GetRange().GetEnd()))

	sstUUID, err := uuid.FromBytes(sstMeta.GetUuid())
	if err != nil {
		enc.AddString("UUID", fmt.Sprintf("invalid UUID %s", hex.EncodeToString(sstMeta.GetUuid())))
	} else {
		enc.AddString("UUID", sstUUID.String())
	}
	return nil
}

type zapKeysMarshaler [][]byte

func (keys zapKeysMarshaler) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, key := range keys {
		encoder.AppendString(RedactKey(key))
	}
	return nil
}

func (keys zapKeysMarshaler) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	total := len(keys)
	encoder.AddInt("total", total)
	if total <= 4 {
		encoder.AddArray("keys", keys)
	} else {
		encoder.AddString("keys", fmt.Sprintf("%s ...(skip %d)... %s",
			RedactKey(keys[0]), total-2, RedactKey(keys[total-1])))
	}
	return nil
}

type zapFilesMarshaler []*backup.File

func (fs zapFilesMarshaler) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, file := range fs {
		encoder.AppendString(file.Name)
	}
	return nil
}

func (fs zapFilesMarshaler) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	total := len(fs)
	encoder.AddInt("total", total)
	if total <= 4 {
		encoder.AddArray("files", fs)
	} else {
		encoder.AddString("files", fmt.Sprintf("%s ...(skip %d)... %s",
			fs[0], total-2, fs[total-1]))
	}

	totalKVs := uint64(0)
	totalSize := uint64(0)
	for _, file := range fs {
		totalKVs += file.GetTotalKvs()
		totalSize += file.GetTotalBytes()
	}
	encoder.AddUint64("totalKVs", totalKVs)
	encoder.AddUint64("totalBytes", totalSize)
	encoder.AddInt("totalFileCount", len(fs))
	return nil
}

// Key constructs a field that carries upper hex format key.
func Key(fieldKey string, key []byte) zap.Field {
	return zap.String(fieldKey, RedactKey(key))
}

// Keys constructs a field that carries upper hex format keys.
func Keys(keys [][]byte) zapcore.Field {
	return zap.Object("keys", zapKeysMarshaler(keys))
}

// RewriteRule make the zap fields for a rewrite rule.
func RewriteRule(rewriteRule *import_sstpb.RewriteRule) zapcore.Field {
	return zap.Object("rewriteRule", zapRewriteRuleMarshaler{rewriteRule})
}

// Region make the zap fields for a region.
func Region(region *metapb.Region) zapcore.Field {
	return zap.Object("region", zapMarshalRegionMarshaler{region})
}

// File make the zap fields for a file.
func File(file *backup.File) zapcore.Field {
	return zap.Object("file", zapFileMarshaler{file})
}

// SSTMeta make the zap fields for a SST meta.
func SSTMeta(sstMeta *import_sstpb.SSTMeta) zapcore.Field {
	return zap.Object("sstMeta", zapSSTMetaMarshaler{sstMeta})
}

// Files make the zap field for a set of file.
func Files(fs []*backup.File) zapcore.Field {
	return zap.Object("files", zapFilesMarshaler(fs))
}

// ShortError make the zap field to display error without verbose representation (e.g. the stack trace).
func ShortError(err error) zapcore.Field {
	return zap.String("error", err.Error())
}
