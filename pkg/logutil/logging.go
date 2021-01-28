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

	"github.com/pingcap/br/pkg/redact"
)

// AbbreviatedArrayMarshaler abbreviates an array of elements.
type AbbreviatedArrayMarshaler []string

// MarshalLogArray implements zapcore.ArrayMarshaler.
func (abb AbbreviatedArrayMarshaler) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	if len(abb) <= 4 {
		for _, e := range abb {
			encoder.AppendString(e)
		}
	} else {
		total := len(abb)
		encoder.AppendString(abb[0])
		encoder.AppendString(fmt.Sprintf("(skip %d)", total-2))
		encoder.AppendString(abb[total-1])
	}
	return nil
}

// AbbreviatedArray constructs a field that abbreviates an array of elements.
func AbbreviatedArray(
	key string, elements interface{}, marshalFunc func(interface{}) []string,
) zap.Field {
	return zap.Array(key, AbbreviatedArrayMarshaler(marshalFunc(elements)))
}

type zapFileMarshaler struct{ *backup.File }

func (file zapFileMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("name", file.GetName())
	enc.AddString("CF", file.GetCf())
	enc.AddString("sha256", hex.EncodeToString(file.GetSha256()))
	enc.AddString("startKey", redact.Key(file.GetStartKey()))
	enc.AddString("endKey", redact.Key(file.GetEndKey()))
	enc.AddUint64("startVersion", file.GetStartVersion())
	enc.AddUint64("endVersion", file.GetEndVersion())
	enc.AddUint64("totalKvs", file.GetTotalKvs())
	enc.AddUint64("totalBytes", file.GetTotalBytes())
	enc.AddUint64("CRC64Xor", file.GetCrc64Xor())
	return nil
}

type zapFilesMarshaler []*backup.File

func (fs zapFilesMarshaler) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	total := len(fs)
	encoder.AddInt("total", total)
	elements := make([]string, 0, total)
	for _, f := range fs {
		elements = append(elements, f.GetName())
	}
	_ = encoder.AddArray("files", AbbreviatedArrayMarshaler(elements))

	totalKVs := uint64(0)
	totalBytes := uint64(0)
	totalSize := uint64(0)
	for _, file := range fs {
		totalKVs += file.GetTotalKvs()
		totalBytes += file.GetTotalBytes()
		totalSize += file.GetSize_()
	}
	encoder.AddUint64("totalKVs", totalKVs)
	encoder.AddUint64("totalBytes", totalBytes)
	encoder.AddUint64("totalSize", totalSize)
	return nil
}

// File make the zap fields for a file.
func File(file *backup.File) zap.Field {
	return zap.Object("file", zapFileMarshaler{file})
}

// Files make the zap field for a set of file.
func Files(fs []*backup.File) zap.Field {
	return zap.Object("files", zapFilesMarshaler(fs))
}

type zapRewriteRuleMarshaler struct{ *import_sstpb.RewriteRule }

func (rewriteRule zapRewriteRuleMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("oldKeyPrefix", hex.EncodeToString(rewriteRule.GetOldKeyPrefix()))
	enc.AddString("newKeyPrefix", hex.EncodeToString(rewriteRule.GetNewKeyPrefix()))
	enc.AddUint64("newTimestamp", rewriteRule.GetNewTimestamp())
	return nil
}

// RewriteRule make the zap fields for a rewrite rule.
func RewriteRule(rewriteRule *import_sstpb.RewriteRule) zap.Field {
	return zap.Object("rewriteRule", zapRewriteRuleMarshaler{rewriteRule})
}

type zapMarshalRegionMarshaler struct{ *metapb.Region }

func (region zapMarshalRegionMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	peers := make([]string, 0, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		peers = append(peers, peer.String())
	}
	enc.AddUint64("ID", region.Id)
	enc.AddString("startKey", redact.Key(region.GetStartKey()))
	enc.AddString("endKey", redact.Key(region.GetEndKey()))
	enc.AddString("epoch", region.GetRegionEpoch().String())
	enc.AddString("peers", strings.Join(peers, ","))
	return nil
}

// Region make the zap fields for a region.
func Region(region *metapb.Region) zap.Field {
	return zap.Object("region", zapMarshalRegionMarshaler{region})
}

// Leader make the zap fields for a peer.
// nolint:interfacer
func Leader(peer *metapb.Peer) zap.Field {
	return zap.String("leader", peer.String())
}

type zapSSTMetaMarshaler struct{ *import_sstpb.SSTMeta }

func (sstMeta zapSSTMetaMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("CF", sstMeta.GetCfName())
	enc.AddBool("endKeyExclusive", sstMeta.EndKeyExclusive)
	enc.AddUint32("CRC32", sstMeta.Crc32)
	enc.AddUint64("length", sstMeta.Length)
	enc.AddUint64("regionID", sstMeta.RegionId)
	enc.AddString("regionEpoch", sstMeta.RegionEpoch.String())
	enc.AddString("startKey", redact.Key(sstMeta.GetRange().GetStart()))
	enc.AddString("endKey", redact.Key(sstMeta.GetRange().GetEnd()))

	sstUUID, err := uuid.FromBytes(sstMeta.GetUuid())
	if err != nil {
		enc.AddString("UUID", fmt.Sprintf("invalid UUID %s", hex.EncodeToString(sstMeta.GetUuid())))
	} else {
		enc.AddString("UUID", sstUUID.String())
	}
	return nil
}

// SSTMeta make the zap fields for a SST meta.
func SSTMeta(sstMeta *import_sstpb.SSTMeta) zap.Field {
	return zap.Object("sstMeta", zapSSTMetaMarshaler{sstMeta})
}

type zapKeysMarshaler [][]byte

func (keys zapKeysMarshaler) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	total := len(keys)
	encoder.AddInt("total", total)
	elements := make([]string, 0, total)
	for _, k := range keys {
		elements = append(elements, redact.Key(k))
	}
	_ = encoder.AddArray("keys", AbbreviatedArrayMarshaler(elements))
	return nil
}

// Key constructs a field that carries upper hex format key.
func Key(fieldKey string, key []byte) zap.Field {
	return zap.String(fieldKey, redact.Key(key))
}

// Keys constructs a field that carries upper hex format keys.
func Keys(keys [][]byte) zap.Field {
	return zap.Object("keys", zapKeysMarshaler(keys))
}

// ShortError make the zap field to display error without verbose representation (e.g. the stack trace).
func ShortError(err error) zap.Field {
	return zap.String("error", err.Error())
}
