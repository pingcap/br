// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package errors

import (
	"github.com/pingcap/errors"
)

// BR errors.
var (
	ErrUnknown         = errors.Normalize("internal error", errors.RFCCodeText("BRIE:Common:ErrUnknown"))
	ErrInvalidArgument = errors.Normalize("invalid argument", errors.RFCCodeText("BRIE:Common:ErrInvalidArgument"))
	ErrVersionMismatch = errors.Normalize("version mismatch", errors.RFCCodeText("BRIE:Common:ErrVersionMismatch"))

	ErrPDUpdateFailed    = errors.Normalize("failed to update PD", errors.RFCCodeText("BRIE:PD:ErrPDUpdateFailed"))
	ErrPDLeaderNotFound  = errors.Normalize("PD leader not found", errors.RFCCodeText("BRIE:PD:ErrPDLeaderNotFound"))
	ErrPDInvalidResponse = errors.Normalize("PD invalid response", errors.RFCCodeText("BRIE:PD:ErrPDInvalidResponse"))

	ErrBackupChecksumMismatch    = errors.Normalize("backup checksum mismatch", errors.RFCCodeText("BRIE:Backup:ErrBackupChecksumMismatch"))
	ErrBackupInvalidRange        = errors.Normalize("backup range invalid", errors.RFCCodeText("BRIE:Backup:ErrBackupInvalidRange"))
	ErrBackupNoLeader            = errors.Normalize("backup no leader", errors.RFCCodeText("BRIE:Backup:ErrBackupNoLeader"))
	ErrBackupGCSafepointExceeded = errors.Normalize("backup GC safepoint exceeded", errors.RFCCodeText("BRIE:Backup:ErrBackupGCSafepointExceeded"))

	ErrRestoreModeMismatch     = errors.Normalize("restore mode mismatch", errors.RFCCodeText("BRIE:Restore:ErrRestoreModeMismatch"))
	ErrRestoreRangeMismatch    = errors.Normalize("restore range mismatch", errors.RFCCodeText("BRIE:Restore:ErrRestoreRangeMismatch"))
	ErrRestoreChecksumMismatch = errors.Normalize("restore checksum mismatch", errors.RFCCodeText("BRIE:Restore:ErrRestoreChecksumMismatch"))
	ErrRestoreTableIDMismatch  = errors.Normalize("restore table ID mismatch", errors.RFCCodeText("BRIE:Restore:ErrRestoreTableIDMismatch"))
	ErrRestoreRejectStore      = errors.Normalize("failed to restore remove rejected store", errors.RFCCodeText("BRIE:Restore:ErrRestoreRejectStore"))
	ErrRestoreNoPeer           = errors.Normalize("region does not have peer", errors.RFCCodeText("BRIE:Restore:ErrRestoreNoPeer"))
	ErrRestoreSplitFailed      = errors.Normalize("fail to split region", errors.RFCCodeText("BRIE:Restore:ErrRestoreSplitFailed"))
	ErrRestoreInvalidRewrite   = errors.Normalize("invalid rewrite rule", errors.RFCCodeText("BRIE:Restore:ErrRestoreInvalidRewrite"))
	ErrRestoreInvalidBackup    = errors.Normalize("invalid backup", errors.RFCCodeText("BRIE:Restore:ErrRestoreInvalidBackup"))
	ErrRestoreInvalidRange     = errors.Normalize("invalid restore range", errors.RFCCodeText("BRIE:Restore:ErrRestoreInvalidRange"))
	ErrRestoreWriteAndIngest   = errors.Normalize("failed to write and ingest", errors.RFCCodeText("BRIE:Restore:ErrRestoreWriteAndIngest"))
	ErrRestoreSchemaNotExists  = errors.Normalize("schema not exists", errors.RFCCodeText("BRIE:Restore:ErrRestoreSchemaNotExists"))

	// TODO maybe it belongs to PiTR.
	ErrRestoreRTsConstrain = errors.Normalize("resolved ts constrain violation", errors.RFCCodeText("BRIE:Restore:ErrRestoreResolvedTsConstrain"))

	ErrPiTRInvalidCDCLogFormat = errors.Normalize("invalid cdc log format", errors.RFCCodeText("BRIE:PiTR:ErrPiTRInvalidCDCLogFormat"))

	ErrStorageUnknown       = errors.Normalize("unknown external storage error", errors.RFCCodeText("BRIE:ExternalStorage:ErrStorageUnknown"))
	ErrStorageInvalidConfig = errors.Normalize("invalid external storage config", errors.RFCCodeText("BRIE:ExternalStorage:ErrStorageInvalidConfig"))

	// Errors reported from TiKV.
	ErrKVUnknown           = errors.Normalize("unknown tikv error", errors.RFCCodeText("BRIE:KV:ErrKVUnknown"))
	ErrKVClusterIDMismatch = errors.Normalize("tikv cluster ID mismatch", errors.RFCCodeText("BRIE:KV:ErrKVClusterIDMismatch"))
	ErrKVNotHealth         = errors.Normalize("tikv cluster not health", errors.RFCCodeText("BRIE:KV:ErrKVNotHealth"))
	ErrKVNotLeader         = errors.Normalize("not leader", errors.RFCCodeText("BRIE:KV:ErrKVNotLeader"))
	ErrKVNotTiKV           = errors.Normalize("storage is not tikv", errors.RFCCodeText("BRIE:KV:ErrNotTiKVStorage"))

	// ErrKVEpochNotMatch is the error raised when ingestion failed with "epoch
	// not match". This error is retryable.
	ErrKVEpochNotMatch = errors.Normalize("epoch not match", errors.RFCCodeText("BRIE:KV:ErrKVEpochNotMatch"))
	// ErrKVKeyNotInRegion is the error raised when ingestion failed with "key not
	// in region". This error cannot be retried.
	ErrKVKeyNotInRegion = errors.Normalize("key not in region", errors.RFCCodeText("BRIE:KV:ErrKVKeyNotInRegion"))
	// ErrKVRewriteRuleNotFound is the error raised when download failed with
	// "rewrite rule not found". This error cannot be retried
	ErrKVRewriteRuleNotFound = errors.Normalize("rewrite rule not found", errors.RFCCodeText("BRIE:KV:ErrKVRewriteRuleNotFound"))
	// ErrKVRangeIsEmpty is the error raised when download failed with "range is
	// empty". This error cannot be retried.
	ErrKVRangeIsEmpty = errors.Normalize("range is empty", errors.RFCCodeText("BRIE:KV:ErrKVRangeIsEmpty"))
	// ErrKVDownloadFailed indicates a generic download error, expected to be
	// retryable.
	ErrKVDownloadFailed = errors.Normalize("download sst failed", errors.RFCCodeText("BRIE:KV:ErrKVDownloadFailed"))
	// ErrKVIngestFailed indicates a generic, retryable ingest error.
	ErrKVIngestFailed = errors.Normalize("ingest sst failed", errors.RFCCodeText("BRIE:KV:ErrKVIngestFailed"))
)
