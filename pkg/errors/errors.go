// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package errors

import "github.com/pingcap/errors"

// BR errors.
var (
	ErrInvalidArgument = errors.Normalize("invalid argument", errors.RFCCodeText("BR:Common:ErrInvalidArgument"), errors.MessageDefaultFormat())
	ErrInternal        = errors.Normalize("internal error", errors.RFCCodeText("BR:Common:ErrInternal"), errors.MessageDefaultFormat())
	ErrVersionMismatch = errors.Normalize("version mismatch", errors.RFCCodeText("BR:Common:ErrVersionMismatch"), errors.MessageDefaultFormat())

	ErrPDUpdateFailed      = errors.Normalize("failed to update PD", errors.RFCCodeText("BR:PD:ErrUpdatePDFailed"), errors.MessageDefaultFormat())
	ErrPDLeaderNotFound    = errors.Normalize("PD leader not found", errors.RFCCodeText("BR:PD:ErrUpdatePDFailed"), errors.MessageDefaultFormat())
	ErrPDInvalidResponse   = errors.Normalize("PD invalid response", errors.RFCCodeText("BR:PD:ErrPDInvalidResponse"), errors.MessageDefaultFormat())
	ErrKVUnknown           = errors.Normalize("unknown tikv error", errors.RFCCodeText("BR:PD:ErrKVUnknown"), errors.MessageDefaultFormat())
	ErrKVClusterIDMismatch = errors.Normalize("tikv cluster ID mismatch", errors.RFCCodeText("BR:PD:ErrKVClusterIDMismatch"), errors.MessageDefaultFormat())

	ErrBackupChecksumMismatch    = errors.Normalize("backup checksum mismatch", errors.RFCCodeText("BR:Backup:ErrBackupChecksumMismatch"), errors.MessageDefaultFormat())
	ErrBackupInvalidRange        = errors.Normalize("backup range invalid", errors.RFCCodeText("BR:Backup:ErrBackupInvalidRange"), errors.MessageDefaultFormat())
	ErrBackupNoLeader            = errors.Normalize("backup no leader", errors.RFCCodeText("BR:Backup:ErrBackupNoLeader"), errors.MessageDefaultFormat())
	ErrBackupGCSafepointExceeded = errors.Normalize("backup GC safepoint exceeded", errors.RFCCodeText("BR:Backup:ErrBackupGCSafepointExceeded"), errors.MessageDefaultFormat())

	ErrRestoreModeMismatch     = errors.Normalize("restore mode mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreModeMistmatch"), errors.MessageDefaultFormat())
	ErrRestoreRangeMismatch    = errors.Normalize("restore range mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreRangeMismatch"), errors.MessageDefaultFormat())
	ErrRestoreChecksumMismatch = errors.Normalize("restore checksum mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreChecksumMismatch"), errors.MessageDefaultFormat())
	ErrRestoreTableIDMismatch  = errors.Normalize("restore table ID mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreTableIDMismatch"), errors.MessageDefaultFormat())
	ErrRestoreRejectStore      = errors.Normalize("failed to restore remove rejected store", errors.RFCCodeText("BR:Restore:ErrRestoreRejectStore"), errors.MessageDefaultFormat())
	ErrRestoreNoPeer           = errors.Normalize("region does not have peer", errors.RFCCodeText("BR:Restore:ErrRestoreNoPeer"), errors.MessageDefaultFormat())
	ErrRestoreSplitFailed      = errors.Normalize("fail to split region", errors.RFCCodeText("BR:Restore:ErrRestoreSplitFailed"), errors.MessageDefaultFormat())
	ErrRestoreInvalidRewrite   = errors.Normalize("invalid rewrite rule", errors.RFCCodeText("BR:Restore:ErrRestoreInvalidRewrite"), errors.MessageDefaultFormat())
	ErrRestoreInvalidBackup    = errors.Normalize("invalid backup", errors.RFCCodeText("BR:Restore:ErrRestoreInvalidBackup"), errors.MessageDefaultFormat())
	ErrRestoreInvalidRange     = errors.Normalize("invalid restore range", errors.RFCCodeText("BR:Restore:ErrRestoreInvalidRange"), errors.MessageDefaultFormat())

	ErrStorageInvalidConfig = errors.Normalize("invalid external storage config", errors.RFCCodeText("BR:ExternalStorage:ErrStorageInvalidConfig"), errors.MessageDefaultFormat())

	// TiKV errors
	ErrKVNotHealth = errors.Normalize("tikv cluster not health", errors.RFCCodeText("BR:KV:ErrKVNotHealth"), errors.MessageDefaultFormat())
	// ErrEpochNotMatch is the error raised when ingestion failed with "epoch
	// not match". This error is retryable.
	ErrEpochNotMatch = errors.Normalize("epoch not match", errors.RFCCodeText("BR:KV:ErrEpochNotMatch"), errors.MessageDefaultFormat())
	// ErrKeyNotInRegion is the error raised when ingestion failed with "key not
	// in region". This error cannot be retried.
	ErrKeyNotInRegion = errors.Normalize("key not in region", errors.RFCCodeText("BR:KV:ErrKeyNotInRegion"), errors.MessageDefaultFormat())
	// ErrRewriteRuleNotFound is the error raised when download failed with
	// "rewrite rule not found". This error cannot be retried
	ErrRewriteRuleNotFound = errors.Normalize("rewrite rule not found", errors.RFCCodeText("BR:KV:ErrRewriteRuleNotFound"), errors.MessageDefaultFormat())
	// ErrRangeIsEmpty is the error raised when download failed with "range is
	// empty". This error cannot be retried.
	ErrRangeIsEmpty = errors.Normalize("range is empty", errors.RFCCodeText("BR:KV:ErrRangeIsEmpty"), errors.MessageDefaultFormat())
	// ErrDownloadFailed indicates a generic download error, expected to be
	// retryable.
	ErrDownloadFailed = errors.Normalize("download sst failed", errors.RFCCodeText("BR:KV:ErrDownloadFailed"), errors.MessageDefaultFormat())
	// ErrIngestFailed indicates a generic, retryable ingest error.
	ErrIngestFailed = errors.Normalize("ingest sst failed", errors.RFCCodeText("BR:KV:ErrIngestFailed"), errors.MessageDefaultFormat())
)
