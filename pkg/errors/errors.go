// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package errors

import "github.com/pingcap/errors"

// BR errors
var (
	// Common errors
	ErrInvalidArgument = errors.Normalize("invalid argument", errors.RFCCodeText("BR:Common:ErrInvalidArgument"))
	ErrInternal        = errors.Normalize("internal error", errors.RFCCodeText("BR:Common:ErrInternal"))
	ErrVersionMismatch = errors.Normalize("version mismatch", errors.RFCCodeText("BR:Common:ErrVersionMismatch"))

	// PD errors
	ErrPDUpdateFailed      = errors.Normalize("failed to update PD", errors.RFCCodeText("BR:PD:ErrUpdatePDFailed"))
	ErrPDLeaderNotFound    = errors.Normalize("PD leader not found", errors.RFCCodeText("BR:PD:ErrUpdatePDFailed"))
	ErrPDInvalidResponse   = errors.Normalize("PD invalid response", errors.RFCCodeText("BR:PD:ErrPDInvalidResponse"))
	ErrKVUnknown           = errors.Normalize("unknown tikv error", errors.RFCCodeText("BR:PD:ErrKVUnknown"))
	ErrKVClusterIDMismatch = errors.Normalize("tikv cluster ID mismatch", errors.RFCCodeText("BR:PD:ErrKVClusterIDMismatch"))

	// Backup errors
	ErrBackupChecksumMismatch    = errors.Normalize("backup checksum mismatch", errors.RFCCodeText("BR:Backup:ErrBackupChecksumMismatch"))
	ErrBackupInvalidRange        = errors.Normalize("backup range invalid", errors.RFCCodeText("BR:Backup:ErrBackupInvalidRange"))
	ErrBackupNoLeader            = errors.Normalize("backup no leader", errors.RFCCodeText("BR:Backup:ErrBackupNoLeader"))
	ErrBackupGCSafepointExceeded = errors.Normalize("backup GC safepoint exceeded", errors.RFCCodeText("BR:Backup:ErrBackupGCSafepointExceeded"))

	// Restore errors
	ErrRestoreModeMismatch     = errors.Normalize("restore mode mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreModeMistmatch"))
	ErrRestoreRangeMismatch    = errors.Normalize("restore range mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreRangeMismatch"))
	ErrRestoreChecksumMismatch = errors.Normalize("restore checksum mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreChecksumMismatch"))
	ErrRestoreTableIDMismatch  = errors.Normalize("restore table ID mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreTableIDMismatch"))
	ErrRestoreRejectStore      = errors.Normalize("failed to restore remove rejected store", errors.RFCCodeText("BR:Restore:ErrRestoreRejectStore"))
	ErrRestoreNoPeer           = errors.Normalize("region does not have peer", errors.RFCCodeText("BR:Restore:ErrRestoreNoPeer"))
	ErrRestoreSplitFailed      = errors.Normalize("fail to split region", errors.RFCCodeText("BR:Restore:ErrRestoreSplitFailed"))
	ErrRestoreInvalidRewrite   = errors.Normalize("invalid rewrite rule", errors.RFCCodeText("BR:Restore:ErrRestoreInvalidRewrite"))
	ErrRestoreInvalidBackup    = errors.Normalize("invalid backup", errors.RFCCodeText("BR:Restore:ErrRestoreInvalidBackup"))
	ErrRestoreInvalidRange     = errors.Normalize("invalid restore range", errors.RFCCodeText("BR:Restore:ErrRestoreInvalidRange"))

	// ExternalStorage errors
	ErrStorageInvalidConfig = errors.Normalize("invalid external storage config", errors.RFCCodeText("BR:ExternalStorage:ErrStorageInvalidConfig"))

	// TiKV errors
	ErrKVNotHealth = errors.Normalize("tikv cluster not health", errors.RFCCodeText("BR:KV:ErrKVNotHealth"))
	// ErrEpochNotMatch is the error raised when ingestion failed with "epoch
	// not match". This error is retryable.
	ErrEpochNotMatch = errors.Normalize("epoch not match", errors.RFCCodeText("BR:KV:ErrEpochNotMatch"))
	// ErrKeyNotInRegion is the error raised when ingestion failed with "key not
	// in region". This error cannot be retried.
	ErrKeyNotInRegion = errors.Normalize("key not in region", errors.RFCCodeText("BR:KV:ErrKeyNotInRegion"))
	// ErrRewriteRuleNotFound is the error raised when download failed with
	// "rewrite rule not found". This error cannot be retried
	ErrRewriteRuleNotFound = errors.Normalize("rewrite rule not found", errors.RFCCodeText("BR:KV:ErrRewriteRuleNotFound"))
	// ErrRangeIsEmpty is the error raised when download failed with "range is
	// empty". This error cannot be retried.
	ErrRangeIsEmpty = errors.Normalize("range is empty", errors.RFCCodeText("BR:KV:ErrRangeIsEmpty"))
	// ErrDownloadFailed indicates a generic download error, expected to be
	// retryable.
	ErrDownloadFailed = errors.Normalize("download sst failed", errors.RFCCodeText("BR:KV:ErrDownloadFailed"))
	// ErrIngestFailed indicates a generic, retryable ingest error.
	ErrIngestFailed = errors.Normalize("ingest sst failed", errors.RFCCodeText("BR:KV:ErrIngestFailed"))
)
