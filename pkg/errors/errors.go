// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package errors

import "github.com/pingcap/errors"

// BR errors.
var (
	ErrUnknown         = errors.Normalize("internal error", errors.RFCCodeText("BR:Common:ErrUnknown"))
	ErrInvalidArgument = errors.Normalize("invalid argument", errors.RFCCodeText("BR:Common:ErrInvalidArgument"))
	ErrVersionMismatch = errors.Normalize("version mismatch", errors.RFCCodeText("BR:Common:ErrVersionMismatch"))

	ErrPDUpdateFailed    = errors.Normalize("failed to update PD", errors.RFCCodeText("BR:PD:ErrUpdatePDFailed"))
	ErrPDLeaderNotFound  = errors.Normalize("PD leader not found", errors.RFCCodeText("BR:PD:ErrUpdatePDFailed"))
	ErrPDInvalidResponse = errors.Normalize("PD invalid response", errors.RFCCodeText("BR:PD:ErrPDInvalidResponse"))

	ErrBackupUnknown             = errors.Normalize("unknown backup error", errors.RFCCodeText("BR:PD:ErrBackupUnknown"))
	ErrBackupChecksumMismatch    = errors.Normalize("backup checksum mismatch", errors.RFCCodeText("BR:Backup:ErrBackupChecksumMismatch"))
	ErrBackupInvalidRange        = errors.Normalize("backup range invalid", errors.RFCCodeText("BR:Backup:ErrBackupInvalidRange"))
	ErrBackupNoLeader            = errors.Normalize("backup no leader", errors.RFCCodeText("BR:Backup:ErrBackupNoLeader"))
	ErrBackupGCSafepointExceeded = errors.Normalize("backup GC safepoint exceeded", errors.RFCCodeText("BR:Backup:ErrBackupGCSafepointExceeded"))

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

	ErrStorageUnknown       = errors.Normalize("unknown external storage error", errors.RFCCodeText("BR:ExternalStorage:ErrStorageUnknown"))
	ErrStorageInvalidConfig = errors.Normalize("invalid external storage config", errors.RFCCodeText("BR:ExternalStorage:ErrStorageInvalidConfig"))

	ErrPITRInvalidCDCLogFormat = errors.Normalize("invalid cdc log format", errors.RFCCodeText("BR:PITR:ErrPITRInvalidCDCLogFormat"))

	// TiKV errors
	ErrKVUnknown           = errors.Normalize("unknown tikv error", errors.RFCCodeText("BR:PD:ErrKVUnknown"))
	ErrKVClusterIDMismatch = errors.Normalize("tikv cluster ID mismatch", errors.RFCCodeText("BR:PD:ErrKVClusterIDMismatch"))
	ErrKVNotHealth         = errors.Normalize("tikv cluster not health", errors.RFCCodeText("BR:KV:ErrKVNotHealth"))
	// ErrKVEpochNotMatch is the error raised when ingestion failed with "epoch
	// not match". This error is retryable.
	ErrKVEpochNotMatch = errors.Normalize("epoch not match", errors.RFCCodeText("BR:KV:ErrEpochNotMatch"))
	// ErrKVKeyNotInRegion is the error raised when ingestion failed with "key not
	// in region". This error cannot be retried.
	ErrKVKeyNotInRegion = errors.Normalize("key not in region", errors.RFCCodeText("BR:KV:ErrKeyNotInRegion"))
	// ErrKVRewriteRuleNotFound is the error raised when download failed with
	// "rewrite rule not found". This error cannot be retried
	ErrKVRewriteRuleNotFound = errors.Normalize("rewrite rule not found", errors.RFCCodeText("BR:KV:ErrRewriteRuleNotFound"))
	// ErrKVRangeIsEmpty is the error raised when download failed with "range is
	// empty". This error cannot be retried.
	ErrKVRangeIsEmpty = errors.Normalize("range is empty", errors.RFCCodeText("BR:KV:ErrRangeIsEmpty"))
	// ErrKVDownloadFailed indicates a generic download error, expected to be
	// retryable.
	ErrKVDownloadFailed = errors.Normalize("download sst failed", errors.RFCCodeText("BR:KV:ErrDownloadFailed"))
	// ErrKVIngestFailed indicates a generic, retryable ingest error.
	ErrKVIngestFailed = errors.Normalize("ingest sst failed", errors.RFCCodeText("BR:KV:ErrIngestFailed"))
)
