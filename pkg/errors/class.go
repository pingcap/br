// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package errors

import "github.com/pingcap/errors"

// Error classes
var (
	reg = errors.NewRegistry("BR")

	// classBackup is backup error class
	classBackup = reg.RegisterErrorClass(1, "backup")
	// classRestore is restore error class
	classRestore = reg.RegisterErrorClass(2, "restore")
	// classKV is TiKV related error class
	classKV = reg.RegisterErrorClass(3, "kv")
	// classDB is TiDB related error class
	classDB = reg.RegisterErrorClass(4, "db")
	// classPD is PD related error class
	classPD = reg.RegisterErrorClass(5, "pd")
	// classExternalStorage is external storage error class
	classExternalStorage = reg.RegisterErrorClass(5, "external_storage")
	// classCommon is common error class
	classCommon = reg.RegisterErrorClass(6, "common")
)

// errors
var (
	ErrInvalidArgument = classCommon.DefineError().TextualCode("ErrInvalidArgument").MessageTemplate("invalid argument: %s").Build()
	ErrInternal        = classCommon.DefineError().TextualCode("ErrInternal").MessageTemplate("internal error: %s").Build()
	ErrVersionMismatch = classCommon.DefineError().TextualCode("ErrVersionMismatch").MessageTemplate("version mismatch: %s").Build()

	ErrPDUpdateFailed    = classPD.DefineError().TextualCode("ErrUpdatePDFailed").MessageTemplate("failed to update PD: %s").Build()
	ErrPDLeaderNotFound  = classPD.DefineError().TextualCode("ErrUpdatePDFailed").MessageTemplate("PD leader not found: %s").Build()
	ErrPDInvalidResponse = classPD.DefineError().TextualCode("ErrPDInvalidResponse").MessageTemplate("PD invalid response: %s").Build()

	ErrKVUnknown           = classPD.DefineError().TextualCode("ErrKVUnknown").MessageTemplate("unknown tikv error: %s").Build()
	ErrKVClusterIDMismatch = classPD.DefineError().TextualCode("ErrKVClusterIDMismatch").MessageTemplate("tikv cluster ID mismatch: %s").Build()

	ErrBackupChecksumMismatch    = classBackup.DefineError().TextualCode("ErrBackupChecksumMismatch").MessageTemplate("backup checksum mismatch: %s").Build()
	ErrBackupInvalidRange        = classBackup.DefineError().TextualCode("ErrBackupInvalidRange").MessageTemplate("backup range invalid: %s").Build()
	ErrBackupNoLeader            = classBackup.DefineError().TextualCode("ErrBackupNoLeader").MessageTemplate("backup no leader: %s").Build()
	ErrBackupGCSafepointExceeded = classBackup.DefineError().TextualCode("ErrBackupGCSafepointExceeded").MessageTemplate("backup GC safepoint exceeded: %s").Build()

	ErrRestoreModeMismatch     = classRestore.DefineError().TextualCode("ErrRestoreModeMistmatch").MessageTemplate("restore mode mismatch: %s").Build()
	ErrRestoreRangeMismatch    = classRestore.DefineError().TextualCode("ErrRestoreRangeMismatch").MessageTemplate("restore range mismatch: %s").Build()
	ErrRestoreChecksumMismatch = classRestore.DefineError().TextualCode("ErrRestoreChecksumMismatch").MessageTemplate("restore checksum mismatch: %s").Build()
	ErrRestoreTableIDMismatch  = classRestore.DefineError().TextualCode("ErrRestoreTableIDMismatch").MessageTemplate("restore table ID mismatch: %s").Build()
	ErrRestoreRejectStore      = classRestore.DefineError().TextualCode("ErrRestoreRejectStore").MessageTemplate("failed to restore remove rejected store: %s").Build()
	ErrRestoreNoPeer           = classRestore.DefineError().TextualCode("ErrRestoreNoPeer").MessageTemplate("region does not have peer").Build()
	ErrRestoreSplitFailed      = classRestore.DefineError().TextualCode("ErrRestoreSplitFailed").MessageTemplate("fail to split region: %s").Build()
	ErrRestoreInvalidRewrite   = classRestore.DefineError().TextualCode("ErrRestoreInvalidRewrite").MessageTemplate("invalid rewrite rule: %s").Build()
	ErrRestoreInvalidBackup    = classRestore.DefineError().TextualCode("ErrRestoreInvalidBackup").MessageTemplate("invalid backup: %s").Build()
	ErrRestoreInvalidRange     = classRestore.DefineError().TextualCode("ErrRestoreInvalidRange").MessageTemplate("invalid restore range: %s").Build()

	ErrStorageInvalidConfig = classExternalStorage.DefineError().TextualCode("ErrStorageInvalidConfig").MessageTemplate("invalid external storage config: %s").Build()

	ErrKVNotHealth = classKV.DefineError().TextualCode("ErrKVNotHealth").MessageTemplate("tikv cluster not health: %s").Build()
	// ErrEpochNotMatch is the error raised when ingestion failed with "epoch
	// not match". This error is retryable.
	ErrEpochNotMatch = classKV.DefineError().TextualCode("ErrEpochNotMatch").MessageTemplate("epoch not match").Build()
	// ErrKeyNotInRegion is the error raised when ingestion failed with "key not
	// in region". This error cannot be retried.
	ErrKeyNotInRegion = classKV.DefineError().TextualCode("ErrKeyNotInRegion").MessageTemplate("key not in region").Build()
	// ErrRewriteRuleNotFound is the error raised when download failed with
	// "rewrite rule not found". This error cannot be retried
	ErrRewriteRuleNotFound = classKV.DefineError().TextualCode("ErrRewriteRuleNotFound").MessageTemplate("rewrite rule not found").Build()
	// ErrRangeIsEmpty is the error raised when download failed with "range is
	// empty". This error cannot be retried.
	ErrRangeIsEmpty = classKV.DefineError().TextualCode("ErrRangeIsEmpty").MessageTemplate("range is empty").Build()
	// ErrDownloadFailed indicates a generic download error, expected to be
	// retryable.
	ErrDownloadFailed = classKV.DefineError().TextualCode("ErrDownloadFailed").MessageTemplate("download sst failed").Build()
	// ErrIngestFailed indicates a generic, retryable ingest error.
	ErrIngestFailed = classKV.DefineError().TextualCode("ErrIngestFailed").MessageTemplate("ingest sst failed").Build()
)
