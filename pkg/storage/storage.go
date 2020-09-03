// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"

	"github.com/pingcap/kvproto/pkg/backup"

	berrors "github.com/pingcap/br/pkg/errors"
)

// ExternalStorage represents a kind of file system storage.
type ExternalStorage interface {
	// Write file to storage
	Write(ctx context.Context, name string, data []byte) error
	// Read storage file
	Read(ctx context.Context, name string) ([]byte, error)
	// FileExists return true if file exists
	FileExists(ctx context.Context, name string) (bool, error)
}

// Create creates ExternalStorage.
func Create(ctx context.Context, backend *backup.StorageBackend, sendCreds bool) (ExternalStorage, error) {
	switch backend := backend.Backend.(type) {
	case *backup.StorageBackend_Local:
		return newLocalStorage(backend.Local.Path)
	case *backup.StorageBackend_S3:
		if backend.S3 == nil {
			return nil, berrors.ErrStorageInvalidConfig.GenWithStackByArgs("s3 config not found")
		}
		return newS3Storage(backend.S3, sendCreds)
	case *backup.StorageBackend_Noop:
		return newNoopStorage(), nil
	case *backup.StorageBackend_Gcs:
		if backend.Gcs == nil {
			return nil, berrors.ErrStorageInvalidConfig.GenWithStackByArgs("GCS config not found")
		}
		return newGCSStorage(ctx, backend.Gcs, sendCreds)
	default:
		return nil, berrors.ErrStorageInvalidConfig.GenWithStack("storage %T is not supported yet", backend)
	}
}
