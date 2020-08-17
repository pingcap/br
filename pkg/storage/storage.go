// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
)

// ReadSeekCloser is the interface that groups the basic Read, Seek and Close methods.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// ExternalStorage represents a kind of file system storage.
type ExternalStorage interface {
	// Write file to storage
	Write(ctx context.Context, name string, data []byte) error
	// Read storage file
	Read(ctx context.Context, name string) ([]byte, error)
	// FileExists return true if file exists
	FileExists(ctx context.Context, name string) (bool, error)
	// Open a Reader by file path. path can be either an absolute path or relative path to storage base path
	Open(ctx context.Context, path string) (ReadSeekCloser, error)
	// WalkDir traverse all the files in a dir.
	//
	// fn is the function called for each regular file visited by WalkDir.
	// The argument `path` is the file path that can be used in `Open`
	// function; the argument `size` is the size in byte of the file determined
	// by path.
	WalkDir(ctx context.Context, fn func(path string, size int64) error) error
}

// Create creates ExternalStorage.
func Create(ctx context.Context, backend *backup.StorageBackend, sendCreds bool) (ExternalStorage, error) {
	switch backend := backend.Backend.(type) {
	case *backup.StorageBackend_Local:
		return NewLocalStorage(backend.Local.Path)
	case *backup.StorageBackend_S3:
		if backend.S3 == nil {
			return nil, errors.New("s3 config not found")
		}
		return newS3Storage(backend.S3, sendCreds)
	case *backup.StorageBackend_Noop:
		return newNoopStorage(), nil
	case *backup.StorageBackend_Gcs:
		if backend.Gcs == nil {
			return nil, errors.New("GCS config not found")
		}
		return newGCSStorage(ctx, backend.Gcs, sendCreds)
	default:
		return nil, errors.Errorf("storage %T is not supported yet", backend)
	}
}
