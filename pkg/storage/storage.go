// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"

	berrors "github.com/pingcap/br/pkg/errors"
)

// WalkOption is the option of storage.WalkDir.
type WalkOption struct {
	// walk on SubDir of specify directory
	SubDir string
	// number of list count, default 1000
	ListCount int64
}

// ReadSeekCloser is the interface that groups the basic Read, Seek and Close methods.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// Uploader upload file with chunks.
type Uploader interface {
	// UploadPart upload part of file data to storage
	UploadPart(ctx context.Context, data []byte) error
	// CompleteUpload make the upload data to a complete file
	CompleteUpload(ctx context.Context) error
}

// ExternalStorage represents a kind of file system storage.
type ExternalStorage interface {
	// Write file to storage
	Write(ctx context.Context, name string, data []byte) error
	// Read storage file
	Read(ctx context.Context, name string) ([]byte, error)
	// FileExists return true if file exists
	FileExists(ctx context.Context, name string) (bool, error)
	// Open a Reader by file path. path is relative path to storage base path
	Open(ctx context.Context, path string) (ReadSeekCloser, error)
	// WalkDir traverse all the files in a dir.
	//
	// fn is the function called for each regular file visited by WalkDir.
	// The argument `path` is the file path that can be used in `Open`
	// function; the argument `size` is the size in byte of the file determined
	// by path.
	WalkDir(ctx context.Context, opt *WalkOption, fn func(path string, size int64) error) error

	// CreateUploader create a uploader that will upload chunks data to storage.
	// It's design for s3 multi-part upload currently. e.g. cdc log backup use this to do multi part upload
	// to avoid generate small fragment files.
	CreateUploader(ctx context.Context, name string) (Uploader, error)
}

// Create creates ExternalStorage.
func Create(ctx context.Context, backend *backup.StorageBackend, sendCreds bool) (ExternalStorage, error) {
	switch backend := backend.Backend.(type) {
	case *backup.StorageBackend_Local:
		return NewLocalStorage(backend.Local.Path)
	case *backup.StorageBackend_S3:
		if backend.S3 == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "s3 config not found")
		}
		return NewS3Storage(backend.S3, sendCreds)
	case *backup.StorageBackend_Noop:
		return newNoopStorage(), nil
	case *backup.StorageBackend_Gcs:
		if backend.Gcs == nil {
			return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "GCS config not found")
		}
		return newGCSStorage(ctx, backend.Gcs, sendCreds)
	default:
		return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "storage %T is not supported yet", backend)
	}
}
