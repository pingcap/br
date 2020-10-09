// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io"
	"net/http"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"

	berrors "github.com/pingcap/br/pkg/errors"
)

// WalkOption is the option of storage.WalkDir.
type WalkOption struct {
	// walk on SubDir of specify directory
	SubDir string
	// ListCount is the number of entries per page.
	//
	// In cloud storages such as S3 and GCS, the files listed and sent in pages.
	// Typically a page contains 1000 files, and if a folder has 3000 descendant
	// files, one would need 3 requests to retrieve all of them. This parameter
	// controls this size. Note that both S3 and GCS limits the maximum to 1000.
	//
	// Typically you want to leave this field unassigned (zero) to use the
	// default value (1000) to minimize the number of requests, unless you want
	// to reduce the possibility of timeout on an extremely slow connection, or
	// perform testing.
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

// Writer is like io.Writer but with Context, create a new writer on top of Uploader with NewUploaderWriter.
type Writer interface {
	// Write writes to buffer and if chunk is filled will upload it
	Write(ctx context.Context, p []byte) (int, error)
	// Close writes final chunk and completes the upload
	Close(ctx context.Context) error
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

	// URI returns the base path as a URI
	URI() string

	// CreateUploader create a uploader that will upload chunks data to storage.
	// It's design for s3 multi-part upload currently. e.g. cdc log backup use this to do multi part upload
	// to avoid generate small fragment files.
	CreateUploader(ctx context.Context, name string) (Uploader, error)
}

// ExternalStorageOptions are backend-independent options provided to New.
type ExternalStorageOptions struct {
	// SendCredentials marks whether to send credentials downstream.
	//
	// This field should be set to false if the credentials are provided to
	// downstream via external key managers, e.g. on K8s or cloud provider.
	SendCredentials bool

	// SkipCheckPath marks whether to skip checking path's existence.
	//
	// This should only be set to true in testing, to avoid interacting with the
	// real world.
	// When this field is false (i.e. path checking is enabled), the New()
	// function will ensure the path referred by the backend exists by
	// recursively creating the folders. This will also throw an error if such
	// operation is impossible (e.g. when the bucket storing the path is missing).
	SkipCheckPath bool

	// HTTPClient to use. The created storage may ignore this field if it is not
	// directly using HTTP (e.g. the local storage).
	HTTPClient *http.Client
}

// Create creates ExternalStorage.
//
// Please consider using `New` in the future.
func Create(ctx context.Context, backend *backup.StorageBackend, sendCreds bool) (ExternalStorage, error) {
	return New(ctx, backend, &ExternalStorageOptions{
		SendCredentials: sendCreds,
		SkipCheckPath:   false,
		HTTPClient:      nil,
	})
}

// New creates an ExternalStorage with options.
func New(ctx context.Context, backend *backup.StorageBackend, opts *ExternalStorageOptions) (ExternalStorage, error) {
	switch backend := backend.Backend.(type) {
	case *backup.StorageBackend_Local:
		if backend.Local == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "local config not found")
		}
		if opts.SkipCheckPath {
			return &LocalStorage{base: backend.Local.Path}, nil
		}
		return NewLocalStorage(backend.Local.Path)
	case *backup.StorageBackend_S3:
		if backend.S3 == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "s3 config not found")
		}
		return newS3Storage(backend.S3, opts)
	case *backup.StorageBackend_Noop:
		return newNoopStorage(), nil
	case *backup.StorageBackend_Gcs:
		if backend.Gcs == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "GCS config not found")
		}
		return newGCSStorage(ctx, backend.Gcs, opts)
	default:
		return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "storage %T is not supported yet", backend)
	}
}
