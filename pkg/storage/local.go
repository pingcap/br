// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
)

const (
	localDirPerm  os.FileMode = 0o755
	localFilePerm os.FileMode = 0o644
)

// LocalStorage represents local file system storage.
//
// export for using in tests.
type LocalStorage struct {
	base string
}

// WriteFile writes data to a file to storage.
func (l *LocalStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	path := filepath.Join(l.base, name)
	return ioutil.WriteFile(path, data, localFilePerm)
	// the backup meta file _is_ intended to be world-readable.
}

// ReadFile reads the file from the storage and returns the contents.
func (l *LocalStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	path := filepath.Join(l.base, name)
	return ioutil.ReadFile(path)
}

// FileExists implement ExternalStorage.FileExists.
func (l *LocalStorage) FileExists(ctx context.Context, name string) (bool, error) {
	path := filepath.Join(l.base, name)
	return pathExists(path)
}

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The first argument is the file path that can be used in `Open`
// function; the second argument is the size in byte of the file determined
// by path.
func (l *LocalStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
	base := filepath.Join(l.base, opt.SubDir)
	return filepath.Walk(base, func(path string, f os.FileInfo, err error) error {
		if os.IsNotExist(err) {
			// if path not exists, we should return nil to continue.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		if f == nil || f.IsDir() {
			return nil
		}
		// in mac osx, the path parameter is absolute path; in linux, the path is relative path to execution base dir,
		// so use Rel to convert to relative path to l.base
		path, _ = filepath.Rel(l.base, path)

		size := f.Size()
		// if not a regular file, we need to use os.stat to get the real file size
		if !f.Mode().IsRegular() {
			stat, err := os.Stat(filepath.Join(l.base, path))
			if err != nil {
				return errors.Trace(err)
			}
			size = stat.Size()
		}
		return fn(path, size)
	})
}

// URI returns the base path as an URI with a file:/// prefix.
func (l *LocalStorage) URI() string {
	return "file:///" + l.base
}

type localStorageUploader struct {
	file io.WriteCloser
}

func (l *localStorageUploader) UploadPart(ctx context.Context, data []byte) error {
	_, err := l.file.Write(data)
	return errors.Trace(err)
}

func (l *localStorageUploader) CompleteUpload(ctx context.Context) error {
	return l.file.Close()
}

// CreateUploader implements ExternalStorage interface.
func (l *LocalStorage) CreateUploader(ctx context.Context, name string) (Uploader, error) {
	path := filepath.Join(l.base, name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, localFilePerm)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &localStorageUploader{file: f}, nil
}

// Open a Reader by file path, path is a relative path to base path.
func (l *LocalStorage) Open(ctx context.Context, path string) (ExternalFileReader, error) {
	return os.Open(filepath.Join(l.base, path))
}

// Create implements ExternalStorage interface.
func (l *LocalStorage) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
	uploader, err := l.CreateUploader(ctx, name)
	if err != nil {
		return nil, err
	}
	uploaderWriter := newUploaderWriter(uploader, hardcodedS3ChunkSize, NoCompression)
	return uploaderWriter, nil
}

func pathExists(_path string) (bool, error) {
	_, err := os.Stat(_path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// NewLocalStorage return a LocalStorage at directory `base`.
//
// export for test.
func NewLocalStorage(base string) (*LocalStorage, error) {
	ok, err := pathExists(base)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		err := mkdirAll(base)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return &LocalStorage{base: base}, nil
}
