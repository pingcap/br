// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
)

// LocalStorage represents local file system storage.
//
// export for using in tests.
type LocalStorage struct {
	base string
}

func (l *LocalStorage) Write(ctx context.Context, name string, data []byte) error {
	filepath := filepath.Join(l.base, name)
	return ioutil.WriteFile(filepath, data, 0644) // nolint:gosec
	// the backupmeta file _is_ intended to be world-readable.
}

func (l *LocalStorage) Read(ctx context.Context, name string) ([]byte, error) {
	filepath := filepath.Join(l.base, name)
	return ioutil.ReadFile(filepath)
}

// FileExists implement ExternalStorage.FileExists.
func (l *LocalStorage) FileExists(ctx context.Context, name string) (bool, error) {
	filepath := filepath.Join(l.base, name)
	return pathExists(filepath)
}

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The first argument is the file path that can be used in `Open`
// function; the second argument is the size in byte of the file determined
// by path.
func (l *LocalStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
	return filepath.Walk(l.base, func(path string, f os.FileInfo, err error) error {
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

// CreateUploader implenments ExternalStorage interface.
func (l *LocalStorage) CreateUploader(ctx context.Context, name string) (Uploader, error) {
	panic("local storage not support multi-upload")
}

// Open a Reader by file path, path is a relative path to base path.
func (l *LocalStorage) Open(ctx context.Context, path string) (ReadSeekCloser, error) {
	return os.Open(filepath.Join(l.base, path))
}

func pathExists(_path string) (bool, error) {
	_, err := os.Stat(_path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// NewLocalStorage return a LocalStorage at directory `base`.
//
// export for test.
func NewLocalStorage(base string) (*LocalStorage, error) {
	ok, err := pathExists(base)
	if err != nil {
		return nil, err
	}
	if !ok {
		err := mkdirAll(base)
		if err != nil {
			return nil, err
		}
	}
	return &LocalStorage{base: base}, nil
}
