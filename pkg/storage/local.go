// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"github.com/pingcap/errors"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

// localStorage represents local file system storage.
type localStorage struct {
	base string
}

func (l *localStorage) Write(ctx context.Context, name string, data []byte) error {
	filepath := path.Join(l.base, name)
	return ioutil.WriteFile(filepath, data, 0644) // nolint:gosec
	// the backupmeta file _is_ intended to be world-readable.
}

func (l *localStorage) Read(ctx context.Context, name string) ([]byte, error) {
	filepath := path.Join(l.base, name)
	return ioutil.ReadFile(filepath)
}

// FileExists implement ExternalStorage.FileExists.
func (l *localStorage) FileExists(ctx context.Context, name string) (bool, error) {
	filepath := path.Join(l.base, name)
	return pathExists(filepath)
}

func (l *localStorage) WalkDir(ctx context.Context, fn func(string, int64) error) error {
	return filepath.Walk(l.base, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return errors.Trace(err)
		}

		if f == nil || f.IsDir() {
			return nil
		}

		return fn(f.Name(), f.Size())
	})
}

func (l *localStorage) Open(ctx context.Context, name string) (ReadSeekCloser, error) {
	return os.Open(path.Join(l.base, name))
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

// NewLocalStorage return a localStorage at directory `base`
// export for test use
func NewLocalStorage(base string) (*localStorage, error) {
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
	return &localStorage{base: base}, nil
}

type LocalReader struct {
}
