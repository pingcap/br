// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
)

type noopStorage struct{}

// WriteFile file to storage.
func (*noopStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	return nil
}

// ReadFile storage file.
func (*noopStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return []byte{}, nil
}

// FileExists return true if file exists.
func (*noopStorage) FileExists(ctx context.Context, name string) (bool, error) {
	return false, nil
}

// Open a Reader by file path.
func (*noopStorage) Open(ctx context.Context, path string) (ExternalFileReader, error) {
	return noopReader{}, nil
}

// WalkDir traverse all the files in a dir.
func (*noopStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
	return nil
}

func (s *noopStorage) URI() string {
	return "noop:///"
}

// Create implements ExternalStorage interface.
func (*noopStorage) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
	panic("noop storage not support multi-upload")
}

func newNoopStorage() *noopStorage {
	return &noopStorage{}
}

type noopReader struct{}

func (noopReader) Read(p []byte) (n int, err error) {
	return 0, nil
}

func (noopReader) Close() error {
	return nil
}

func (noopReader) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}
