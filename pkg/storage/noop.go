// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import "context"

type noopStorage struct{}

// Write file to storage
func (*noopStorage) Write(ctx context.Context, name string, data []byte) error {
	return nil
}

// Read storage file
func (*noopStorage) Read(ctx context.Context, name string) ([]byte, error) {
	return []byte{}, nil
}

// FileExists return true if file exists
func (*noopStorage) FileExists(ctx context.Context, name string) (bool, error) {
	return false, nil
}

func newNoopStorage() *noopStorage {
	return &noopStorage{}
}
