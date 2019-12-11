// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"

	"gocloud.dev/blob"
)

// RemoteStorage info for remote storage
type RemoteStorage struct {
	bucket *blob.Bucket
}

// Write write to remote storage
func (rs *RemoteStorage) Write(file string, data []byte) error {
	ctx := context.Background()

	// Open the key for writing with the default options.
	err := rs.bucket.WriteAll(ctx, file, data, nil)
	if err != nil {
		return err
	}
	return nil
}

// Read read file from remote storage
func (rs *RemoteStorage) Read(file string) ([]byte, error) {
	ctx := context.Background()
	// Read from the key.
	return rs.bucket.ReadAll(ctx, file)
}

// FileExists check if file exists on remote storage
func (rs *RemoteStorage) FileExists(file string) (bool, error) {
	ctx := context.Background()
	// Check the key.
	return rs.bucket.Exists(ctx, file)
}
