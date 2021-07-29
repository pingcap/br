// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package config

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/backup"

	"github.com/pingcap/br/pkg/storage"
)

// SourceDir is the parsed data source directory. It hides the "access-key" and
// "secret-access-key" when dumped as JSON.
type SourceDir struct {
	*backuppb.StorageBackend
}

// String implements fmt.Stringer
func (sd SourceDir) String() string {
	// need the intermediate variable to avoid "cannot call pointer method" compiler error.
	u := storage.FormatBackendURL(sd.StorageBackend)
	return u.String()
}

func (sd *SourceDir) UnmarshalText(text []byte) error {
	var err error
	sd.StorageBackend, err = storage.ParseBackend(string(text), nil)
	return errors.Trace(err)
}

func (sd SourceDir) MarshalJSON() ([]byte, error) {
	return json.Marshal(sd.String())
}

// NewStorage creates a new external storage interface from the storage backend
// URL. It is basically a wrapper around `storage.New()` function.
func (sd SourceDir) NewStorage(ctx context.Context, checkPermissions []storage.Permission) (storage.ExternalStorage, error) {
	return storage.New(
		ctx, sd.StorageBackend,
		&storage.ExternalStorageOptions{CheckPermissions: checkPermissions},
	)
}

// NewSourceDirFromPath creates a SourceDir from a file path.
// This should only be used in test.
func NewSourceDirFromPath(path string) SourceDir {
	return SourceDir{
		StorageBackend: &backuppb.StorageBackend{
			Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: path}},
		},
	}
}
