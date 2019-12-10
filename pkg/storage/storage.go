package storage

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
)

// ExternalStorage represents a kind of file system storage
type ExternalStorage interface {
	// Write file to storage
	Write(name string, data []byte) error
	// Read storage file
	Read(name string) ([]byte, error)
	// FileExists return true if file exists
	FileExists(name string) bool
}

// Create creates ExternalStorage
func Create(backend *backup.StorageBackend) (ExternalStorage, error) {
	switch backend := backend.Backend.(type) {
	case *backup.StorageBackend_Local:
		return newLocalStorage(backend.Local.Path)
	case *backup.StorageBackend_Noop:
		return newNoopStorage(), nil
	default:
		return nil, errors.Errorf("storage %T is not supported yet", backend)
	}
}
