package utils

import (
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"syscall"

	"github.com/pingcap/errors"
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

// CreateStorage create ExternalStorage
func CreateStorage(rawURL string) (ExternalStorage, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch u.Scheme {
	case "local":
		return newLocalStorage(u.Path)
	case "noop":
		return newNoopStorage(), nil
	default:
		return nil, errors.Errorf("storage %s not support yet", u.Scheme)
	}
}

// LocalStorage represents local file system storage
type LocalStorage struct {
	base string
}

func (l *LocalStorage) Write(name string, data []byte) error {
	filepath := path.Join(l.base, name)
	return ioutil.WriteFile(filepath, data, 0644)
}

func (l *LocalStorage) Read(name string) ([]byte, error) {
	filepath := path.Join(l.base, name)
	return ioutil.ReadFile(filepath)
}

// FileExists implement ExternalStorage.FileExists
func (l *LocalStorage) FileExists(name string) bool {
	filepath := path.Join(l.base, name)
	return pathExists(filepath)
}

func pathExists(_path string) bool {
	_, err := os.Stat(_path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func newLocalStorage(base string) (*LocalStorage, error) {
	ok := pathExists(base)
	if !ok {
		mask := syscall.Umask(0)
		defer syscall.Umask(mask)
		err := os.MkdirAll(base, 0755)
		if err != nil {
			return nil, err
		}
	}
	return &LocalStorage{base}, nil
}

type noopStorage struct{}

// Write file to storage
func (*noopStorage) Write(name string, data []byte) error {
	return nil
}

// Read storage file
func (*noopStorage) Read(name string) ([]byte, error) {
	return []byte{}, nil
}

// FileExists return true if file exists
func (*noopStorage) FileExists(name string) bool {
	return false
}

func newNoopStorage() *noopStorage {
	return &noopStorage{}
}
