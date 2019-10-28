package utils

import (
	"github.com/pingcap/errors"
	"io/ioutil"
	"net/url"
	"path"
)

// ExternalStorage represents a kind of file system storage
type ExternalStorage interface {
	// Write file to storage
	Write(name string, data []byte) error
	// Read storage file
	Read(name string) ([]byte, error)
}

// CreateStorage create ExternalStorage
func CreateStorage(rawURL string) (ExternalStorage, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch u.Scheme {
	case "local":
		return newLocalStorage(u.Path), nil
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

func newLocalStorage(base string) *LocalStorage {
	return &LocalStorage{base}
}
