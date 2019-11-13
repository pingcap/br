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

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func newLocalStorage(base string) (*LocalStorage, error) {
	ok, _ := pathExists(base)
	if !ok {
		mask := syscall.Umask(0)
		defer syscall.Umask(mask)
		err := os.MkdirAll(base, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	return &LocalStorage{base}, nil
}
