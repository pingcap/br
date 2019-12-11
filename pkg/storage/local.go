package storage

import (
	"io/ioutil"
	"os"
	"path"
)

// localStorage represents local file system storage
type localStorage struct {
	base string
}

func (l *localStorage) Write(name string, data []byte) error {
	filepath := path.Join(l.base, name)
	return ioutil.WriteFile(filepath, data, 0644)
}

func (l *localStorage) Read(name string) ([]byte, error) {
	filepath := path.Join(l.base, name)
	return ioutil.ReadFile(filepath)
}

// FileExists implement ExternalStorage.FileExists
func (l *localStorage) FileExists(name string) bool {
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

func newLocalStorage(base string) (*localStorage, error) {
	ok := pathExists(base)
	if !ok {
		err := mkdirAll(base)
		if err != nil {
			return nil, err
		}
	}
	return &localStorage{base: base}, nil
}
