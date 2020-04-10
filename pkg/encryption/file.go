// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"encoding/hex"
	"io/ioutil"

	"github.com/pingcap/errors"
)

type fileBackendOptions struct {
	path string
}

// A key backend that read hex encoded key from a file.
type fileBackend struct {
	path string
}

func newFileBackend(options *fileBackendOptions) (*fileBackend, error) {
	backend := &fileBackend{path: options.path}
	return backend, nil
}

// For file backend, encryptedKey is ignored.
func (backend *fileBackend) GetKey() (key []byte, encryptedKey []byte, err error) {
	key, err = backend.read()
	return key, nil, err
}

// For file backend, encryptedKey is ignored.
func (backend *fileBackend) DecryptKey(encryptedKey []byte) (key []byte, err error) {
	return backend.read()
}

func (backend *fileBackend) read() (key []byte, err error) {
	data, err := ioutil.ReadFile(backend.path)
	if err != nil {
		return nil, errors.Annotatef(err, "fail to get key from file %s", backend.path)
	}
	if data[len(data)-1] != '\n' {
		return nil, errors.New("key file must end with newline")
	}
	key, err = hex.DecodeString(string(data[:len(data)-1]))
	if err != nil {
		return nil, errors.Annotate(err, "failed to decode key from file, the key must be in hex form")
	}
	if len(key) != keySize {
		return nil, errors.Errorf("key size must be %d bytes, got %s bytes", keySize, len(key))
	}
	return key, nil
}
