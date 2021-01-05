// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"io/ioutil"

	"github.com/pingcap/errors"
)

type withCompression struct {
	ExternalStorage
	compressType CompressType
}

// UnwrapCompression unwraps withCompression to original ExternalStorage
func UnwrapCompression(storage ExternalStorage) ExternalStorage {
	if compressExt, ok := storage.(*withCompression); ok {
		return UnwrapCompression(compressExt.ExternalStorage)
	}
	return storage
}

// WithCompression returns an ExternalStorage with compress option
func WithCompression(inner ExternalStorage, compressionType CompressType) ExternalStorage {
	if compressionType == NoCompression {
		return UnwrapCompression(inner)
	}
	return &withCompression{ExternalStorage: UnwrapCompression(inner), compressType: compressionType}
}

func (w *withCompression) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
	uploader, err := CreateUploader(ctx, w.ExternalStorage, name)
	if err != nil {
		return nil, err
	}
	uploaderWriter := newUploaderWriter(uploader, hardcodedS3ChunkSize, w.compressType)
	return uploaderWriter, nil
}

func (w *withCompression) Open(ctx context.Context, path string) (ExternalFileReader, error) {
	fileReader, err := w.ExternalStorage.Open(ctx, path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	uncompressReader, err := newInterceptReader(fileReader, w.compressType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return uncompressReader, nil
}

func (w *withCompression) WriteFile(ctx context.Context, name string, data []byte) error {
	bf := bytes.NewBuffer(make([]byte, 0, len(data)))
	compressBf := newCompressWriter(w.compressType, bf)
	_, err := compressBf.Write(data)
	if err != nil {
		return errors.Trace(err)
	}
	err = compressBf.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return w.ExternalStorage.WriteFile(ctx, name, bf.Bytes())
}

func (w *withCompression) ReadFile(ctx context.Context, name string) ([]byte, error) {
	data, err := w.ExternalStorage.ReadFile(ctx, name)
	if err != nil {
		return data, errors.Trace(err)
	}
	bf := bytes.NewBuffer(data)
	compressBf, err := newCompressReader(w.compressType, bf)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(compressBf)
}
