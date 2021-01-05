// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

	berrors "github.com/pingcap/br/pkg/errors"

	"github.com/pingcap/errors"
)

type withCompression struct {
	ExternalStorage
	compressType CompressType
}

// WithCompression returns an ExternalStorage with compress option
func WithCompression(inner ExternalStorage, compressionType CompressType) ExternalStorage {
	if compressionType == NoCompression {
		return inner
	}
	return &withCompression{ExternalStorage: inner, compressType: compressionType}
}

func (w *withCompression) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
	uploader, err := createUploader(ctx, w.ExternalStorage, name)
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

type compressReader struct {
	io.ReadCloser
}

// nolint:interfacer
func newInterceptReader(fileReader ExternalFileReader, compressType CompressType) (ExternalFileReader, error) {
	if compressType == NoCompression {
		return fileReader, nil
	}
	r, err := newCompressReader(compressType, fileReader)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &compressReader{
		ReadCloser: r,
	}, nil
}

func (r *compressReader) Seek(_ int64, _ int) (int64, error) {
	return int64(0), errors.Annotatef(berrors.ErrStorageInvalidConfig, "compressReader doesn't support Seek now")
}
