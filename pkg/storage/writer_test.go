// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"io/ioutil"
	"path/filepath"
	"strings"

	. "github.com/pingcap/check"
)

type UploaderWriter struct {
	buf      *bytes.Buffer
	uploader Uploader
}

func (u *UploaderWriter) Write(ctx context.Context, p []byte) (int, error) {
	bytesWritten := 0
	for u.buf.Len()+len(p) > u.buf.Cap() {
		// We won't fit p in this chunk

		// Is this chunk full?
		chunkToFill := u.buf.Cap() - u.buf.Len()
		if chunkToFill > 0 {
			// It's not full so we write enough of p to fill it
			prewrite := p[0:chunkToFill]
			w, err := u.buf.Write(prewrite)
			bytesWritten += w
			if err != nil {
				return bytesWritten, err
			}
			p = p[w:]
		}
		err := u.uploadChunk(ctx)
		if err != nil {
			return 0, err
		}
	}
	w, err := u.buf.Write(p)
	bytesWritten += w
	return bytesWritten, err
}

func (u *UploaderWriter) uploadChunk(ctx context.Context) error {
	if u.buf.Len() == 0 {
		return nil
	}
	b := u.buf.Bytes()
	u.buf.Reset()
	err := u.uploader.UploadPart(ctx, b)
	if err != nil {
		return err
	}
	return nil
}

func (u *UploaderWriter) Close(ctx context.Context) error {
	err := u.uploadChunk(ctx)
	if err != nil {
		return err
	}
	return u.uploader.CompleteUpload(ctx)
}

func NewUploaderWriter(uploader Uploader, chunkSize int) *UploaderWriter {
	return &UploaderWriter{
		uploader: uploader,
		buf:      bytes.NewBuffer(make([]byte, 0, chunkSize))}
}

func (r *testStorageSuite) TestUploaderWriter(c *C) {
	dir := c.MkDir()

	type testcase struct {
		name      string
		content   []string
		chunkSize int
	}
	testFn := func(test *testcase, c *C) {
		c.Log(test.name)
		backend, err := ParseBackend("local:///"+dir, nil)
		c.Assert(err, IsNil)
		ctx := context.Background()
		storage, err := Create(ctx, backend, true)
		c.Assert(err, IsNil)
		fileName := strings.ReplaceAll(test.name, " ", "-") + ".txt"
		uploader, err := storage.CreateUploader(ctx, fileName)
		c.Assert(err, IsNil)
		writer := NewUploaderWriter(uploader, test.chunkSize)
		for _, str := range test.content {
			p := []byte(str)
			written, err := writer.Write(ctx, p)
			c.Assert(err, IsNil)
			c.Assert(written, Equals, len(p))
		}
		err = writer.Close(ctx)
		c.Assert(err, IsNil)
		content, err := ioutil.ReadFile(filepath.Join(dir, fileName))
		c.Assert(err, IsNil)
		c.Assert(string(content), Equals, strings.Join(test.content, ""))
		// Sanity check we didn't write past the chunk size
		c.Assert(writer.buf.Cap(), Equals, test.chunkSize)
	}
	tests := []testcase{
		{
			name:      "short and sweet",
			content:   []string{"hi"},
			chunkSize: 5,
		},
		{
			name: "long text small chunks",
			content: []string{
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
			},
			chunkSize: 5,
		},
		{
			name: "long text medium chunks",
			content: []string{
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
			},
			chunkSize: 30,
		},
		{
			name: "long text large chunks",
			content: []string{
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
			},
			chunkSize: 500,
		},
	}
	for i := range tests {
		testFn(&tests[i], c)
	}
}
