// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	. "github.com/pingcap/check"
)

func (r *testStorageSuite) TestExternalFileWriter(c *C) {
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
		writer, err := storage.Create(ctx, fileName)
		c.Assert(err, IsNil)
		for _, str := range test.content {
			p := []byte(str)
			written, err2 := writer.Write(ctx, p)
			c.Assert(err2, IsNil)
			c.Assert(written, Equals, len(p))
		}
		err = writer.Close(ctx)
		c.Assert(err, IsNil)
		content, err := ioutil.ReadFile(filepath.Join(dir, fileName))
		c.Assert(err, IsNil)
		c.Assert(string(content), Equals, strings.Join(test.content, ""))
		// Sanity check we didn't write past the chunk size
		c.Assert(writer.(*uploaderWriter).buf.Cap(), Equals, hardcodedS3ChunkSize)
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

func (r *testStorageSuite) TestUploaderCompressWriter(c *C) {
	dir := c.MkDir()

	type testcase struct {
		name         string
		content      []string
		chunkSize    int
		compressType CompressType
	}
	testFn := func(test *testcase, c *C) {
		c.Log(test.name)
		backend, err := ParseBackend("local:///"+dir, nil)
		c.Assert(err, IsNil)
		ctx := context.Background()
		storage, err := Create(ctx, backend, true)
		c.Assert(err, IsNil)
		storage = WithCompression(storage, Gzip)
		fileName := strings.ReplaceAll(test.name, " ", "-") + ".txt.gz"
		writer, err := storage.Create(ctx, fileName)
		c.Assert(err, IsNil)
		for _, str := range test.content {
			p := []byte(str)
			written, err2 := writer.Write(ctx, p)
			c.Assert(err2, IsNil)
			c.Assert(written, Equals, len(p))
		}
		err = writer.Close(ctx)
		c.Assert(err, IsNil)
		file, err := os.Open(filepath.Join(dir, fileName))
		c.Assert(err, IsNil)
		var r io.Reader
		switch test.compressType {
		case Gzip:
			r, err = gzip.NewReader(file)
		default:
			c.Fatal("unknown compressType")
		}
		c.Assert(err, IsNil)
		var bf bytes.Buffer
		_, err = bf.ReadFrom(r)
		c.Assert(err, IsNil)
		c.Assert(bf.String(), Equals, strings.Join(test.content, ""))
		// Sanity check we didn't write past the chunk size
		c.Assert(writer.(*uploaderWriter).buf.Cap(), Equals, hardcodedS3ChunkSize)
		c.Assert(file.Close(), IsNil)
	}
	compressTypeArr := []CompressType{Gzip}
	tests := []testcase{
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
		for _, compressType := range compressTypeArr {
			tests[i].compressType = compressType
			testFn(&tests[i], c)
		}
	}
}
