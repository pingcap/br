// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage_test

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

	. "github.com/pingcap/br/pkg/storage"
)

type uploadWriterSuite struct{}

var _ = Suite(&uploadWriterSuite{})

// uploaderWithSizeCheck is an Uploader with sanity check that the data we write are of correct size.
type uploaderWithSizeCheck struct {
	inner         Uploader
	c             *C
	expectedSizes []int
}

func (u *uploaderWithSizeCheck) UploadPart(ctx context.Context, data []byte) error {
	u.c.Assert(len(u.expectedSizes), Greater, 0, Commentf("unexpected write of %d more bytes", len(data)))
	u.c.Assert(len(data), Equals, u.expectedSizes[0], Commentf("len(u.expectedSizes) = %d", len(u.expectedSizes)))
	// don't use HasLen, we don't want to know the content of data.
	u.expectedSizes = u.expectedSizes[1:]
	return u.inner.UploadPart(ctx, data)
}

func (u *uploaderWithSizeCheck) CompleteUpload(ctx context.Context) error {
	u.c.Assert(u.expectedSizes, HasLen, 0) // fails if not all parts are uploaded.
	return u.inner.CompleteUpload(ctx)
}

func (r *uploadWriterSuite) TestUploaderWriter(c *C) {
	dir := c.MkDir()

	type testcase struct {
		name          string
		content       []string
		chunkSize     int
		expectedSizes []int
	}
	testFn := func(test *testcase, c *C) {
		c.Log(test.name)
		ctx := context.Background()
		storage, err := NewLocalStorage(dir)
		c.Assert(err, IsNil)
		fileName := strings.ReplaceAll(test.name, " ", "-") + ".txt"
		uploader, err := storage.CreateUploader(ctx, fileName)
		c.Assert(err, IsNil)
		uploader = &uploaderWithSizeCheck{
			inner:         uploader,
			c:             c,
			expectedSizes: test.expectedSizes,
		}
		writer, err := NewUploaderWriter(uploader, &UploaderWriterOptions{FirstPartSize: test.chunkSize})
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
	}
	tests := []testcase{
		{
			name:          "short and sweet",
			content:       []string{"hi"},
			chunkSize:     5,
			expectedSizes: []int{2},
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
			chunkSize:     5,
			expectedSizes: []int{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 1},
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
			chunkSize:     30,
			expectedSizes: []int{30, 30, 6},
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
			chunkSize:     500,
			expectedSizes: []int{66},
		},
	}
	for i := range tests {
		testFn(&tests[i], c)
	}
}

func (r *uploadWriterSuite) TestUploaderCompressWriter(c *C) {
	dir := c.MkDir()

	type testcase struct {
		name          string
		content       []string
		options       UploaderWriterOptions
		expectedSizes []int
	}
	testFn := func(test *testcase, c *C) {
		c.Log(test.name)
		ctx := context.Background()
		storage, err := NewLocalStorage(dir)
		c.Assert(err, IsNil)
		fileName := strings.ReplaceAll(test.name, " ", "-") + ".txt.gz"
		uploader, err := storage.CreateUploader(ctx, fileName)
		c.Assert(err, IsNil)
		uploader = &uploaderWithSizeCheck{
			inner:         uploader,
			c:             c,
			expectedSizes: test.expectedSizes,
		}
		writer, err := NewUploaderWriter(uploader, &test.options)
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
		switch test.options.CompressType {
		case Gzip:
			r, err = gzip.NewReader(file)
		default:
			c.Fatalf("unknown compressType %d", test.options.CompressType)
		}
		c.Assert(err, IsNil)
		var bf bytes.Buffer
		_, err = bf.ReadFrom(r)
		c.Assert(err, IsNil)
		c.Assert(bf.String(), Equals, strings.Join(test.content, ""))
		c.Assert(file.Close(), IsNil)
	}
	tests := []testcase{
		{
			name: "long text medium chunks",
			content: []string{
				strings.Repeat("hello world!", 300),
				"hello world?",
				strings.Repeat("h", 1000),
			},
			options: UploaderWriterOptions{
				CompressType:  Gzip,
				CompressLevel: gzip.BestSpeed,
				FirstPartSize: 30,
			},
			expectedSizes: []int{30, 30, 30, 16},
		},
		{
			name: "long text large chunks",
			content: []string{
				strings.Repeat("hello world!", 300),
				"hello world?",
				strings.Repeat("h", 1000),
			},
			options: UploaderWriterOptions{
				CompressType:  Gzip,
				CompressLevel: gzip.BestSpeed,
				FirstPartSize: 500,
			},
			expectedSizes: []int{106},
		},
	}
	for i := range tests {
		testFn(&tests[i], c)
	}
}

func (r *uploadWriterSuite) TestInflation(c *C) {
	ctx := context.Background()

	dir := c.MkDir()
	storage, err := NewLocalStorage(dir)
	c.Assert(err, IsNil)
	uploader, err := storage.CreateUploader(ctx, "test.bin")
	c.Assert(err, IsNil)
	uploader = &uploaderWithSizeCheck{
		inner:         uploader,
		c:             c,
		expectedSizes: []int{100, 110, 121, 133, 146, 160, 176, 193, 61},
	}

	writer, err := NewUploaderWriter(uploader, &UploaderWriterOptions{
		FirstPartSize:            100,
		PartSizeInflationDivisor: 10, // x[n] = x[n-1] * (1 + 1/10)
	})
	c.Assert(err, IsNil)

	data := make([]byte, 1200)

	written, err := writer.Write(ctx, data)
	c.Assert(err, IsNil)
	c.Assert(written, Equals, len(data))

	c.Assert(writer.Close(ctx), IsNil)

	content, err := storage.Read(ctx, "test.bin")
	c.Assert(err, IsNil)
	c.Assert(content, BytesEquals, data)
}

func (r *uploadWriterSuite) TestErrorChecking(c *C) {
	ctx := context.Background()
	backend, err := ParseBackend("noop://", nil)
	c.Assert(err, IsNil)
	storage, err := New(ctx, backend, nil)
	c.Assert(err, IsNil)
	uploader, err := storage.CreateUploader(ctx, "na")
	c.Assert(err, IsNil)

	_, err = NewUploaderWriter(uploader, &UploaderWriterOptions{
		FirstPartSize: -1,
	})
	c.Assert(err, ErrorMatches, ".*FirstPartSize must be positive, not -1.*")

	_, err = NewUploaderWriter(uploader, &UploaderWriterOptions{
		FirstPartSize:            100,
		PartSizeInflationDivisor: -2,
	})
	c.Assert(err, ErrorMatches, ".*PartSizeInflatorDivisor must be non-negative, not -2.*")

	_, err = NewUploaderWriter(uploader, &UploaderWriterOptions{})
	c.Assert(err, ErrorMatches, ".*FirstPartSize must be positive, not 0.*")

	_, err = NewUploaderWriter(uploader, &UploaderWriterOptions{
		FirstPartSize: 100,
		CompressType:  Gzip,
		CompressLevel: 999,
	})
	c.Assert(err, ErrorMatches, "gzip: invalid compression level: 999.*")

	_, err = NewUploaderWriter(uploader, &UploaderWriterOptions{
		FirstPartSize: 100,
		CompressType:  255,
	})
	c.Assert(err, ErrorMatches, ".*unsupported compression type 255.*")
}
