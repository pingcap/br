package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
)

// CompressType represents the type of compression.
type CompressType uint8

const (
	// NoCompression won't compress given bytes.
	NoCompression CompressType = iota
	// Gzip will compress given bytes in gzip format.
	Gzip
)

type interceptBuffer interface {
	io.WriteCloser
	Len() int
	Cap() int
	Bytes() []byte
	Flush() error
	Reset()
}

func newInterceptBuffer(chunkSize int, compressType CompressType) interceptBuffer {
	switch compressType {
	case NoCompression:
		return newNoCompressionBuffer(chunkSize)
	case Gzip:
		return newGzipBuffer(chunkSize)
	default:
		return nil
	}
}

type noCompressionBuffer struct {
	*bytes.Buffer
}

func (b *noCompressionBuffer) Flush() error {
	return nil
}

func (b *noCompressionBuffer) Close() error {
	return nil
}

func newNoCompressionBuffer(chunkSize int) *noCompressionBuffer {
	return &noCompressionBuffer{bytes.NewBuffer(make([]byte, 0, chunkSize))}
}

type gzipCompressBuffer struct {
	*bytes.Buffer
	compressWriter *gzip.Writer
	len            int
	cap            int
}

func (b *gzipCompressBuffer) Write(p []byte) (int, error) {
	written, err := b.compressWriter.Write(p)
	b.len += written
	return written, err
}

func (b *gzipCompressBuffer) Len() int {
	return b.len
}

func (b *gzipCompressBuffer) Cap() int {
	return b.cap
}

func (b *gzipCompressBuffer) Reset() {
	b.len = 0
	b.Buffer.Reset()
}

func (b *gzipCompressBuffer) Flush() error {
	return b.compressWriter.Flush()
}

func (b *gzipCompressBuffer) Close() error {
	return b.compressWriter.Close()
}

func newGzipBuffer(chunkSize int) *gzipCompressBuffer {
	bf := bytes.NewBuffer(make([]byte, 0, chunkSize))
	return &gzipCompressBuffer{
		Buffer:         bf,
		len:            0,
		cap:            chunkSize,
		compressWriter: gzip.NewWriter(bf),
	}
}

type uploaderWriter struct {
	buf      interceptBuffer
	uploader Uploader
}

func (u *uploaderWriter) Write(ctx context.Context, p []byte) (int, error) {
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
		u.buf.Flush()
		err := u.uploadChunk(ctx)
		if err != nil {
			return 0, err
		}
	}
	w, err := u.buf.Write(p)
	bytesWritten += w
	return bytesWritten, err
}

func (u *uploaderWriter) uploadChunk(ctx context.Context) error {
	if u.buf.Len() == 0 {
		return nil
	}
	b := u.buf.Bytes()
	u.buf.Reset()
	return u.uploader.UploadPart(ctx, b)
}

func (u *uploaderWriter) Close(ctx context.Context) error {
	u.buf.Close()
	err := u.uploadChunk(ctx)
	if err != nil {
		return err
	}
	return u.uploader.CompleteUpload(ctx)
}

// NewUploaderWriter wraps the Writer interface over an uploader.
func NewUploaderWriter(uploader Uploader, chunkSize int, compressType CompressType) Writer {
	return newUploaderWriter(uploader, chunkSize, compressType)
}

// newUploaderWriter is used for testing only.
func newUploaderWriter(uploader Uploader, chunkSize int, compressType CompressType) *uploaderWriter {
	return &uploaderWriter{
		uploader: uploader,
		buf:      newInterceptBuffer(chunkSize, compressType)}
}

// BufferWriter is a Writer implementation on top of bytes.Buffer that is useful for testing.
type BufferWriter struct {
	buf *bytes.Buffer
}

// Write delegates to bytes.Buffer.
func (u *BufferWriter) Write(ctx context.Context, p []byte) (int, error) {
	return u.buf.Write(p)
}

// Close delegates to bytes.Buffer.
func (u *BufferWriter) Close(ctx context.Context) error {
	// noop
	return nil
}

// Bytes delegates to bytes.Buffer.
func (u *BufferWriter) Bytes() []byte {
	return u.buf.Bytes()
}

// String delegates to bytes.Buffer.
func (u *BufferWriter) String() string {
	return u.buf.String()
}

// Reset delegates to bytes.Buffer.
func (u *BufferWriter) Reset() {
	u.buf.Reset()
}

// NewBufferWriter creates a Writer that simply writes to a buffer (useful for testing).
func NewBufferWriter() *BufferWriter {
	return &BufferWriter{buf: &bytes.Buffer{}}
}
