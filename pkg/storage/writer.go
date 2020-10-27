package storage

import (
	"bytes"
	"compress/gzip"
	"context"
)

type interceptBuffer struct {
	*bytes.Buffer
	compressWriter *gzip.Writer
	len int
	cap int
}

func (b *interceptBuffer) Write(p []byte) (int, error) {
	if b.compressWriter == nil {
		return b.Buffer.Write(p)
	}
	written, err := b.compressWriter.Write(p)
	b.len += written
	return written, err
}

func (b *interceptBuffer) Len() int {
	if b.compressWriter == nil {
		return b.Buffer.Len()
	}
	return b.len
}

func (b *interceptBuffer) Cap() int {
	if b.compressWriter == nil {
		return b.Buffer.Cap()
	}
	return b.cap
}

func (b *interceptBuffer) Reset() {
	b.len = 0
	b.Buffer.Reset()
}

func (b *interceptBuffer) Flush() error {
	if b.compressWriter == nil {
		return nil
	}
	return b.compressWriter.Flush()
}

func (b *interceptBuffer) Close() error {
	if b.compressWriter == nil {
		return nil
	}
	return b.compressWriter.Close()
}

func newInterceptBuffer(chunkSize int, compress bool) *interceptBuffer {
	b := &interceptBuffer{Buffer: bytes.NewBuffer(make([]byte, 0, chunkSize))}
	if compress {
		b.len = 0
		b.cap = chunkSize
		b.compressWriter = gzip.NewWriter(b.Buffer)
	}
	return b
}

type uploaderWriter struct {
	buf      *interceptBuffer
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
func NewUploaderWriter(uploader Uploader, chunkSize int, compress bool) Writer {
	return newUploaderWriter(uploader, chunkSize, compress)
}

// newUploaderWriter is used for testing only.
func newUploaderWriter(uploader Uploader, chunkSize int, compress bool) *uploaderWriter {
	return &uploaderWriter{
		uploader: uploader,
		buf:      newInterceptBuffer(chunkSize, compress)}
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
