package storage

import (
	"bytes"
	"compress/gzip"
	"context"
)

type uploaderWriter struct {
	buf      *bytes.Buffer
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
	err := u.uploadChunk(ctx)
	if err != nil {
		return err
	}
	return u.uploader.CompleteUpload(ctx)
}

type uploaderCompressWriter struct {
	*uploaderWriter
	bytesWritten int
	cap          int
	gzipWriter   *gzip.Writer
}

func (u *uploaderCompressWriter) Write(ctx context.Context, p []byte) (int, error) {
	bytesWritten := 0
	for u.bytesWritten+len(p) > u.cap {
		// We won't fit p in this chunk

		// Is this chunk full?
		chunkToFill := u.cap - u.bytesWritten
		if chunkToFill > 0 {
			// It's not full so we write enough of p to fill it
			prewrite := p[0:chunkToFill]
			w, err := u.gzipWriter.Write(prewrite)
			bytesWritten += w
			u.bytesWritten += w
			if err != nil {
				return bytesWritten, err
			}
			p = p[w:]
		}
		u.gzipWriter.Flush()
		err := u.uploadChunk(ctx)
		u.bytesWritten = 0
		if err != nil {
			return 0, err
		}
	}
	w, err := u.gzipWriter.Write(p)
	bytesWritten += w
	u.bytesWritten += w
	return bytesWritten, err
}

func (u *uploaderCompressWriter) Close(ctx context.Context) error {
	u.gzipWriter.Close()
	u.bytesWritten = 0
	return u.uploaderWriter.Close(ctx)
}

// NewUploaderCompressWriter wraps the Writer interface over an uploader.
func NewUploaderCompressWriter(uploader Uploader, chunkSize int) Writer {
	return newUploaderCompressWriter(uploader, chunkSize)
}

func newUploaderCompressWriter(uploader Uploader, chunkSize int) *uploaderCompressWriter {
	uploaderWriterImpl := newUploaderWriter(uploader, chunkSize)
	return &uploaderCompressWriter{
		uploaderWriter: uploaderWriterImpl,
		bytesWritten:   0,
		cap:            chunkSize,
		gzipWriter:     gzip.NewWriter(uploaderWriterImpl.buf),
	}
}

// NewUploaderWriter wraps the Writer interface over an uploader.
func NewUploaderWriter(uploader Uploader, chunkSize int) Writer {
	return newUploaderWriter(uploader, chunkSize)
}

// newUploaderWriter is used for testing only.
func newUploaderWriter(uploader Uploader, chunkSize int) *uploaderWriter {
	return &uploaderWriter{
		uploader: uploader,
		buf:      bytes.NewBuffer(make([]byte, 0, chunkSize))}
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
