package storage

import (
	"bytes"
	"context"
	"io"
)

type uploaderWriter struct {
	buf      *bytes.Buffer
	uploader Uploader
	ctx      context.Context
}

func (u *uploaderWriter) Write(p []byte) (int, error) {
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
		err := u.uploadChunk(u.ctx)
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

func (u *uploaderWriter) Close() error {
	err := u.uploadChunk(u.ctx)
	if err != nil {
		return err
	}
	return u.uploader.CompleteUpload(u.ctx)
}

// NewUploaderWriter wraps the Writer interface over an uploader.
func NewUploaderWriter(ctx context.Context, uploader Uploader, chunkSize int) io.WriteCloser {
	return newUploaderWriter(ctx, uploader, chunkSize)
}

// newUploaderWriter is used for testing only.
func newUploaderWriter(ctx context.Context, uploader Uploader, chunkSize int) *uploaderWriter {
	return &uploaderWriter{
		uploader: uploader,
		ctx:      ctx,
		buf:      bytes.NewBuffer(make([]byte, 0, chunkSize))}
}
