package storage

import (
	"bytes"
	"context"
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
