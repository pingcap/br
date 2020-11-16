package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"

	"github.com/pingcap/errors"

	berrors "github.com/pingcap/br/pkg/errors"
)

// CompressType represents the type of compression.
type CompressType uint8

const (
	// NoCompression won't compress given bytes.
	NoCompression CompressType = iota
	// Gzip will compress given bytes in gzip format.
	Gzip
)

// UploaderWriterOptions is the options used to configure an UploaderWriter.
type UploaderWriterOptions struct {
	// CompressType is the algorithm used to compress the input before uploading.
	// If the algorithm is not NoCompression, ensure the CompressLevel is also filled in.
	CompressType CompressType

	// CompressLevel is the compression level. Higher number means smaller size but slower speed.
	// Zero typically means "no compression", so if the CompressType is not NoCompression,
	// this field should be populated by some numbers (this field is ignored when NoCompression).
	CompressLevel int

	// FirstPartSize is the maximum output size (after compression) of the first uploaded part.
	FirstPartSize int

	// PartSizeInflationDivisor is a factor added to the input size for the second and subsequent
	// parts to be uploaded. Some cloud providers like AWS S3 restricts the number of parts (10,000),
	// so we design the part size to gradually increase to allow uploading extremely large files.
	//
	// 		PartSize[n] ≈ FirstPartSize * (1 + 1/PartSizeInflationDivisor)^n
	//
	// Setting PartSizeInflationDivisor to 0 is equivalent to infinity (no inflation).
	//
	// For AWS S3, we recommend setting the FirstPartSize to 5 MiB, and the PartSizeInflationDivisor
	// to 1530 (or less). This makes the last part (10,000) to have size 3.36 GiB and the total size
	// of all parts to be 5.01 TiB, which happens to just exceed the maximum allowed object size on
	// AWS S3.
	PartSizeInflationDivisor int
}

type simpleCompressWriter interface {
	io.WriteCloser
	Flush() error
}

// noopFlushCloser wraps an io.Writer with the Flush() and Close() methods doing nothing
// successfully.
type noopFlushCloser struct{ io.Writer }

func (n noopFlushCloser) Flush() error {
	return nil
}

func (n noopFlushCloser) Close() error {
	return nil
}

type simpleCompressBuffer struct {
	// buffer is the currently written data after compression.
	buffer *bytes.Buffer
	// compressWriter is an intermediate writer which compresses input into the buffer.
	compressWriter simpleCompressWriter
	// len is the estimated output size. This value is used to guide when to flush the
	// compressWriter into the buffer.
	//
	// The general principle is that:
	//  * after calling compressWriter.Flush(), len == buffer.Len() == actual output size.
	//    we call this state having an "anchored" buffer.
	//  * after calling compressWriter.Write(), len > buffer.Len().
	//    we call this state having a "floating" buffer.
	// So everytime you want to read the buffer, ensure it has been `flush()`ed first.
	len int
}

func newInterceptBuffer(options *UploaderWriterOptions) (*simpleCompressBuffer, error) {
	bf := bytes.NewBuffer(make([]byte, 0, options.FirstPartSize))
	var compressWriter simpleCompressWriter
	switch options.CompressType {
	case NoCompression:
		compressWriter = noopFlushCloser{Writer: bf}
	case Gzip:
		var err error
		compressWriter, err = gzip.NewWriterLevel(bf, options.CompressLevel)
		if err != nil {
			return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "%v", err)
		}
	default:
		return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "unsupported compression type %d for uploading data", options.CompressType)
	}
	return &simpleCompressBuffer{
		buffer:         bf,
		compressWriter: compressWriter,
		len:            0,
	}, nil
}

// write writes some bytes into the buffer.
func (b *simpleCompressBuffer) write(p []byte) (int, error) {
	written, err := b.compressWriter.Write(p)
	// the buffer is "floating", where b.len overestimates the actual size and
	// b.buffer is not yet up-to-date with the bytes written into b.compressWriter.
	b.len += written
	return written, err
}

// flush flushes any pending compressed data into the buffer. This also makes
// b.len accurately indicate the output size.
func (b *simpleCompressBuffer) flush() error {
	if err := b.compressWriter.Flush(); err != nil {
		return err
	}
	// the buffer is now "anchored", where b.buffer is now up-to-date with
	// the data in b.compressWriter, so we also set b.len to the actual buffer size.
	b.len = b.buffer.Len()
	return nil
}

// extract reads up to `cap` bytes from the buffer. Excess bytes remains
// in-place.
// This method must be never called after a `write()`. The returned bytes must
// be immediately consumed or copied before the next `write()`.
func (b *simpleCompressBuffer) extract(cap int) []byte {
	res := b.buffer.Next(cap)
	// we extracted data from the "anchored" buffer.
	// the buffer is still "anchored" as no Write() happened.
	// so b.len should still be accurate.
	b.len = b.buffer.Len()
	return res
}

// finish flushes any pending compressed data into the buffer, and writes the
// footer for compressed format. The buffer should not be used after calling
// finish. These remaining bytes are returned.
func (b *simpleCompressBuffer) finish() ([]byte, error) {
	if err := b.compressWriter.Close(); err != nil {
		return nil, err
	}
	// closing the compressWriter always "anchor" the buffer.
	return b.buffer.Bytes(), nil
}

type uploaderWriter struct {
	buf      *simpleCompressBuffer
	uploader Uploader
	cap      int
	inflator int
}

func (u *uploaderWriter) Write(ctx context.Context, p []byte) (int, error) {
	w, err := u.buf.write(p)
	if err != nil {
		return 0, err
	}

	// if we have written enough many data, flush the compressor to check if we are over capacity.
	if u.buf.len > u.cap {
		if err = u.buf.flush(); err != nil {
			return 0, err
		}

		// after flushing, the actual output size may be smaller due to compression,
		// so we must check again.
		for u.buf.len > u.cap {
			// the chunk is now confirmed full, upload it.
			if err = u.uploadChunk(ctx); err != nil {
				return 0, err
			}
			// one chunk may not be enough, repeat until not full.
		}
	}

	return w, nil
}

func (u *uploaderWriter) uploadChunk(ctx context.Context) error {
	b := u.buf.extract(u.cap)
	if len(b) == 0 {
		return nil
	}
	if u.inflator != 0 {
		u.cap += u.cap / u.inflator
	}
	return u.uploader.UploadPart(ctx, b)
}

func (u *uploaderWriter) Close(ctx context.Context) error {
	b, err := u.buf.finish()
	if err != nil {
		return err
	}
	if len(b) != 0 {
		err = u.uploader.UploadPart(ctx, b)
		if err != nil {
			return err
		}
	}
	return u.uploader.CompleteUpload(ctx)
}

// NewUploaderWriter wraps the Writer interface over an uploader.
func NewUploaderWriter(uploader Uploader, options *UploaderWriterOptions) (Writer, error) {
	if options.FirstPartSize <= 0 {
		return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "UploaderWriterOptions.FirstPartSize must be positive, not %d", options.FirstPartSize)
	}
	if options.PartSizeInflationDivisor < 0 {
		return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "UploaderWriterOptions.PartSizeInflatorDivisor must be non-negative, not %d", options.PartSizeInflationDivisor)
	}

	buf, err := newInterceptBuffer(options)
	if err != nil {
		return nil, err
	}
	return &uploaderWriter{
		uploader: uploader,
		buf:      buf,
		cap:      options.FirstPartSize,
		inflator: options.PartSizeInflationDivisor,
	}, nil
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
