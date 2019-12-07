package storage

import (
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
)

// BackendOption further configures the storage backend not expressed by the
// storage URL.
type BackendOption interface {
	OptionName() string
	applyOnNoop(noop *backup.Noop, name string) error
	applyOnLocal(local *backup.Local, name string) error
	applyOnS3(s3 *backup.S3, name string) error
}

type backendOption struct{}

func (backendOption) applyOnNoop(_ *backup.Noop, name string) error {
	return errors.Errorf("option '%s' is not applicable to noop storage", name)
}

func (backendOption) applyOnLocal(_ *backup.Local, name string) error {
	return errors.Errorf("option '%s' is not applicable to local storage", name)
}

func (backendOption) applyOnS3(_ *backup.Noop, name string) error {
	return errors.Errorf("option '%s' is not applicable to s3 storage", name)
}

// ParseBackend constructs a structured backend description from the
// storage URL.
func ParseBackend(rawURL string, options ...BackendOption) (*backup.StorageBackend, error) {
	if len(rawURL) == 0 {
		return nil, errors.New("empty store is not allowed")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch u.Scheme {
	case "":
		return nil, errors.Errorf("please specify the storage type (e.g. --storage 'local://%s')", u.Path)

	case "local", "file":
		local := &backup.Local{Path: u.Path}
		for _, option := range options {
			if err := option.applyOnLocal(local, option.OptionName()); err != nil {
				return nil, err
			}
		}
		return &backup.StorageBackend{Backend: &backup.StorageBackend_Local{Local: local}}, nil

	case "noop":
		noop := &backup.Noop{}
		for _, option := range options {
			if err := option.applyOnNoop(noop, option.OptionName()); err != nil {
				return nil, err
			}
		}
		return &backup.StorageBackend{Backend: &backup.StorageBackend_Noop{Noop: noop}}, nil

	case "s3":
		s3 := &backup.S3{Bucket: u.Host, Prefix: u.Path}
		for _, option := range options {
			if err := option.applyOnS3(s3, option.OptionName()); err != nil {
				return nil, err
			}
		}
		return &backup.StorageBackend{Backend: &backup.StorageBackend_S3{S3: s3}}, nil

	default:
		return nil, errors.Errorf("storage %s not support yet", u.Scheme)
	}
}

// FormatBackendURL obtains the raw URL which can be used the reconstruct the
// backend. The returned URL does not contain options for further configurating
// the backend. This is to avoid exposing secret tokens.
func FormatBackendURL(backend *backup.StorageBackend) (u url.URL) {
	switch b := backend.Backend.(type) {
	case *backup.StorageBackend_Local:
		u.Scheme = "local"
		u.Path = b.Local.Path
	case *backup.StorageBackend_Noop:
		u.Scheme = "noop"
	case *backup.StorageBackend_S3:
		u.Scheme = "s3"
		u.Host = b.S3.Bucket
		u.Path = b.S3.Prefix
	}
	return
}
