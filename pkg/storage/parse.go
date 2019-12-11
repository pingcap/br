package storage

import (
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
)

// BackendOptions further configures the storage backend not expressed by the
// storage URL.
type BackendOptions struct {
	S3  S3BackendOptions  `json:"s3" toml:"s3"`
	GCS GCSBackendOptions `json:"gcs", toml:"gcs"`
}

// ParseBackend constructs a structured backend description from the
// storage URL.
func ParseBackend(rawURL string, options *BackendOptions) (*backup.StorageBackend, error) {
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
		return &backup.StorageBackend{Backend: &backup.StorageBackend_Local{Local: local}}, nil

	case "noop":
		noop := &backup.Noop{}
		return &backup.StorageBackend{Backend: &backup.StorageBackend_Noop{Noop: noop}}, nil

	case "s3":
		s3 := &backup.S3{Bucket: u.Host, Prefix: u.Path}
		if options != nil {
			if err := options.S3.apply(s3); err != nil {
				return nil, err
			}
		}
		return &backup.StorageBackend{Backend: &backup.StorageBackend_S3{S3: s3}}, nil

	case "gcs":
		gcs := &backup.GCS{Bucket: u.Host, Prefix: u.Path}
		if options != nil {
			if err := options.GCS.apply(gcs); err != nil {
				return nil, err
			}
		}
		return &backup.StorageBackend{Backend: &backup.StorageBackend_Gcs{Gcs: gcs}}, nil

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
		u.Path = "/"
	case *backup.StorageBackend_S3:
		u.Scheme = "s3"
		u.Host = b.S3.Bucket
		u.Path = b.S3.Prefix
	}
	return
}
