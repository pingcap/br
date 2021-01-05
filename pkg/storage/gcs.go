// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io"
	"io/ioutil"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	berrors "github.com/pingcap/br/pkg/errors"
)

const (
	gcsEndpointOption     = "gcs.endpoint"
	gcsStorageClassOption = "gcs.storage-class"
	gcsPredefinedACL      = "gcs.predefined-acl"
	gcsCredentialsFile    = "gcs.credentials-file"
)

// GCSBackendOptions are options for configuration the GCS storage.
type GCSBackendOptions struct {
	Endpoint        string `json:"endpoint" toml:"endpoint"`
	StorageClass    string `json:"storage-class" toml:"storage-class"`
	PredefinedACL   string `json:"predefined-acl" toml:"predefined-acl"`
	CredentialsFile string `json:"credentials-file" toml:"credentials-file"`
}

func (options *GCSBackendOptions) apply(gcs *backup.GCS) error {
	gcs.Endpoint = options.Endpoint
	gcs.StorageClass = options.StorageClass
	gcs.PredefinedAcl = options.PredefinedACL

	if options.CredentialsFile != "" {
		b, err := ioutil.ReadFile(options.CredentialsFile)
		if err != nil {
			return errors.Trace(err)
		}
		gcs.CredentialsBlob = string(b)
	}
	return nil
}

func defineGCSFlags(flags *pflag.FlagSet) {
	// TODO: remove experimental tag if it's stable
	flags.String(gcsEndpointOption, "", "(experimental) Set the GCS endpoint URL")
	flags.String(gcsStorageClassOption, "", "(experimental) Specify the GCS storage class for objects")
	flags.String(gcsPredefinedACL, "", "(experimental) Specify the GCS predefined acl for objects")
	flags.String(gcsCredentialsFile, "", "(experimental) Set the GCS credentials file path")
}

func (options *GCSBackendOptions) parseFromFlags(flags *pflag.FlagSet) error {
	var err error
	options.Endpoint, err = flags.GetString(gcsEndpointOption)
	if err != nil {
		return errors.Trace(err)
	}

	options.StorageClass, err = flags.GetString(gcsStorageClassOption)
	if err != nil {
		return errors.Trace(err)
	}

	options.PredefinedACL, err = flags.GetString(gcsPredefinedACL)
	if err != nil {
		return errors.Trace(err)
	}

	options.CredentialsFile, err = flags.GetString(gcsCredentialsFile)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

type gcsStorage struct {
	gcs    *backup.GCS
	bucket *storage.BucketHandle
}

func (s *gcsStorage) objectName(name string) string {
	// to make it compatible with old version case 1
	// see details https://github.com/pingcap/br/issues/675#issuecomment-753780742
	ctx, cancel := context.WithTimeout(context.TODO(), 5 * time.Second)
	defer cancel()
	if _, err := s.bucket.Object(s.gcs.Prefix+name).Attrs(ctx); err != storage.ErrObjectNotExist {
		return s.gcs.Prefix+name
	}
	return path.Join(s.gcs.Prefix, name)
}

// Write file to storage.
func (s *gcsStorage) Write(ctx context.Context, name string, data []byte) error {
	object := s.objectName(name)
	wc := s.bucket.Object(object).NewWriter(ctx)
	wc.StorageClass = s.gcs.StorageClass
	wc.PredefinedACL = s.gcs.PredefinedAcl
	_, err := wc.Write(data)
	if err != nil {
		return errors.Trace(err)
	}
	return wc.Close()
}

// Read storage file.
func (s *gcsStorage) Read(ctx context.Context, name string) ([]byte, error) {
	object := s.objectName(name)
	rc, err := s.bucket.Object(object).NewReader(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rc.Close()

	b := make([]byte, rc.Attrs.Size)
	_, err = io.ReadFull(rc, b)
	return b, errors.Trace(err)
}

// FileExists return true if file exists.
func (s *gcsStorage) FileExists(ctx context.Context, name string) (bool, error) {
	object := s.objectName(name)
	_, err := s.bucket.Object(object).Attrs(ctx)
	if err != nil {
		if errors.Cause(err) == storage.ErrObjectNotExist { // nolint:errorlint
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// Open a Reader by file path.
func (s *gcsStorage) Open(ctx context.Context, path string) (ReadSeekCloser, error) {
	// TODO, implement this if needed
	panic("Unsupported Operation")
}

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The first argument is the file path that can be used in `Open`
// function; the second argument is the size in byte of the file determined
// by path.
func (s *gcsStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
	// TODO, implement this if needed
	panic("Unsupported Operation")
}

func (s *gcsStorage) URI() string {
	return "gcs://" + s.gcs.Bucket + "/" + s.gcs.Prefix
}

// CreateUploader implenments ExternalStorage interface.
func (s *gcsStorage) CreateUploader(ctx context.Context, name string) (Uploader, error) {
	// TODO, implement this if needed
	panic("gcs storage not support multi-upload")
}

func newGCSStorage(ctx context.Context, gcs *backup.GCS, opts *ExternalStorageOptions) (*gcsStorage, error) {
	var clientOps []option.ClientOption
	if gcs.CredentialsBlob == "" {
		creds, err := google.FindDefaultCredentials(ctx, storage.ScopeReadWrite)
		if err != nil {
			return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "%v Or you should provide '--gcs.credentials_file'", err)
		}
		if opts.SendCredentials {
			if len(creds.JSON) > 0 {
				gcs.CredentialsBlob = string(creds.JSON)
			} else {
				return nil, errors.Annotate(berrors.ErrStorageInvalidConfig,
					"You should provide '--gcs.credentials_file' when '--send-credentials-to-tikv' is true")
			}
		}
		clientOps = append(clientOps, option.WithCredentials(creds))
	} else {
		clientOps = append(clientOps, option.WithCredentialsJSON([]byte(gcs.GetCredentialsBlob())))
	}

	if gcs.Endpoint != "" {
		clientOps = append(clientOps, option.WithEndpoint(gcs.Endpoint))
	}
	if opts.HTTPClient != nil {
		clientOps = append(clientOps, option.WithHTTPClient(opts.HTTPClient))
	}
	client, err := storage.NewClient(ctx, clientOps...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if !opts.SendCredentials {
		// Clear the credentials if exists so that they will not be sent to TiKV
		gcs.CredentialsBlob = ""
	}

	bucket := client.Bucket(gcs.Bucket)
	// we need adjust prefix for gcs storage to make restore compatible with old version backup files.
	// see details about case 2 at https://github.com/pingcap/br/issues/675#issuecomment-753780742
	sstInPrefix := false
	sstInPrefixSlash := false
	it := bucket.Objects(ctx, &storage.Query{Prefix:gcs.Prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.Annotatef(berrors.ErrStorageUnknown, "Bucket(%q).Objects: %v", bucket, err)
		}
		if strings.HasPrefix(attrs.Name, "backup") {
			log.Info("meta file found in prefix", zap.String("file", attrs.Name))
			continue
		}
		if strings.HasSuffix(attrs.Name, ".sst") {
			log.Info("sst file found in prefix", zap.String("file", attrs.Name))
			sstInPrefix = true
			break
		}
	}
	it2 := bucket.Objects(ctx, &storage.Query{Prefix:gcs.Prefix + "//"})
	for {
		attrs, err := it2.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.Annotatef(berrors.ErrStorageUnknown, "Bucket(%q).Objects: %v", bucket, err)
		}
		if strings.HasSuffix(attrs.Name, ".sst") {
			log.Info("sst file found in prefix slash", zap.String("file", attrs.Name))
			sstInPrefixSlash = true
			break
		}
	}
	if sstInPrefixSlash && !sstInPrefix {
		// This is a old bug, but we must make it compatible.
		// so we need find sst in slash directory
		gcs.Prefix += "/"
	}
	if !opts.SkipCheckPath {
		// check bucket exists
		_, err = bucket.Attrs(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return &gcsStorage{gcs: gcs, bucket: bucket}, nil
}
