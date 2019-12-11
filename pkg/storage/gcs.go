package storage

import (
	"context"
	"io"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/pflag"
	"google.golang.org/api/option"
)

const (
	gcsEndpointOption     = "gcs.endpoint"
	gcsStorageClassOption = "gcs.storage_class"
	gcsPredefinedAcl      = "gcs.predefined_acl"
	gcsCredentialsFile    = "gcs.credentials_file"
)

// GCSBackendOptions are options for configuration the GCS storage.
type GCSBackendOptions struct {
	Endpoint        string `json:"endpoint" toml:"endpoint"`
	StorageClass    string `json:"storage_class" toml:"storage_class"`
	PredefinedAcl   string `json:"predefined_acl" toml:"predefined_acl"`
	CredentialsFile string `json:"credentials_file" toml:"credentials_file"`
}

func (options *GCSBackendOptions) apply(gcs *backup.GCS) error {
	if options.CredentialsFile == "" {
		return errors.New("must provide 'gcs.credentials_file'")
	}

	gcs.Endpoint = options.Endpoint
	gcs.StorageClass = options.StorageClass
	gcs.PredefinedAcl = options.PredefinedAcl

	b, err := ioutil.ReadFile(options.CredentialsFile)
	if err != nil {
		return err
	}
	gcs.CredentialsBlob = string(b)
	return nil
}

func defineGCSFlags(flags *pflag.FlagSet) {
	flags.String(gcsEndpointOption, "", "Set the GCS endpoint URL")
	flags.String(gcsStorageClassOption, "", "Set the GCS storage class")
	flags.String(gcsPredefinedAcl, "", "Set the GCS predefined acl")
	flags.String(gcsCredentialsFile, "", "Set the GCS credentials file path")

	_ = flags.MarkHidden(gcsEndpointOption)
}

func getBackendOptionsFromGCSFlags(flags *pflag.FlagSet) (options GCSBackendOptions, err error) {
	options.Endpoint, err = flags.GetString(s3EndpointOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	options.StorageClass, err = flags.GetString(gcsStorageClassOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	options.PredefinedAcl, err = flags.GetString(gcsPredefinedAcl)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	options.CredentialsFile, err = flags.GetString(gcsCredentialsFile)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	return
}

type gcsStorage struct {
	gcs    *backup.GCS
	bucket *storage.BucketHandle
}

// Write file to storage
func (s *gcsStorage) Write(ctx context.Context, name string, data []byte) error {
	object := s.gcs.Prefix + name
	wc := s.bucket.Object(object).NewWriter(ctx)
	wc.StorageClass = s.gcs.StorageClass
	wc.PredefinedACL = s.gcs.PredefinedAcl
	_, err := wc.Write(data)
	if err != nil {
		return err
	}
	return wc.Close()
}

// Read storage file
func (s *gcsStorage) Read(ctx context.Context, name string) ([]byte, error) {
	object := s.gcs.Prefix + name
	rc, err := s.bucket.Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	b := make([]byte, rc.Attrs.Size)
	_, err = io.ReadFull(rc, b)
	return b, err
}

// FileExists return true if file exists
func (s *gcsStorage) FileExists(ctx context.Context, name string) (bool, error) {
	object := s.gcs.Prefix + name
	_, err := s.bucket.Object(object).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func newGCSStorage(gcs *backup.GCS) (*gcsStorage, error) {
	var clientOps []option.ClientOption
	clientOps = append(clientOps, option.WithCredentialsJSON([]byte(gcs.GetCredentialsBlob())))
	if gcs.Endpoint != "" {
		clientOps = append(clientOps, option.WithEndpoint(gcs.Endpoint))
	}
	client, err := storage.NewClient(context.Background(), clientOps...)
	if err != nil {
		return nil, err
	}
	bucket := client.Bucket(gcs.Bucket)
	return &gcsStorage{gcs: gcs, bucket: bucket}, nil
}
