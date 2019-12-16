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
	if options.CredentialsFile == "" {
		return errors.New("must provide 'gcs.credentials_file'")
	}

	gcs.Endpoint = options.Endpoint
	gcs.StorageClass = options.StorageClass
	gcs.PredefinedAcl = options.PredefinedACL

	b, err := ioutil.ReadFile(options.CredentialsFile)
	if err != nil {
		return err
	}
	gcs.CredentialsBlob = string(b)
	return nil
}

func defineGCSFlags(flags *pflag.FlagSet) {
	flags.String(gcsEndpointOption, "", "Set the GCS endpoint URL")
	flags.String(gcsStorageClassOption, "",
		`Specify the GCS storage class for objects.
If it is not set, objects uploaded are
followed by the default storage class of the bucket.
See https://cloud.google.com/storage/docs/storage-classes
for valid values.`)
	flags.String(gcsPredefinedACL, "",
		`Specify the GCS predefined acl for objects.
If it is not set, objects uploaded are
followed by the acl of bucket scope.
See https://cloud.google.com/storage/docs/access-control/lists#predefined-acl
for valid values.`)
	flags.String(gcsCredentialsFile, "",
		`Set the GCS credentials file path.
You can get one from
https://console.cloud.google.com/apis/credentials.`)

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

	options.PredefinedACL, err = flags.GetString(gcsPredefinedACL)
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

func newGCSStorage(ctx context.Context, gcs *backup.GCS) (*gcsStorage, error) {
	var clientOps []option.ClientOption
	clientOps = append(clientOps, option.WithCredentialsJSON([]byte(gcs.GetCredentialsBlob())))
	if gcs.Endpoint != "" {
		clientOps = append(clientOps, option.WithEndpoint(gcs.Endpoint))
	}
	client, err := storage.NewClient(ctx, clientOps...)
	if err != nil {
		return nil, err
	}
	bucket := client.Bucket(gcs.Bucket)
	// check bucket exists
	_, err = bucket.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	return &gcsStorage{gcs: gcs, bucket: bucket}, nil
}
