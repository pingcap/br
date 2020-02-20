package storage

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"

	"cloud.google.com/go/storage"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/pflag"
	"golang.org/x/oauth2/google"
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

	gcs.Endpoint = options.Endpoint
	gcs.StorageClass = options.StorageClass
	gcs.PredefinedAcl = options.PredefinedACL

	if options.CredentialsFile != "" {
		b, err := ioutil.ReadFile(options.CredentialsFile)
		if err != nil {
			return err
		}
		gcs.CredentialsBlob = string(b)
	}
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
	_ = flags.MarkHidden(gcsStorageClassOption)
	_ = flags.MarkHidden(gcsPredefinedACL)
	_ = flags.MarkHidden(gcsCredentialsFile)
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

func newGCSStorage(ctx context.Context, gcs *backup.GCS, sendCredential bool) (*gcsStorage, error) {
	return newGCSStorageWithHTTPClient(ctx, gcs, nil, sendCredential)
}

func newGCSStorageWithHTTPClient( // revive:disable-line:flag-parameter
	ctx context.Context,
	gcs *backup.GCS,
	hclient *http.Client,
	sendCredential bool,
) (*gcsStorage, error) {
	var clientOps []option.ClientOption
	if gcs.CredentialsBlob == "" {
		creds, err := google.FindDefaultCredentials(ctx, storage.ScopeReadWrite)
		if err != nil {
			return nil, errors.New(err.Error() + "Or you should provide '--gcs.credentials_file'.")
		}
		if sendCredential {
			if len(creds.JSON) > 0 {
				gcs.CredentialsBlob = string(creds.JSON)
			} else {
				return nil, errors.New(
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
	if hclient != nil {
		clientOps = append(clientOps, option.WithHTTPClient(hclient))
	}
	client, err := storage.NewClient(ctx, clientOps...)
	if err != nil {
		return nil, err
	}

	if !sendCredential {
		// Clear the credentials if exists so that they will not be sent to TiKV
		gcs.CredentialsBlob = ""
	}

	bucket := client.Bucket(gcs.Bucket)
	// check bucket exists
	_, err = bucket.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	return &gcsStorage{gcs: gcs, bucket: bucket}, nil
}
