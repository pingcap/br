package storage

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/pflag"
)

const (
	s3EndpointOption = "s3.endpoint"
)

type S3BackendOptions struct {
	Endpoint string `json:"endpoint" toml:"endpoint"`
}

func (options *S3BackendOptions) apply(s3 *backup.S3) error {
	// TODO: verify at least one of 'region' or 'endpoint' must be provided.
	s3.Endpoint = options.Endpoint
	return nil
}

// S3EndpointOption is a BackendOption for changing the endpoint of the storage.
func defineS3Flags(flags *pflag.FlagSet) {
	flags.String(s3EndpointOption, "", "Set the AWS S3 endpoint URL")
	// TODO: Finalize the list of options.
	flags.MarkHidden(s3EndpointOption)
}

func getBackendOptionsFromS3Flags(flags *pflag.FlagSet) (options S3BackendOptions, err error) {
	options.Endpoint, err = flags.GetString(s3EndpointOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	// TODO: Add more options here.

	return
}

// TODO: Define S3 storage.
