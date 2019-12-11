package storage

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/pflag"
)

const (
	s3EndpointOption = "s3.endpoint"
	s3RegionOption   = "s3.region"
)

// S3BackendOptions are options for configurating the S3 storage.
type S3BackendOptions struct {
	Endpoint string `json:"endpoint" toml:"endpoint"`
	Region   string `json:"region" toml:"region"`
}

func (options *S3BackendOptions) apply(s3 *backup.S3) error {
	if options.Endpoint == "" && options.Region == "" {
		return errors.New("must provide either 's3.region' or 's3.endpoint'")
	}
	// TODO: Verify Region.

	s3.Endpoint = options.Endpoint
	s3.Region = options.Region
	return nil
}

func defineS3Flags(flags *pflag.FlagSet) {
	flags.String(s3EndpointOption, "", "Set the AWS S3 endpoint URL")
	flags.String(s3RegionOption, "", "Set the AWS region")
	// TODO: Finalize the list of options.
	_ = flags.MarkHidden(s3EndpointOption)
}

func getBackendOptionsFromS3Flags(flags *pflag.FlagSet) (options S3BackendOptions, err error) {
	options.Endpoint, err = flags.GetString(s3EndpointOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	options.Region, err = flags.GetString(s3RegionOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	// TODO: Add more options here.

	return
}

// TODO: Define S3 storage.
