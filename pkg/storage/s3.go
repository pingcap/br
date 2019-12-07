package storage

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/pflag"
)

const (
	s3EndpointOption = "s3.endpoint"
)

// S3EndpointOption is a BackendOption for changing the endpoint of the storage.
type S3Endpoint struct {
	backendOption
	// Endpoint is the host name of the S3 endpoint.
	Endpoint string
}

func (o *S3Endpoint) applyOnS3(s3 *backup.S3, name string) error {
	s3.Endpoint = o.Endpoint
	return nil
}

// OptionName returns the name of this option.
func (o *S3Endpoint) OptionName() string {
	return s3EndpointOption
}

func defineS3Flags(flags *pflag.FlagSet) {
	flags.String(s3EndpointOption, "", "Set the AWS S3 endpoint URL")
	// TODO: Finalize the list of options.
	flags.MarkHidden(s3EndpointOption)
}

func appendBackendOptionsFromS3Flags(options []BackendOption, flags *pflag.FlagSet) ([]BackendOption, error) {
	endpoint, err := flags.GetString(s3EndpointOption)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if endpoint != "" {
		options = append(options, &S3Endpoint{Endpoint: endpoint})
	}
	// TODO: Finalize the list of options.
	return options, nil
}

// TODO: Define S3 storage.
