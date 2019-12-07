package storage

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/pflag"
)

// DefineFlags adds flags to the flag set corresponding to all backend options.
func DefineFlags(flags *pflag.FlagSet) {
	defineS3Flags(flags)
}

// GetBackendOptionsFromFlags obtains the backend options from the flag set.
func GetBackendOptionsFromFlags(flags *pflag.FlagSet) (options []BackendOption, err error) {
	if options, err = appendBackendOptionsFromS3Flags(options, flags); err != nil {
		return nil, err
	}
	return
}

// ParseBackendFromFlags is a convenient function to consecutively call
// GetBackendOptionsFromFlags and ParseBackend.
func ParseBackendFromFlags(flags *pflag.FlagSet, storageFlag string) (*backup.StorageBackend, error) {
	u, err := flags.GetString(storageFlag)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opts, err := GetBackendOptionsFromFlags(flags)
	if err != nil {
		return nil, err
	}
	return ParseBackend(u, opts...)
}
