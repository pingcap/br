package storage

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/pflag"
)

const (
	// flagSendCredentialOption specify whether to send credentials to tikv
	flagSendCredentialOption = "send-credentials-to-tikv"
)

var (
	sendCredential bool
)

// DefineFlags adds flags to the flag set corresponding to all backend options.
func DefineFlags(flags *pflag.FlagSet) {
	flags.BoolP(flagSendCredentialOption, "c", true,
		"Whether send credentials to tikv")
	defineS3Flags(flags)
	defineGCSFlags(flags)
}

// GetBackendOptionsFromFlags obtains the backend options from the flag set.
func GetBackendOptionsFromFlags(flags *pflag.FlagSet) (options BackendOptions, err error) {
	sendCredential, err = flags.GetBool(flagSendCredentialOption)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if options.S3, err = getBackendOptionsFromS3Flags(flags); err != nil {
		return
	}
	if options.GCS, err = getBackendOptionsFromGCSFlags(flags); err != nil {
		return
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
	return ParseBackend(u, &opts)
}
