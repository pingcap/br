// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"strings"

	"github.com/spf13/pflag"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

const (
	flagEncryption        = "encryption"
	flagEncryptionKeyType = "encryption-key-type"
	flagEncryptionKeyPath = "encryption-key-path"
)

// DefineFlags adds encryption related flags.
func DefineFlags(flags *pflag.FlagSet) {
	flags.String(flagEncryption, "", "Encrypt backup with specific encryption"+
		" method if non-empty. Can be one of aes128-ctr, aes192-ctr oraes256-ctr."+
		" The flag is only used for backup and ignored for restore.")
	// The prefix of the key is to support key management service later.
	// For example to specify a KMS CMK, we can allow 'kms:<cmk-key-id>' in the future.
	flags.String(flagEncryptionKeyType, "", "Type of encryption key to be used."+
		" Can only be \"file\" currently. On backup, if encryption is specified,"+
		" encryption key must be specified. On restore, the same key used for"+
		" backup must be passed back.")
	flags.String(flagEncryptionKeyPath, "", "Path to encryption key file."+
		" The file must contain a 32 bytes key encoded in readable hex form,"+
		" and end with newline.")
}

// ParseFromFlags obtains the encryption options from the flag set.
func (options *Options) ParseFromFlags(flags *pflag.FlagSet) error {
	// Parse encryption method.
	methodName, err := flags.GetString(flagEncryption)
	if err != nil {
		return errors.Trace(err)
	}
	if methodName == "" {
		options.Config.Method = encryptionpb.EncryptionMethod_PLAINTEXT
	} else {
		methodName = strings.ToUpper(strings.ReplaceAll(methodName, "-", "_"))
		method, ok := encryptionpb.EncryptionMethod_value[methodName]
		options.Config.Method = encryptionpb.EncryptionMethod(method)
		if !ok || options.Config.Method == encryptionpb.EncryptionMethod_UNKNOWN {
			return errors.Errorf("unrecognized encryption %s", methodName)
		}
	}

	// Parse encryption key.
	options.keyBackendType, err = flags.GetString(flagEncryptionKeyType)
	if err != nil {
		return errors.Trace(err)
	}
	switch options.keyBackendType {
	case keyBackendTypeFile:
		options.path, err = flags.GetString(flagEncryptionKeyPath)
		if err != nil {
			return errors.Trace(err)
		}
	case "":
		// no encryption key provided, do nothing
	default:
		return errors.Errorf("unrecognized encryption key type %s", options.keyBackendType)
	}

	return options.fillDataKey()
}
