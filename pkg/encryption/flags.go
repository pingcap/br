// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"strings"

	"github.com/spf13/pflag"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

const (
	flagEncryption    = "encryption"
	flagEncryptionKey = "encryption-key"
)

// DefineFlags adds encryption related flags.
func DefineFlags(flags *pflag.FlagSet) {
	flags.StringP(flagEncryption, "e", "", "Encrypt backup with specific"+
		" encryption method if non-empty. Can be one of aes128-ctr, aes192-ctr or"+
		" aes256-ctr. The flag is only used for backup and ignored for restore.")
	flags.StringP(flagEncryptionKey, "k", "", "Encryption key to be used,"+
		" in the form of file:<path-to-file>, where the file contain a 32 bytes"+
		" key encoded in readable hex form.")
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
	keyString, err := flags.GetString(flagEncryptionKey)
	if err != nil {
		return errors.Trace(err)
	}
	if keyString == "" {
		options.keyBackendType = ""
		return nil
	}
	keyTypeValue := strings.SplitN(keyString, ":", 2)
	if len(keyTypeValue) != 2 {
		return errors.Errorf("malformed encryption key %s", keyString)
	}
	if keyTypeValue[0] != keyBackendTypeFile {
		return errors.Errorf("unrecognized encryption key type %s", keyTypeValue[0])
	}
	options.keyBackendType = keyTypeValue[0]
	options.fileBackendOptions.path = keyTypeValue[1]
	return nil
}

// PrepareForBackup validates options and generates data encryption key.
func (options *Options) PrepareForBackup() error {
	if options.Config.Method == encryptionpb.EncryptionMethod_PLAINTEXT {
		if options.keyBackendType != "" {
			return errors.New("encryption not enabled but encryption key provided")
		}
		return nil
	} else {
		var err error
		switch options.keyBackendType {
		case "":
			return errors.New("missing encryption key")
		case keyBackendTypeFile:
			err = options.fileBackendOptions.validate()
		}
		if err != nil {
			return err
		}
		return options.fillDataKey()
	}
}

func (options *Options) EncryptionEnabled() bool {
	return options.keyBackendType != ""
}
