// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"encoding/hex"
	"strings"

	"github.com/spf13/pflag"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

const (
	flagEncryption    = "encryption"
	flagEncryptionKey = "encryption-key"
)

func DefineFlags(flags *pflag.FlagSet) {
	flags.String(flagEncryption, "", "Enable encryption and use the specific"+
		" method if non-empty. Can be one of aes128-ctr, aes192-ctr oraes256-ctr.")
	// The prefix of the key is to support key management service later.
	// For example to specify a KMS CMK, we can allow 'kms:<cmk-key-id>' in the future.
	flags.String(flagEncryptionKey, "", "Encryption key to be use, if encryption"+
		" is enabled. Must be encoded in hex and starts with 'hex:' prefix,"+
		" e.g. 'hex:107fad4360d527e818856f0281219c5d'")
}

func (options *EncryptionOptions) ParseFromFlags(flags *pflag.FlagSet) error {
	// Parse encryption method.
	methodName, err := flags.GetString(flagEncryption)
	if err != nil {
		return errors.Trace(err)
	}
	if methodName == "" {
		options.Method = encryptionpb.EncryptionMethod_PLAINTEXT
	} else {
		methodName = strings.ToUpper(strings.ReplaceAll(methodName, "-", "_"))
		method, ok := encryptionpb.EncryptionMethod_value[methodName]
		options.Method = EncryptionMethod(method)
		if !ok || options.Method == encryptionpb.EncryptionMethod_UNKNOWN {
			return errors.Errorf("unrecognized encryption %s", methodName)
		}
	}

	// Parse encryption key.
	keyString, err := flags.GetString(flagEncryptionKey)
	if err != nil {
		return errors.Trace(err)
	}
	if options.Method == encryptionpb.EncryptionMethod_PLAINTEXT {
		if keyString != "" {
			return errors.New("specified encryption key but encryption is not enabled")
		}
	} else {
		keyTypeValue := strings.SplitN(keyString, ":", 2)
		if len(keyTypeValue) != 2 || keyTypeValue[0] != "hex" {
			return errors.New("malformed encryption key")
		}
		key, err := hex.DecodeString(keyTypeValue[1])
		if err != nil {
			return errors.Annotate(err, "failed to decode encryption key")
		}
		keySize, err := KeySize(options.Method)
		if err != nil {
			return err
		}
		if len(key) != keySize {
			return errors.Errorf("mismatch encryption key size, expected %d vs actual %d",
				keySize, len(options.Key))
		}
		options.Key = key
	}
	return nil
}
