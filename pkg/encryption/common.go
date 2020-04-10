// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"crypto/rand"

	"github.com/gogo/protobuf/proto"
	"github.com/spacemonkeygo/openssl"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

const (
	keyBackendTypeFile = "file"

	metaEncryptedKey = "encrypted_key"
	metaKeyIV        = "iv"
	metaKeyTag       = "tag"

	keySize = 32
	ivSize  = 12
)

type Config = encryptionpb.EncryptionConfig

// EncryptionOptions contain common encryption configurations.
type Options struct {
	Config
	fileBackendOptions

	keyBackendType string
}

type keyBackend interface {
	GetKey() (key []byte, encryptedKey []byte, err error)
	DecryptKey(encryptedKey []byte) (key []byte, err error)
}

func (options *Options) fillDataKey() error {
	dataKeySize := 0
	switch options.Config.Method {
	case encryptionpb.EncryptionMethod_PLAINTEXT:
		// No encryption key needed.
		return nil
	case encryptionpb.EncryptionMethod_AES128_CTR:
		dataKeySize = 16
	case encryptionpb.EncryptionMethod_AES192_CTR:
		dataKeySize = 24
	case encryptionpb.EncryptionMethod_AES256_CTR:
		dataKeySize = 32
	case encryptionpb.EncryptionMethod_UNKNOWN:
		return errors.New("encryption method unknown")
	default:
		return errors.New("unrecognized encryption method")
	}
	options.Config.Key = make([]byte, dataKeySize)
	_, err := rand.Read(options.Config.Key)
	if err != nil {
		return errors.Annotate(err, "failed to generate data encryption key")
	}
	return nil
}

func getKeyBackend(options *Options) (keyBackend, error) {
	switch options.keyBackendType {
	case "":
		// No encryption key provided.
		return nil, nil
	case keyBackendTypeFile:
		return newFileBackend(&options.fileBackendOptions)
	default:
		return nil, errors.Errorf("unrecognized encryption key type %s", options.keyBackendType)
	}
}

// MaybeEncrypt returns content as-is if encryption method is set to plaintext
// (i.e. when encryption is not enabled). Otherwise it encrypt the content with AES256-GCM,
// store the result in EncryptedContent struct, and marshal it.
func MaybeEncrypt(content []byte, options *Options) ([]byte, error) {
	backend, err := getKeyBackend(options)
	if err != nil {
		return nil, err
	}
	// If no key provided, the content is not encrypted.
	if backend == nil {
		return content, nil
	}
	key, encryptedKey, err := backend.GetKey()
	if err != nil {
		return nil, err
	}
	if len(key) != keySize {
		return nil, errors.Errorf("key size must be %d bytes, got %s bytes", keySize, len(key))
	}
	// Recommanded IV size for GCM is 12 bytes.
	// https://www.openssl.org/docs/man1.1.0/man3/EVP_rc2_40_cbc.html#GCM-and-OCB-Modes
	iv := make([]byte, ivSize)
	_, err = rand.Read(iv)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// NewGCMEncryptionCipherCtx() require to pass a block size, which is actually key size in bits,
	// since block size is fixed 16 bytes with AES.
	// https://github.com/spacemonkeygo/openssl/blob/37dddbfb29b47a9299ba85f27e3e35bcf6733440/ciphers_gcm.go#L58
	ctx, err := openssl.NewGCMEncryptionCipherCtx(keySize*8, nil, key, iv)
	if err != nil {
		return nil, errors.Trace(err)
	}
	output, err := ctx.EncryptUpdate(content)
	if err != nil {
		return nil, errors.Trace(err)
	}
	finalOutput, err := ctx.EncryptFinal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	tag, err := ctx.GetTag()
	if err != nil {
		return nil, errors.Trace(err)
	}
	encryptedContent := encryptionpb.EncryptedContent{
		Metadata: map[string][]byte{
			metaEncryptedKey: encryptedKey,
			metaKeyIV:        iv,
			metaKeyTag:       tag,
		},
		Content: append(output, finalOutput...),
	}
	return proto.Marshal(&encryptedContent)
}

// MaybeDecrypt returns content as-is if encryption method is set to plaintext
// (i.e. when encryption is not enabled). Otherwise it unmarshal the content into
// EncryptedContent struct, decrypt the content with AES256-GCM and return the result.
func MaybeDecrypt(content []byte, options *Options) ([]byte, error) {
	backend, err := getKeyBackend(options)
	if err != nil {
		return nil, err
	}
	// If no key provided, the content is not encrypted.
	if backend == nil {
		return content, nil
	}
	// Decode encrypted content.
	encryptedContent := &encryptionpb.EncryptedContent{}
	err = proto.Unmarshal(content, encryptedContent)
	if err != nil {
		return nil, errors.Trace(err)
	}
	encryptedKey, ok := encryptedContent.Metadata[metaEncryptedKey]
	if !ok {
		return nil, errors.New("missing encrypted key in encrypted content metadata")
	}
	key, err := backend.DecryptKey(encryptedKey)
	if err != nil {
		return nil, err
	}
	if len(key) != keySize {
		return nil, errors.Errorf("key size must be %d bytes, got %s bytes", keySize, len(key))
	}
	iv, ok := encryptedContent.Metadata[metaKeyIV]
	if !ok {
		return nil, errors.New("missing iv in encrypted content metadata")
	}
	tag, ok := encryptedContent.Metadata[metaKeyTag]
	if !ok {
		return nil, errors.New("missing tag in encrypted content metadata")
	}
	// See comment around use of NewGCMEncryptionCipherCtx for the keySize(blockSize) param.
	ctx, err := openssl.NewGCMDecryptionCipherCtx(keySize*8, nil, key, iv)
	if err != nil {
		return nil, errors.Trace(err)
	}
	output, err := ctx.DecryptUpdate(encryptedContent.Content)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = ctx.SetTag(tag); err != nil {
		return nil, errors.Trace(err)
	}
	finalOutput, err := ctx.DecryptFinal()
	if err != nil {
		return nil, errors.Annotate(err, "possibly wrong encryption key, or content being altered")
	}
	return append(output, finalOutput...), nil
}

// MayBeEncrypted checks if the content is encrypted, by trying to deserialize it
// as EncryptedContent.
func MayBeEncrypted(content []byte) bool {
	encryptedContent := &encryptionpb.EncryptedContent{}
	err := proto.Unmarshal(content, encryptedContent)
	return err == nil
}
