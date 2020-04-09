// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package encryption

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/gogo/protobuf/proto"
	"github.com/spacemonkeygo/openssl"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

const (
	metaKeyIV               = "IV"
	metaKeyEncryptionMethod = "encryption_method"
	metaKeyTag              = "tag"
)

type EncryptionMethod = encryptionpb.EncryptionMethod
type EncryptionConfig = encryptionpb.EncryptionConfig

// EncryptionOptions contain common encryption configurations.
type EncryptionOptions struct {
	EncryptionConfig
}

// GetCipher gets cipher with respect to the encryption method.
func GetCipher(method EncryptionMethod) (*openssl.Cipher, error) {
	var cipherName string
	switch method {
	case encryptionpb.EncryptionMethod_UNKNOWN:
		return nil, errors.New("encryption method unknown")
	case encryptionpb.EncryptionMethod_PLAINTEXT:
		return nil, errors.New("encryption disabled")
	case encryptionpb.EncryptionMethod_AES128_CTR:
		cipherName = "aes-128-ctr"
	case encryptionpb.EncryptionMethod_AES192_CTR:
		cipherName = "aes-192-ctr"
	case encryptionpb.EncryptionMethod_AES256_CTR:
		cipherName = "aes-256-ctr"
	default:
		return nil, errors.New("unrecognized encryption method")
	}
	cipher, err := openssl.GetCipherByName(cipherName)
	if err != nil {
		err = errors.Annotate(err, "failed to get cipher")
	}
	return cipher, err
}

// KeySize gets key size in bytes for an encryption method.
func KeySize(method EncryptionMethod) (size int, err error) {
	cipher, err := GetCipher(method)
	if err == nil {
		size = cipher.KeySize()
	}
	return
}

// MaybeEncrypt returns content as-is if encryption method is set to plaintext
// (i.e. when encryption is not enabled). Otherwise it encrypt the content, store the
// result in EncryptedContent struct, and marshal it.
//
// Despite config.method specifies CTR mode, MaybeDecrypt use GCM mode with the same key size
// to authenticate the key and content.
func MaybeEncrypt(content []byte, config *EncryptionConfig) ([]byte, error) {
	if config.Method == encryptionpb.EncryptionMethod_PLAINTEXT {
		return content, nil
	}
	keySize, err := KeySize(config.Method)
	if err != nil {
		return nil, err
	}
	// Recommanded IV size for GCM is 12 bytes.
	// https://www.openssl.org/docs/man1.1.0/man3/EVP_rc2_40_cbc.html#GCM-and-OCB-Modes
	iv := make([]byte, 12)
	_, err = rand.Read(iv)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// NewGCMEncryptionCipherCtx() require to pass a block size, which is actually key size in bits,
	// since block size is fixed 16 bytes with AES.
	// https://github.com/spacemonkeygo/openssl/blob/37dddbfb29b47a9299ba85f27e3e35bcf6733440/ciphers_gcm.go#L58
	ctx, err := openssl.NewGCMEncryptionCipherCtx(keySize*8, nil, config.Key, iv)
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
	methodBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(methodBytes, uint32(config.Method))
	encryptedContent := encryptionpb.EncryptedContent{
		Metadata: map[string][]byte{
			metaKeyEncryptionMethod: methodBytes,
			metaKeyIV:               iv,
			metaKeyTag:              tag,
		},
		Content: append(output, finalOutput...),
	}
	return proto.Marshal(&encryptedContent)
}

// MaybeDecrypt returns content as-is if encryption method is set to plaintext
// (i.e. when encryption is not enabled). Otherwise it unmarshal the content into
// EncryptedContent struct, decrypt the content and return the result.
//
// Despite config.method specifies CTR mode, MaybeDecrypt use GCM mode with the same key size
// to authenticate the key and content.
func MaybeDecrypt(content []byte, config *EncryptionConfig) ([]byte, error) {
	if config.Method == encryptionpb.EncryptionMethod_PLAINTEXT {
		return content, nil
	}
	encryptedContent := &encryptionpb.EncryptedContent{}
	err := proto.Unmarshal(content, encryptedContent)
	if err != nil {
		return nil, errors.Trace(err)
	}
	methodBytes, ok := encryptedContent.Metadata[metaKeyEncryptionMethod]
	if !ok {
		return nil, errors.New("missing encryption method in encrypted content metadata")
	}
	if len(methodBytes) != 4 {
		return nil, errors.New("malformed encryption method in encrypted content metadata")
	}
	method := EncryptionMethod(binary.BigEndian.Uint32(methodBytes))
	if method != config.Method {
		return nil, errors.Errorf("mismatched encryption method, given %d vs actual %d",
			int32(config.Method), int32(method))
	}
	keySize, err := KeySize(config.Method)
	if err != nil {
		return nil, err
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
	ctx, err := openssl.NewGCMDecryptionCipherCtx(keySize*8, nil, config.Key, iv)
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
