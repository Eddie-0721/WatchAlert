// Package secretbox encrypts small configuration secrets before persistence.
// The key is supplied by deployment configuration and is never stored in DB.
package secretbox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
)

func Encrypt(plaintext, key string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	if key == "" {
		return "", fmt.Errorf("Agent credentialKey 未配置，无法保存模型 API Key")
	}
	block, err := aes.NewCipher(keyBytes(key))
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(append(nonce, gcm.Seal(nil, nonce, []byte(plaintext), nil)...)), nil
}

func Decrypt(ciphertext, key string) (string, error) {
	if ciphertext == "" {
		return "", nil
	}
	if key == "" {
		return "", fmt.Errorf("Agent credentialKey 未配置，无法读取模型 API Key")
	}
	value, err := base64.RawURLEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("模型 API Key 密文格式无效")
	}
	block, err := aes.NewCipher(keyBytes(key))
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(value) < gcm.NonceSize() {
		return "", fmt.Errorf("模型 API Key 密文无效")
	}
	plaintext, err := gcm.Open(nil, value[:gcm.NonceSize()], value[gcm.NonceSize():], nil)
	if err != nil {
		return "", fmt.Errorf("模型 API Key 无法解密，请检查 Agent credentialKey")
	}
	return string(plaintext), nil
}

func keyBytes(key string) []byte {
	sum := sha256.Sum256([]byte(key))
	return sum[:]
}
