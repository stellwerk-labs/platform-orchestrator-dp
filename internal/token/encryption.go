package token

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"io"

	"github.com/pkg/errors"
)

var cryptoKeyBytes = []byte("oMKwFZSWPsbAbzjrDeTucccZeRgKgvrc")

func encrypt(token string) (string, error) {
	block, err := aes.NewCipher(cryptoKeyBytes)
	if err != nil {
		return "", errors.Wrap(err, "failed to create cipher")
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", errors.Wrap(err, "failed to create GCM")
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", errors.Wrap(err, "failed to generate nonce")
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(token), nil)

	return base64.URLEncoding.EncodeToString(ciphertext), nil
}

func decrypt(token string) (string, error) {
	ciphertext, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return "", errors.Wrap(err, "failed to decode base64")
	}

	block, err := aes.NewCipher(cryptoKeyBytes)
	if err != nil {
		return "", errors.Wrap(err, "failed to create cipher")
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", errors.Wrap(err, "failed to create GCM")
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", errors.New("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to decrypt token")
	}

	return string(plaintext), nil
}
