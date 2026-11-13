// Package costestimation handles custody of the installation's Infracost key.
package costestimation

import (
	"context"
	"errors"
	"time"
	"unicode"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

const transitKey = "cost-estimation-api-key"
const maximumAPIKeyBytes = 4096
const credentialTimeout = 5 * time.Second

var ErrCredentialUnavailable = errors.New("cost estimation credential is unavailable")

func ValidAPIKey(value string) bool {
	if len(value) > maximumAPIKeyBytes {
		return false
	}
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// Encrypt returns only sanitized errors. Vault diagnostics must not enter HTTP
// responses or logs because an upstream response may contain credential data.
func Encrypt(ctx context.Context, vlt vault.VaultClientInterface, value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if vlt == nil || !ValidAPIKey(value) {
		return "", ErrCredentialUnavailable
	}
	key, err := vlt.ReadKey(ctx, transitKey)
	if errors.Is(err, vault.ErrSecretNotFound) {
		if err := vlt.CreateEncryptionKey(ctx, transitKey, "720h"); err != nil {
			return "", ErrCredentialUnavailable
		}
		key, err = vlt.ReadKey(ctx, transitKey)
	}
	if err != nil || key == nil || key.Type != vault.AES256GCM96 || key.Exportable {
		return "", ErrCredentialUnavailable
	}
	ciphertext, err := vlt.Encrypt(ctx, transitKey, []byte(value))
	if err != nil || ciphertext == "" {
		return "", ErrCredentialUnavailable
	}
	return ciphertext, nil
}

func Resolve(ctx context.Context, db model.Databaser, vlt vault.VaultClientInterface) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, credentialTimeout)
	defer cancel()
	if db == nil || vlt == nil {
		return "", ErrCredentialUnavailable
	}
	ciphertext, err := model.ReadCostAPIKeyCiphertext(ctx, db)
	if err != nil || ciphertext == "" {
		return "", ErrCredentialUnavailable
	}
	plaintext, err := vlt.Decrypt(ctx, transitKey, ciphertext)
	if err != nil || len(plaintext) == 0 || !ValidAPIKey(string(plaintext)) {
		return "", ErrCredentialUnavailable
	}
	return string(plaintext), nil
}
