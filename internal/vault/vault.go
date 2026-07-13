//go:generate go tool mockgen -destination=mocks/vaulter.go -package mock_vault github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault VaultClientInterface

package vault

import (
	"context"
	"errors"

	vaultapi "github.com/hashicorp/vault/api"
	"go.uber.org/zap"
)

const (
	SecretPrefix  = "secret"
	TransitPrefix = "transit"
)

var ErrSecretNotFound = errors.New("not found")

type VaultClientInterface interface {
	ReadSecret(ctx context.Context, path string, version int) (map[string]interface{}, error)

	ReadKey(ctx context.Context, keyName string) (*TransitKey, error)
	CreateKey(ctx context.Context, keyName, autoRotatePeriod string) error
	SignData(ctx context.Context, key string, input string) (map[string]interface{}, error)
	VerifySignature(ctx context.Context, key, input, signature string) (bool, error)
}

type vaultClient struct {
	path        string
	transitPath string
	client      *vaultapi.Client
	logger      *zap.Logger
}

// NewVaultClient creates a new instance of a Client for internal Vault
func NewVaultClient(vaultApiClient *vaultapi.Client, logger *zap.Logger) VaultClientInterface {
	return &vaultClient{
		client:      vaultApiClient,
		path:        SecretPrefix,
		transitPath: TransitPrefix,
		logger:      logger,
	}
}
