package api

import (
	"context"
	"errors"

	"filippo.io/age"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

const deploymentLogMaterialTransitKey = "deployment-log-material"

// DeploymentLogMaterialService owns identities only for Deployments without a
// caller-supplied recipient. Only wrapped private identities reach the database.
type DeploymentLogMaterialService struct{ Vault vault.VaultClientInterface }

func (s DeploymentLogMaterialService) Generate(ctx context.Context) (*model.DeploymentLogMaterial, error) {
	if s.Vault == nil {
		return nil, errors.New("deployment log material service is not configured")
	}
	key, err := s.Vault.ReadKey(ctx, deploymentLogMaterialTransitKey)
	if errors.Is(err, vault.ErrSecretNotFound) {
		if err := s.Vault.CreateEncryptionKey(ctx, deploymentLogMaterialTransitKey, externalOperationMaterialKeyRotationPeriod); err != nil {
			return nil, errors.New("failed to create deployment log Transit key")
		}
		key, err = s.Vault.ReadKey(ctx, deploymentLogMaterialTransitKey)
	}
	if err != nil {
		return nil, errors.New("failed to read deployment log Transit key")
	}
	if key.Type != vault.AES256GCM96 || key.Exportable {
		return nil, errors.New("deployment log Transit key must be non-exportable and symmetric")
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		return nil, errors.New("failed to generate deployment log identity")
	}
	plaintext := []byte(identity.String())
	defer clear(plaintext)
	wrapped, err := s.Vault.Encrypt(ctx, deploymentLogMaterialTransitKey, plaintext)
	if err != nil {
		return nil, errors.New("failed to wrap deployment log identity")
	}
	return &model.DeploymentLogMaterial{Recipient: identity.Recipient().String(), EncryptedIdentity: wrapped}, nil
}

func (s DeploymentLogMaterialService) Identity(ctx context.Context, material model.DeploymentLogMaterial) (age.Identity, error) {
	if s.Vault == nil {
		return nil, errors.New("deployment log material service is not configured")
	}
	plaintext, err := s.Vault.Decrypt(ctx, deploymentLogMaterialTransitKey, material.EncryptedIdentity)
	if err != nil {
		return nil, errors.New("deployment log identity is unavailable")
	}
	defer clear(plaintext)
	identity, err := age.ParseX25519Identity(string(plaintext))
	if err != nil || identity.Recipient().String() != material.Recipient {
		return nil, errors.New("stored deployment log identity is invalid")
	}
	return identity, nil
}

func (options DeploymentOptions) prepareLogMaterial(ctx context.Context, recipient *string) (*string, *model.DeploymentLogMaterial, error) {
	if !options.ManagedLogs {
		return recipient, nil, nil
	}
	if recipient != nil || options.LogMaterial == nil {
		return nil, nil, errors.New("managed deployment logs require a material service and no caller recipient")
	}
	material, err := options.LogMaterial.Generate(ctx)
	if err != nil {
		return nil, nil, err
	}
	return &material.Recipient, material, nil
}
