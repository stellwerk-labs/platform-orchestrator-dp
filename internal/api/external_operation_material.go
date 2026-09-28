package api

import (
	"context"
	"errors"

	"filippo.io/age"
	"github.com/google/uuid"
	pkgerrors "github.com/pkg/errors"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

const (
	externalOperationMaterialTransitKey        = "external-operation-material"
	externalOperationMaterialKeyRotationPeriod = "720h"
)

// ExternalOperationMaterialService creates one DP-owned age identity per
// authorized plugin operation. Only the public recipient reaches the ordinary
// Deployment. The private identity remains wrapped by Vault Transit in DP's
// database and is never returned through plugin admission APIs.
type ExternalOperationMaterialService struct {
	Vault vault.VaultClientInterface
}

func (s ExternalOperationMaterialService) GetOrCreate(ctx context.Context, tx model.Tx, orgID, pluginID string, operationRef uuid.UUID) (*model.ExternalOperationMaterial, error) {
	if s.Vault == nil || tx == nil || orgID == "" || pluginID == "" || operationRef == uuid.Nil {
		return nil, errors.New("external operation material service is not configured")
	}
	material, err := model.GetExternalOperationMaterial(ctx, tx, orgID, pluginID, operationRef)
	if err == nil {
		if _, parseErr := age.ParseX25519Recipient(material.Recipient); parseErr != nil || material.EncryptedIdentity == "" {
			return nil, errors.New("stored external operation encryption material is invalid")
		}
		return material, nil
	}
	if _, notFound := model.IsErrNotFound(err); !notFound {
		return nil, err
	}

	if key, err := s.Vault.ReadKey(ctx, externalOperationMaterialTransitKey); err != nil {
		if !errors.Is(err, vault.ErrSecretNotFound) {
			return nil, pkgerrors.Wrap(err, "failed to read external operation material Transit key")
		}
		if err := s.Vault.CreateEncryptionKey(ctx, externalOperationMaterialTransitKey, externalOperationMaterialKeyRotationPeriod); err != nil {
			return nil, pkgerrors.Wrap(err, "failed to create external operation material Transit key")
		}
	} else if key.Type != vault.AES256GCM96 || key.Exportable {
		return nil, errors.New("external operation material Transit key is not a non-exportable symmetric key")
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		return nil, pkgerrors.Wrap(err, "failed to generate external operation age identity")
	}
	encryptedIdentity, err := s.Vault.Encrypt(ctx, externalOperationMaterialTransitKey, []byte(identity.String()))
	if err != nil {
		return nil, pkgerrors.Wrap(err, "failed to wrap external operation age identity")
	}
	return model.StoreExternalOperationMaterial(ctx, tx, model.ExternalOperationMaterial{
		OrgID: orgID, PluginID: pluginID, OperationRef: operationRef,
		Recipient: identity.Recipient().String(), EncryptedIdentity: encryptedIdentity,
	})
}
