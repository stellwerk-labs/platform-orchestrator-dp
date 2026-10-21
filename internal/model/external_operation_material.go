package model

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// ExternalOperationMaterial stores only an age public recipient and the
// corresponding private identity wrapped by DP's non-exportable Transit key.
type ExternalOperationMaterial struct {
	OrgID             string
	PluginID          string
	OperationRef      uuid.UUID
	Recipient         string
	EncryptedIdentity string
	CreatedAt         time.Time
}

func GetExternalOperationMaterial(ctx context.Context, tx Tx, orgID, pluginID string, operationRef uuid.UUID) (*ExternalOperationMaterial, error) {
	if tx == nil {
		return nil, errors.New("transaction is required")
	}
	material := &ExternalOperationMaterial{OrgID: orgID, PluginID: pluginID, OperationRef: operationRef}
	if err := tx.QueryRowContext(ctx, `SELECT recipient, encrypted_identity, created_at
		FROM external_operation_material WHERE org_id=$1 AND plugin_id=$2 AND operation_ref=$3`,
		orgID, pluginID, operationRef,
	).Scan(&material.Recipient, &material.EncryptedIdentity, &material.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, NewErrNotFound("external operation encryption material not found")
		}
		return nil, errors.Wrap(err, "failed to read external operation encryption material")
	}
	return material, nil
}

func StoreExternalOperationMaterial(ctx context.Context, tx Tx, material ExternalOperationMaterial) (*ExternalOperationMaterial, error) {
	if tx == nil {
		return nil, errors.New("transaction is required")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO external_operation_material
		(org_id, plugin_id, operation_ref, recipient, encrypted_identity)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT (org_id,plugin_id,operation_ref) DO NOTHING`,
		material.OrgID, material.PluginID, material.OperationRef, material.Recipient, material.EncryptedIdentity,
	); err != nil {
		return nil, errors.Wrap(err, "failed to store external operation encryption material")
	}
	return GetExternalOperationMaterial(ctx, tx, material.OrgID, material.PluginID, material.OperationRef)
}
