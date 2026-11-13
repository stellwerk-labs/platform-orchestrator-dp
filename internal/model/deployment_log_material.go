package model

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
)

// DeploymentLogMaterial retains only a public recipient and a Transit-wrapped
// private identity. Caller-owned identities are never stored here.
type DeploymentLogMaterial struct {
	Recipient         string
	EncryptedIdentity string
}

func StoreDeploymentLogMaterial(ctx context.Context, tx Tx, orgID string, deploymentID uuid.UUID, material DeploymentLogMaterial) error {
	result, err := tx.ExecContext(ctx, `INSERT INTO deployment_log_material (deployment_id, org_id, recipient, encrypted_identity)
        SELECT d.id, e.org_id, $3, $4 FROM deployments d
        JOIN deployment_environments e ON e.id=d.de_id
        WHERE d.id=$1 AND e.org_id=$2 AND d.encrypted_logs_recipient=$3`, deploymentID, orgID, material.Recipient, material.EncryptedIdentity)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.New("deployment log identity does not match its deployment")
	}
	return nil
}

func GetDeploymentLogMaterial(ctx context.Context, tx Tx, orgID string, deploymentID uuid.UUID) (*DeploymentLogMaterial, error) {
	var material DeploymentLogMaterial
	err := tx.QueryRowContext(ctx, `SELECT m.recipient, m.encrypted_identity FROM deployment_log_material m
        JOIN deployments d ON d.id=m.deployment_id
        JOIN deployment_environments e ON e.id=d.de_id
        WHERE m.deployment_id=$1 AND m.org_id=$2 AND e.org_id=$2
        AND m.recipient=d.encrypted_logs_recipient`, deploymentID, orgID).Scan(&material.Recipient, &material.EncryptedIdentity)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewErrNotFound("server-managed deployment log identity is not retained")
	}
	if err != nil {
		return nil, err
	}
	return &material, nil
}
