package model

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// ExternalOperationRollbackAnchor separates the first captured recovery base
// from later forward attempts' fresh admission snapshots.
type ExternalOperationRollbackAnchor struct {
	OrgID, PluginID, OperationRef string
	ProjectID, EnvID              string
	ProjectUUID, EnvironmentUUID  uuid.UUID
	FirstForwardDeploymentID      uuid.UUID
	RollbackBaseDeploymentID      uuid.UUID
}

// LockExternalOperationRollbackAnchor serializes even the absence of an
// anchor. It must be held through ordinary admission and receipt creation.
func LockExternalOperationRollbackAnchor(ctx context.Context, tx Tx, scope ExternalOperationRollbackAnchor) (*ExternalOperationRollbackAnchor, error) {
	if tx == nil {
		return nil, errors.New("transaction is required")
	}
	identity, err := json.Marshal([]string{"external-operation-rollback-anchor", scope.OrgID, scope.PluginID, scope.OperationRef, scope.EnvironmentUUID.String()})
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", string(identity)); err != nil {
		return nil, errors.Wrap(err, "failed to lock operation rollback lineage")
	}
	return ReadExternalOperationRollbackAnchor(ctx, tx, scope)
}

func ReadExternalOperationRollbackAnchor(ctx context.Context, tx Tx, scope ExternalOperationRollbackAnchor) (*ExternalOperationRollbackAnchor, error) {
	if tx == nil {
		return nil, errors.New("transaction is required")
	}
	anchor := scope
	err := tx.QueryRowContext(ctx, `SELECT project_id, env_id, project_uuid, first_forward_deployment_id, rollback_base_deployment_id
		FROM external_operation_rollback_anchors WHERE org_id=$1 AND plugin_id=$2 AND operation_ref=$3 AND environment_uuid=$4`,
		scope.OrgID, scope.PluginID, scope.OperationRef, scope.EnvironmentUUID).Scan(&anchor.ProjectID, &anchor.EnvID, &anchor.ProjectUUID, &anchor.FirstForwardDeploymentID, &anchor.RollbackBaseDeploymentID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to read operation rollback lineage")
	}
	if anchor.ProjectID != scope.ProjectID || anchor.EnvID != scope.EnvID || anchor.ProjectUUID != scope.ProjectUUID {
		return nil, NewErrConflict("operation rollback lineage scope changed")
	}
	return &anchor, nil
}

func CreateExternalOperationRollbackAnchor(ctx context.Context, tx Tx, anchor ExternalOperationRollbackAnchor) error {
	if tx == nil {
		return errors.New("transaction is required")
	}
	if anchor.FirstForwardDeploymentID == uuid.Nil || anchor.RollbackBaseDeploymentID == uuid.Nil || anchor.ProjectUUID == uuid.Nil || anchor.EnvironmentUUID == uuid.Nil ||
		anchor.OrgID == "" || anchor.PluginID == "" || anchor.OperationRef == "" || anchor.ProjectID == "" || anchor.EnvID == "" {
		return NewErrBadRequest("operation rollback anchor is incomplete")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO external_operation_rollback_anchors
		(org_id,plugin_id,operation_ref,environment_uuid,project_uuid,project_id,env_id,first_forward_deployment_id,rollback_base_deployment_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, anchor.OrgID, anchor.PluginID, anchor.OperationRef, anchor.EnvironmentUUID,
		anchor.ProjectUUID, anchor.ProjectID, anchor.EnvID, anchor.FirstForwardDeploymentID, anchor.RollbackBaseDeploymentID)
	return errors.Wrap(err, "failed to retain immutable operation rollback anchor")
}
