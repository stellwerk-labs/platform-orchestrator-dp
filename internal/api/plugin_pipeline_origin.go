package api

import (
	"context"
	"database/sql"
	"errors"
	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

type deploymentPluginOrigin struct {
	PluginID         string     `json:"plugin_id"`
	RequestID        string     `json:"request_id"`
	OperationRef     *string    `json:"operation_ref,omitempty"`
	ObservationRunID *uuid.UUID `json:"observation_run_id,omitempty"`
}

func readDeploymentPluginOrigin(ctx context.Context, tx model.Tx, org string, id uuid.UUID) (*deploymentPluginOrigin, error) {
	var value deploymentPluginOrigin
	var operation sql.NullString
	var observation uuid.NullUUID
	err := tx.QueryRowContext(ctx, `SELECT plugin_id,request_id,operation_ref,NULL::uuid FROM external_operation_deployments WHERE org_id=$1 AND deployment_id=$2
 UNION ALL SELECT plugin_id,request_id::text,NULL::text,run_id FROM environment_observation_redeploy_receipts WHERE org_id=$1 AND deployment_id=$2 LIMIT 1`, org, id).Scan(&value.PluginID, &value.RequestID, &operation, &observation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if operation.Valid {
		value.OperationRef = &operation.String
	}
	if observation.Valid {
		value.ObservationRunID = &observation.UUID
	}
	return &value, nil
}
