package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

// ExternalOperationDeploymentKey is the durable idempotency identity for one
// plugin request. A request ID cannot be reused for a different operation.
type ExternalOperationDeploymentKey struct {
	OrgID     string
	PluginID  string
	RequestID string
}

// ExternalOperationDeploymentReceipt links one authorized external operation
// request to the ordinary Deployment created by the Data Plane.
type ExternalOperationDeploymentReceipt struct {
	ExternalOperationDeploymentKey
	OperationRef      string
	Fence             int64
	AssertionAction   string
	ServiceAction     string
	RequestBodySHA256 string
	ActorID           string
	ProjectID         string
	EnvID             string
	ProjectUUID       opt.Opt[uuid.UUID]
	EnvironmentUUID   opt.Opt[uuid.UUID]
	DeploymentID      uuid.UUID
	CreatedAt         time.Time
}

// GetExternalOperationDeploymentReceipt serializes the first use of a request
// identity, including the interval before a receipt row exists. The caller must
// keep tx open through creation of both the normal Deployment and its receipt.
func GetExternalOperationDeploymentReceipt(ctx context.Context, tx Tx, key ExternalOperationDeploymentKey) (*ExternalOperationDeploymentReceipt, error) {
	if tx == nil {
		return nil, errors.New("transaction is required")
	}
	identity, err := json.Marshal([]string{"external-operation-deployment", key.OrgID, key.PluginID, key.RequestID})
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode external operation lock")
	}
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", string(identity)); err != nil {
		return nil, errors.Wrap(err, "failed to lock external operation request")
	}

	receipt := &ExternalOperationDeploymentReceipt{ExternalOperationDeploymentKey: key}
	if err := tx.QueryRowContext(ctx, `
		SELECT operation_ref, fence, assertion_action, service_action, request_body_sha256, actor_id, project_id, env_id, project_uuid, environment_uuid, deployment_id, created_at
		FROM external_operation_deployments
		WHERE org_id=$1 AND plugin_id=$2 AND request_id=$3
		FOR UPDATE
	`, key.OrgID, key.PluginID, key.RequestID).Scan(
		&receipt.OperationRef,
		&receipt.Fence,
		&receipt.AssertionAction,
		&receipt.ServiceAction,
		&receipt.RequestBodySHA256,
		&receipt.ActorID,
		&receipt.ProjectID,
		&receipt.EnvID,
		opt.Scan(&receipt.ProjectUUID),
		opt.Scan(&receipt.EnvironmentUUID),
		&receipt.DeploymentID,
		&receipt.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, NewErrNotFound("no deployment receipt found for the external operation request")
		}
		return nil, errors.Wrap(err, "failed to query external operation deployment receipt")
	}
	return receipt, nil
}

func ReadExternalOperationDeploymentReceiptByDeployment(ctx context.Context, tx Tx, orgID, pluginID string, deploymentID uuid.UUID) (*ExternalOperationDeploymentReceipt, error) {
	if tx == nil {
		return nil, errors.New("transaction is required")
	}
	receipt := &ExternalOperationDeploymentReceipt{
		ExternalOperationDeploymentKey: ExternalOperationDeploymentKey{OrgID: orgID, PluginID: pluginID},
		DeploymentID:                   deploymentID,
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT request_id, operation_ref, fence, assertion_action, service_action, request_body_sha256, actor_id,
		       project_id, env_id, project_uuid, environment_uuid, created_at
		FROM external_operation_deployments
		WHERE org_id=$1 AND plugin_id=$2 AND deployment_id=$3
	`, orgID, pluginID, deploymentID).Scan(
		&receipt.RequestID, &receipt.OperationRef, &receipt.Fence, &receipt.AssertionAction, &receipt.ServiceAction,
		&receipt.RequestBodySHA256, &receipt.ActorID, &receipt.ProjectID, &receipt.EnvID,
		opt.Scan(&receipt.ProjectUUID), opt.Scan(&receipt.EnvironmentUUID), &receipt.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, NewErrNotFound("no external operation receipt found for the deployment")
		}
		return nil, errors.Wrap(err, "failed to read external operation receipt by deployment")
	}
	return receipt, nil
}

// CreateExternalOperationDeploymentReceipt must run in the same transaction as
// CreateDeployment so neither the deployment nor its durable receipt can commit
// alone.
func CreateExternalOperationDeploymentReceipt(ctx context.Context, tx Tx, receipt ExternalOperationDeploymentReceipt) (*ExternalOperationDeploymentReceipt, error) {
	if tx == nil {
		return nil, errors.New("transaction is required")
	}
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO external_operation_deployments
			(org_id, plugin_id, operation_ref, fence, request_id, assertion_action, service_action, request_body_sha256, actor_id, project_id, env_id, project_uuid, environment_uuid, deployment_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		RETURNING created_at
	`, receipt.OrgID, receipt.PluginID, receipt.OperationRef, receipt.Fence, receipt.RequestID,
		receipt.AssertionAction, receipt.ServiceAction, receipt.RequestBodySHA256, receipt.ActorID, receipt.ProjectID, receipt.EnvID,
		receipt.ProjectUUID.Ref(), receipt.EnvironmentUUID.Ref(), receipt.DeploymentID,
	).Scan(&receipt.CreatedAt); err != nil {
		return nil, errors.Wrap(err, "failed to insert external operation deployment receipt")
	}
	return &receipt, nil
}
