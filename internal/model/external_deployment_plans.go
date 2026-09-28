package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// ExternalDeploymentPlanSnapshot is a complete immutable planning result kept
// inside DP. External callers receive only its identity, fingerprint and a
// value-free projection.
type ExternalDeploymentPlanSnapshot struct {
	ID                uuid.UUID
	OrgID             string
	PluginID          string
	RequestID         string
	ActorID           uuid.UUID
	RequestBodySHA256 string
	ProjectID         string
	ProjectUUID       uuid.UUID
	EnvID             string
	EnvironmentUUID   uuid.UUID
	BaseDeploymentID  uuid.UUID
	Fingerprint       string
	Manifest          json.RawMessage
	Graph             json.RawMessage
	Diff              json.RawMessage
	ModuleArtifacts   []ModuleArtifactRequirement
	CreatedAt         time.Time
}

func CreateOrGetExternalDeploymentPlanSnapshot(ctx context.Context, tx Tx, candidate ExternalDeploymentPlanSnapshot) (*ExternalDeploymentPlanSnapshot, error) {
	if tx == nil {
		return nil, errors.New("transaction is required")
	}
	if candidate.ID == uuid.Nil {
		candidate.ID = uuid.New()
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO external_deployment_plan_snapshots
		(id, org_id, plugin_id, request_id, actor_id, request_body_sha256, project_id, project_uuid, env_id, environment_uuid,
		 base_deployment_id, fingerprint, manifest, graph, diff, module_artifacts)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		ON CONFLICT (org_id, plugin_id, request_id) DO NOTHING`,
		candidate.ID, candidate.OrgID, candidate.PluginID, candidate.RequestID, candidate.ActorID, candidate.RequestBodySHA256,
		candidate.ProjectID, candidate.ProjectUUID, candidate.EnvID, candidate.EnvironmentUUID, candidate.BaseDeploymentID, candidate.Fingerprint,
		candidate.Manifest, candidate.Graph, candidate.Diff, asJson(&candidate.ModuleArtifacts))
	if err != nil {
		return nil, errors.Wrap(err, "failed to store external deployment plan snapshot")
	}

	stored, err := GetExternalDeploymentPlanSnapshot(ctx, tx, candidate.OrgID, candidate.PluginID, candidate.RequestID)
	if err != nil {
		return nil, err
	}
	if stored.ActorID != candidate.ActorID || stored.RequestBodySHA256 != candidate.RequestBodySHA256 ||
		stored.ProjectID != candidate.ProjectID || stored.ProjectUUID != candidate.ProjectUUID || stored.EnvID != candidate.EnvID || stored.EnvironmentUUID != candidate.EnvironmentUUID {
		return nil, NewErrConflict("external deployment planning request identity was reused with different authority or scope")
	}
	return stored, nil
}

func GetExternalDeploymentPlanSnapshot(ctx context.Context, tx Tx, orgID, pluginID, requestID string) (*ExternalDeploymentPlanSnapshot, error) {
	if tx == nil {
		return nil, errors.New("transaction is required")
	}
	stored := &ExternalDeploymentPlanSnapshot{OrgID: orgID, PluginID: pluginID, RequestID: requestID}
	var artifacts json.RawMessage
	err := tx.QueryRowContext(ctx, `SELECT id, actor_id, request_body_sha256, project_id, project_uuid, env_id, environment_uuid,
		base_deployment_id, fingerprint, manifest, graph, diff, module_artifacts, created_at
		FROM external_deployment_plan_snapshots WHERE org_id=$1 AND plugin_id=$2 AND request_id=$3`,
		orgID, pluginID, requestID,
	).Scan(&stored.ID, &stored.ActorID, &stored.RequestBodySHA256, &stored.ProjectID, &stored.ProjectUUID, &stored.EnvID, &stored.EnvironmentUUID,
		&stored.BaseDeploymentID, &stored.Fingerprint, &stored.Manifest, &stored.Graph, &stored.Diff, &artifacts, &stored.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewErrNotFound("external deployment plan snapshot not found")
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to load external deployment plan snapshot")
	}
	if err := json.Unmarshal(artifacts, &stored.ModuleArtifacts); err != nil {
		return nil, errors.Wrap(err, "failed to decode external deployment plan module inventory")
	}
	return stored, nil
}
