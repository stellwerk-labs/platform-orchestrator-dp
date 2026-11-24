package model

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"

	"github.com/google/uuid"
)

// EffectiveStateDeploymentID keeps an operational attempt separate from a
// proven unchanged infrastructure base. The predecessor is captured already
// resolved, so repeated no-effect failures never create a recursive lookup.
func EffectiveStateDeploymentID(ctx context.Context, tx Tx, environment uuid.UUID) (uuid.UUID, error) {
	var id uuid.NullUUID
	err := tx.QueryRowContext(ctx, `SELECT deployment_id FROM deployment_effective_state WHERE environment_id=$1`, environment).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return uuid.Nil, nil
	}
	return id.UUID, err
}

// RecordPipelineNoEffect accepts only authenticated completion evidence, after
// its API caller checked the receipt version, disposition and result shape.
func RecordPipelineNoEffect(ctx context.Context, tx Tx, dep *DeploymentSummary, commandDigest string) error {
	if err := LockEnvironmentMutation(ctx, tx, dep.DeploymentEnvUuid); err != nil {
		return err
	}
	var command []byte
	var captured, applied, blocked, outputs, succeeded bool
	var metrics DeploymentMetrics
	var phase string
	row := tx.QueryRowContext(ctx, `SELECT p.command,p.effect_base_captured,
 COALESCE((p.progress->>'infrastructure_applied')::boolean,false),p.no_effect_blocked,
 COALESCE(p.progress->>'phase',''),d.outputs IS NOT NULL,d.status='succeeded',d.metrics
 FROM deployment_pipeline_runs p JOIN deployments d ON d.id=p.deployment_id
 WHERE p.deployment_id=$1 FOR UPDATE OF p`, dep.Id)
	if err := row.Scan(&command, &captured, &applied, &blocked, &phase, &outputs, &succeeded, asJson(&metrics)); err != nil {
		return err
	}
	if !captured || applied || blocked || phase == "outcome_unknown" || priorPipelineResultHasEffect(succeeded, outputs, metrics) || !pipelineCommandDigestMatches(command, dep, commandDigest) {
		return NewErrConflict("no-effect receipt does not match captured pipeline authority")
	}
	var previous string
	err := tx.QueryRowContext(ctx, `INSERT INTO pipeline_no_effect_receipts(deployment_id,command_sha256) VALUES($1,$2)
 ON CONFLICT(deployment_id) DO UPDATE SET command_sha256=pipeline_no_effect_receipts.command_sha256 RETURNING command_sha256`, dep.Id, commandDigest).Scan(&previous)
	if err != nil {
		return err
	}
	if previous != commandDigest {
		return NewErrConflict("no-effect completion is immutable")
	}
	return SettleDeploymentPipeline(ctx, tx, dep.Id, DeploymentStatusFailed)
}

func pipelineCommandDigestMatches(raw []byte, dep *DeploymentSummary, expected string) bool {
	payload, err := FrozenPipelinePayload(raw, dep)
	if err != nil {
		return false
	}
	sum := sha256.Sum256(payload)
	return expected == hex.EncodeToString(sum[:])
}

// RecordPipelineEffectEvidence retains positive result or uncertainty facts even
// when later progress or a duplicate event has no such evidence.
func RecordPipelineEffectEvidence(ctx context.Context, tx Tx, id string) error {
	var locked uuid.UUID
	if err := tx.QueryRowContext(ctx, `SELECT deployment_id FROM deployment_pipeline_runs WHERE deployment_id=$1 FOR UPDATE`, id).Scan(&locked); err != nil {
		return err
	}
	var stopped bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_no_effect_receipts WHERE deployment_id=$1)`, id).Scan(&stopped); err != nil {
		return err
	}
	if stopped {
		return NewErrConflict("pipeline effect contradicts immutable no-effect receipt")
	}
	_, err := tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET no_effect_blocked=true WHERE deployment_id=$1`, id)
	return err
}

func priorPipelineResultHasEffect(succeeded, outputs bool, metrics DeploymentMetrics) bool {
	return outputs || succeeded || metrics.TfResources != 0 || metrics.TfResourcesAdded != 0 || metrics.TfResourcesChanged != 0 || metrics.TfResourcesRemoved != 0
}
