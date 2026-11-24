package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

// Lock before the Deployment row, matching mutation admission's lock order.
func lockPipelineNoEffectTarget(ctx context.Context, tx model.Tx, request UpdateDeploymentResultsRequestObject) error {
	if request.Body.NoEffectReceipt == nil {
		return nil
	}
	var environment uuid.UUID
	if err := tx.QueryRowContext(ctx, `SELECT d.de_id FROM deployments d JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1 AND e.org_id=$2`, request.DeploymentId, request.OrgId).Scan(&environment); err != nil {
		return pipelineNoEffectError(err)
	}
	return model.LockEnvironmentMutation(ctx, tx, environment)
}

func ingestPipelineNoEffect(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary, body *DeploymentResultsUpdateBody, completionProven bool) error {
	if err := recordPipelineResultEvidence(ctx, tx, dep, body, completionProven); err != nil {
		return err
	}
	receipt := body.NoEffectReceipt
	if receipt == nil {
		return nil
	}
	if !completionProven || !dep.HasPipeline || body.Status != Failure || body.Outputs != nil || len(body.Metadata) != 0 || body.ObservationResult != nil ||
		receipt.Version != "stellwerk.no-effect/v1" || receipt.Effect != "not_started" || body.TfResourceCounts != (DeploymentTFResourceCounts{}) {
		return fmt.Errorf("%w: invalid pipeline no-effect completion", ErrRunnerEventTargetInvalid)
	}
	return pipelineNoEffectError(model.RecordPipelineNoEffect(ctx, tx, dep, receipt.CommandSha256))
}

func recordPipelineResultEvidence(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary, body *DeploymentResultsUpdateBody, completionProven bool) error {
	if !completionProven || !dep.HasPipeline || body.Status != Success && body.Outputs == nil && len(body.Metadata) == 0 && body.TfResourceCounts == (DeploymentTFResourceCounts{}) {
		return nil
	}
	return pipelineNoEffectError(model.RecordPipelineEffectEvidence(ctx, tx, dep.Id.String()))
}

func pipelineNoEffectError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %w", ErrRunnerEventTargetInvalid, model.NewErrNotFound("pipeline target not found"))
	}
	_, conflict := model.IsErrConflict(err)
	if conflict {
		return fmt.Errorf("%w: %w", ErrRunnerEventTargetInvalid, err)
	}
	return err
}
