package model

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
)

// RequireStateCustody refuses an unreviewed state-location transfer. Creation
// calls it under LockEnvironmentMutation; an earlier read is only a precheck.
func RequireStateCustody(ctx context.Context, tx Tx, org, project, env string, environment uuid.UUID, runner string, selected *cp.NativeExecutionTargetReference) error {
	if tx == nil {
		return errors.New("state custody check requires a transaction")
	}
	if selected != nil && !validNativeExecutionTarget(selected, runner) {
		return NewErrConflict("selected state custody is malformed; explicit handoff is required")
	}
	var matches int
	var id uuid.UUID
	var storedOrg, storedProject, storedEnv string
	var anchor, anchorEnvironment uuid.NullUUID
	var anchorRunner sql.NullString
	var retained *cp.NativeExecutionTargetReference
	var history bool
	err := tx.QueryRowContext(ctx, `SELECT count(*) OVER (), e.id,e.org_id,e.project_id,e.env_id,
 COALESCE(effective.deployment_id,e.last_state_deployment_id),d.de_id,d.runner_id,d.native_target,
 EXISTS(SELECT 1 FROM deployments history WHERE history.de_id=e.id)
 FROM deployment_environments e
 LEFT JOIN deployment_effective_state effective ON effective.environment_id=e.id
 LEFT JOIN deployments d ON d.id=COALESCE(effective.deployment_id,e.last_state_deployment_id)
 WHERE e.id=$1 OR (e.org_id=$2 AND e.project_id=$3 AND e.env_id=$4)
 LIMIT 1`, environment, org, project, env).Scan(&matches, &id, &storedOrg, &storedProject, &storedEnv, &anchor, &anchorEnvironment, &anchorRunner, asJson(&retained), &history)
	if errors.Is(err, sql.ErrNoRows) {
		// Missing local history is not proof of globally empty provider state.
		return nil
	}
	if err != nil {
		return err
	}
	if matches != 1 || id != environment || storedOrg != org || storedProject != project || storedEnv != env ||
		(anchor.Valid && (!anchorEnvironment.Valid || anchorEnvironment.UUID != environment)) || (!anchor.Valid && history) {
		return NewErrConflict("retained state custody identity is inconsistent; explicit handoff is required")
	}
	if retained == nil && selected == nil {
		return nil
	}
	if anchor.Valid && (retained == nil || selected == nil || !validNativeExecutionTarget(retained, anchorRunner.String) ||
		retained.RunnerUuid != selected.RunnerUuid || retained.StateBackendType != selected.StateBackendType || retained.StateConfigurationDigest != selected.StateConfigurationDigest) {
		return NewErrConflict("state custody changed; explicit handoff is required").AboutDeployment(anchor.UUID)
	}
	return requirePriorStateWritersStopped(ctx, tx, org, environment)
}

func validNativeExecutionTarget(target *cp.NativeExecutionTargetReference, runner string) bool {
	if target == nil {
		return false
	}
	_, err := hmessaging.NativeRunnerToken(target.RunnerUuid.String())
	return err == nil && target.RunnerId == runner && nativeCommandRunnerID.MatchString(target.RunnerId) &&
		target.RuntimeVersion.Valid() && target.PipelineVersion.Valid() && target.StateBackendType.Valid() &&
		nativeCommandDigest(target.ToolsDigest) && nativeCommandDigest(target.StateConfigurationDigest)
}

func requirePriorStateWritersStopped(ctx context.Context, tx Tx, org string, environment uuid.UUID) error {
	var unresolved uuid.UUID
	err := tx.QueryRowContext(ctx, `SELECT d.id FROM deployments d
 JOIN deployment_environments e ON e.id=d.de_id
 LEFT JOIN deployment_dispatch_budgets budget ON budget.deployment_id=d.id
 WHERE e.id=$1 AND e.org_id=$2 AND d.mode IN ('deploy','rollback','destroy')
 AND (budget.deployment_id IS NULL OR budget.first_attempt_at IS NOT NULL)
 AND budget.completion_received_at IS NULL
 AND NOT EXISTS(SELECT 1 FROM runner_event_inbox result WHERE result.deployment_id=d.id
     AND result.event_type='deployment-result' AND result.org_id=e.org_id AND result.runner_id=d.runner_id)
 AND NOT EXISTS(SELECT 1 FROM deployment_late_runner_results late WHERE late.deployment_id=d.id
     AND late.org_id=e.org_id AND late.runner_id=d.runner_id)
 AND NOT EXISTS(SELECT 1 FROM deployment_outcome_acknowledgements ack WHERE ack.deployment_id=d.id AND ack.org_id=e.org_id)
 LIMIT 1`, environment, org).Scan(&unresolved)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return NewErrCodedConflict(CodeOutcomeUnreconciled, "a prior state-writing Deployment has no authoritative stop proof").AboutDeployment(unresolved)
}
