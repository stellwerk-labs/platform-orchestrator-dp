package model

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

const RunnerExecutionBudget = time.Hour

func LockEnvironmentMutation(ctx context.Context, tx Tx, environmentID uuid.UUID) error {
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "deployment-environment:"+environmentID.String())
	return err
}

type DeploymentDispatchBudget struct {
	Revision             int
	FirstAttemptAt       sql.NullTime
	Deadline             sql.NullTime
	CompletionReceivedAt sql.NullTime
	Legacy               bool
}

func (d *databaser) BeginDeploymentDispatch(ctx context.Context, dep *DeploymentSummary) (time.Time, error) {
	return BeginDeploymentDispatch(ctx, d, dep)
}

func (d *databaser) GetDeploymentDispatchBudget(ctx context.Context, optionalTx Tx, deploymentID uuid.UUID) (DeploymentDispatchBudget, error) {
	return ReadDeploymentDispatchBudget(ctx, d.txOrDb(optionalTx), deploymentID)
}

func ReadDeploymentDispatchBudget(ctx context.Context, tx Tx, deploymentID uuid.UUID) (DeploymentDispatchBudget, error) {
	var out DeploymentDispatchBudget
	err := tx.QueryRowContext(ctx, `SELECT deployment_revision, first_attempt_at, deadline, completion_received_at, legacy
        FROM deployment_dispatch_budgets WHERE deployment_id=$1`, deploymentID).Scan(
		&out.Revision, &out.FirstAttemptAt, &out.Deadline, &out.CompletionReceivedAt, &out.Legacy)
	return out, err
}

// BeginDeploymentDispatch commits the immutable budget before a Runner can
// receive any command. A retry can consume remaining time, never reset it.
func BeginDeploymentDispatch(ctx context.Context, db Databaser, dep *DeploymentSummary) (time.Time, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := LockEnvironmentMutation(ctx, tx, dep.DeploymentEnvUuid); err != nil {
		return time.Time{}, err
	}
	var revision int
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT revision,status FROM deployments WHERE id=$1 FOR UPDATE`, dep.Id).Scan(&revision, &status); err != nil {
		return time.Time{}, err
	}
	if revision != dep.Revision || status != string(DeploymentStatusExecuting) {
		return time.Time{}, NewErrConflict("deployment changed before Runner dispatch")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now(), deadline=now()+interval '1 hour'
        WHERE deployment_id=$1 AND deployment_revision=$2 AND first_attempt_at IS NULL AND NOT legacy`, dep.Id, dep.Revision); err != nil {
		return time.Time{}, err
	}
	budget, err := ReadDeploymentDispatchBudget(ctx, tx, dep.Id)
	if err != nil {
		return time.Time{}, err
	}
	if budget.Revision != dep.Revision || !budget.Deadline.Valid || !time.Now().Before(budget.Deadline.Time) {
		return time.Time{}, NewErrConflict("Runner dispatch budget is exhausted or unknown")
	}
	if err := tx.Commit(); err != nil {
		return time.Time{}, err
	}
	return budget.Deadline.Time, nil
}

// DeploymentStopEvidence is what DP knows about whether a Runner attempt of one
// Deployment can still change its Environment.
type DeploymentStopEvidence struct {
	Failed bool
	// PluginDispatched is set when a plugin operation receipt admitted the
	// Deployment. Ordinary Core Deployments never have one.
	PluginDispatched bool
	// Dispatched is set when a durable first dispatch attempt exists, or when
	// no dispatch budget exists at all and the attempt must be assumed.
	Dispatched bool
	// StopProven is set by an authenticated Runner completion: an HTTP result,
	// the NATS result inbox receipt, or a retained genuine late result.
	StopProven bool
}

// OutcomeUnreconciled reports a possibly started Runner attempt without stop
// proof.
func (e DeploymentStopEvidence) OutcomeUnreconciled() bool {
	return e.Dispatched && !e.StopProven
}

// BlocksReplacement fences an Environment only behind a failed
// plugin-dispatched Deployment with an unreconciled outcome. An ordinary Core
// Deployment failure never blocks the next Deployment, exactly as before
// dispatch evidence existed.
func (e DeploymentStopEvidence) BlocksReplacement() bool {
	return e.Failed && e.PluginDispatched && e.OutcomeUnreconciled()
}

func ReadDeploymentStopEvidence(ctx context.Context, tx Tx, deploymentID uuid.UUID) (DeploymentStopEvidence, error) {
	var evidence DeploymentStopEvidence
	err := tx.QueryRowContext(ctx, `SELECT d.status='failed',
        EXISTS (SELECT 1 FROM external_operation_deployments receipt WHERE receipt.deployment_id=d.id AND receipt.org_id=e.org_id),
        budget.deployment_id IS NULL OR budget.first_attempt_at IS NOT NULL,
        budget.completion_received_at IS NOT NULL
          OR EXISTS (SELECT 1 FROM runner_event_inbox result WHERE result.deployment_id=d.id AND result.event_type='deployment-result'
                     AND result.org_id=e.org_id AND result.runner_id=d.runner_id)
          OR EXISTS (SELECT 1 FROM deployment_late_runner_results late WHERE late.deployment_id=d.id
                     AND late.org_id=e.org_id AND late.runner_id=d.runner_id)
        FROM deployments d
        JOIN deployment_environments e ON e.id=d.de_id
        LEFT JOIN deployment_dispatch_budgets budget ON budget.deployment_id=d.id
        WHERE d.id=$1`, deploymentID).Scan(
		&evidence.Failed, &evidence.PluginDispatched, &evidence.Dispatched, &evidence.StopProven)
	if errors.Is(err, sql.ErrNoRows) {
		return DeploymentStopEvidence{}, NewErrNotFound("deployment not found for stop evidence")
	}
	return evidence, err
}

// DeploymentOutcomeUnreconciled distinguishes a failure record from proof that
// a possibly-started Runner has stopped. Normal authenticated HTTP results and
// NATS results both mark completion in their existing update transaction.
func DeploymentOutcomeUnreconciled(ctx context.Context, tx Tx, deploymentID uuid.UUID) (bool, error) {
	evidence, err := ReadDeploymentStopEvidence(ctx, tx, deploymentID)
	if err != nil {
		return false, err
	}
	return evidence.OutcomeUnreconciled(), nil
}

func RecordDeploymentCompletion(ctx context.Context, tx Tx, deploymentID uuid.UUID) error {
	_, err := tx.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET completion_received_at=COALESCE(completion_received_at,now()) WHERE deployment_id=$1`, deploymentID)
	return err
}
