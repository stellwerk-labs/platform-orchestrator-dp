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

// DeploymentOutcomeUnreconciled distinguishes a failure record from proof that
// a possibly-started Runner has stopped. Normal authenticated HTTP results and
// NATS results both mark completion in their existing update transaction.
func DeploymentOutcomeUnreconciled(ctx context.Context, tx Tx, deploymentID uuid.UUID) (bool, error) {
	budget, err := ReadDeploymentDispatchBudget(ctx, tx, deploymentID)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if !budget.FirstAttemptAt.Valid || budget.CompletionReceivedAt.Valid {
		return false, nil
	}
	var stopped bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runner_event_inbox result JOIN deployments d ON d.id=result.deployment_id
        JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1 AND result.event_type='deployment-result'
        AND result.org_id=e.org_id AND result.runner_id=d.runner_id)
        OR EXISTS (SELECT 1 FROM deployment_late_runner_results result JOIN deployments d ON d.id=result.deployment_id
        JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1 AND result.org_id=e.org_id AND result.runner_id=d.runner_id)`, deploymentID).Scan(&stopped)
	return !stopped, err
}

func RecordDeploymentCompletion(ctx context.Context, tx Tx, deploymentID uuid.UUID) error {
	_, err := tx.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET completion_received_at=COALESCE(completion_received_at,now()) WHERE deployment_id=$1`, deploymentID)
	return err
}
