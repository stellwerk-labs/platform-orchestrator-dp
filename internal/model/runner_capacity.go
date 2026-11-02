package model

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
)

var ErrRunnerCapacityFull = errors.New("runner has no available job capacity")

// RunnerAdmissionConfiguration reads only additive, nonsecret scheduling fields
// from the existing versioned Runner configuration union.
func RunnerAdmissionConfiguration(configuration any) (int, bool, error) {
	raw, err := json.Marshal(configuration)
	if err != nil {
		return 0, false, err
	}
	var fields struct {
		MaxParallelJobs    *int     `json:"max_parallel_jobs"`
		SupportedProtocols []string `json:"supported_protocols"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return 0, false, err
	}
	limit := 0
	if fields.MaxParallelJobs != nil {
		limit = *fields.MaxParallelJobs
		if limit <= 0 {
			return 0, false, fmt.Errorf("max_parallel_jobs must be positive")
		}
	}
	return limit, slices.Contains(fields.SupportedProtocols, "environment-observation/v1"), nil
}

// ReserveRunnerJob serializes every replica on the canonical organization and
// Runner identity. There is deliberately no clock-based ownership expiry.
func ReserveRunnerJob(ctx context.Context, tx Tx, dep *DeploymentSummary, limit int) error {
	if limit < 0 {
		return NewErrBadRequest("max_parallel_jobs must be positive when configured")
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "runner-job:"+dep.OrgId+":"+dep.RunnerId); err != nil {
		return err
	}
	var revision int
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT d.revision,d.status FROM deployments d JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1 AND d.runner_id=$2 AND e.org_id=$3 FOR UPDATE OF d`, dep.Id, dep.RunnerId, dep.OrgId).Scan(&revision, &status); err != nil {
		return err
	}
	if revision != dep.Revision || status != string(DeploymentStatusExecuting) {
		return NewErrConflict("deployment changed before Runner capacity admission")
	}
	var active, released bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runner_job_reservations WHERE deployment_id=$1 AND released_at IS NULL), EXISTS(SELECT 1 FROM runner_job_reservations WHERE deployment_id=$1 AND released_at IS NOT NULL)`, dep.Id).Scan(&active, &released); err != nil {
		return err
	}
	if active {
		return nil
	}
	if released {
		return NewErrConflict("Runner job ownership has already been released")
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM runner_job_reservations WHERE org_id=$1 AND runner_id=$2 AND released_at IS NULL`, dep.OrgId, dep.RunnerId).Scan(&count); err != nil {
		return err
	}
	if limit > 0 && count >= limit {
		return ErrRunnerCapacityFull
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO runner_job_reservations(deployment_id,org_id,runner_id) VALUES($1,$2,$3)`, dep.Id, dep.OrgId, dep.RunnerId)
	return err
}

func (d *databaser) ReserveRunnerDispatch(ctx context.Context, dep *DeploymentSummary, limit int) error {
	return ReserveRunnerDispatch(ctx, d, dep, limit)
}

func ReserveRunnerDispatch(ctx context.Context, db Databaser, dep *DeploymentSummary, limit int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := ReserveRunnerJob(ctx, tx, dep, limit); err != nil {
		return err
	}
	return tx.Commit()
}

// ReleaseRunnerJob requires positive stop proof or proof that dispatch never
// began. A failed status and an exhausted dispatch budget prove neither.
func ReleaseRunnerJob(ctx context.Context, tx Tx, deploymentID uuid.UUID) error {
	_, err := tx.ExecContext(ctx, `UPDATE runner_job_reservations r SET released_at=COALESCE(released_at,now())
 WHERE deployment_id=$1 AND (EXISTS(SELECT 1 FROM deployment_dispatch_budgets b WHERE b.deployment_id=r.deployment_id AND (b.completion_received_at IS NOT NULL OR (b.first_attempt_at IS NULL AND NOT b.legacy)))
 OR EXISTS(SELECT 1 FROM runner_event_inbox i WHERE i.deployment_id=r.deployment_id AND i.event_type='deployment-result' AND i.org_id=r.org_id AND i.runner_id=r.runner_id)
 OR EXISTS(SELECT 1 FROM deployment_late_runner_results l WHERE l.deployment_id=r.deployment_id)
 OR EXISTS(SELECT 1 FROM deployment_outcome_acknowledgements a WHERE a.deployment_id=r.deployment_id))`, deploymentID)
	return err
}

// RecordLateOrdinaryRunnerCompletion proves stop for a capacity-owned ordinary
// attempt after watchdog failure. It retains no outputs and never repairs the
// failed Deployment or the Environment's state/adoption history.
func RecordLateOrdinaryRunnerCompletion(ctx context.Context, tx Tx, dep *DeploymentSummary, body any) (bool, error) {
	if dep.Mode == DeploymentModeObservation || dep.Status != DeploymentStatusFailed || !dep.CompletedAt.IsSet() {
		return false, nil
	}
	var external bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM external_operation_deployments WHERE deployment_id=$1)`, dep.Id).Scan(&external); err != nil {
		return false, err
	}
	if external {
		return false, nil
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return false, err
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	var previous string
	err = tx.QueryRowContext(ctx, `SELECT payload_sha256 FROM runner_job_completion_receipts WHERE deployment_id=$1 AND org_id=$2 AND runner_id=$3`, dep.Id, dep.OrgId, dep.RunnerId).Scan(&previous)
	if err == nil {
		if previous != digest {
			return true, NewErrConflict("late Runner completion conflicts with retained stop receipt")
		}
		return true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runner_job_reservations WHERE deployment_id=$1 AND org_id=$2 AND runner_id=$3 AND released_at IS NULL)`, dep.Id, dep.OrgId, dep.RunnerId).Scan(&active); err != nil {
		return false, err
	}
	if !active {
		return false, nil
	}
	budget, err := ReadDeploymentDispatchBudget(ctx, tx, dep.Id)
	if err != nil {
		return true, err
	}
	if !budget.FirstAttemptAt.Valid || budget.Revision+1 != dep.Revision {
		return true, NewErrConflict("late Runner completion does not match retained dispatch attempt")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runner_job_completion_receipts(deployment_id,org_id,runner_id,payload_sha256) VALUES($1,$2,$3,$4)`, dep.Id, dep.OrgId, dep.RunnerId, digest); err != nil {
		return true, err
	}
	return true, RecordDeploymentCompletion(ctx, tx, dep.Id)
}
