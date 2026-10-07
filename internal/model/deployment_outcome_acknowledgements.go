package model

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// DeploymentOutcomeAcknowledgement is the audit record of an operator who
// accepted responsibility that the Runner attempt of one failed
// plugin-dispatched Deployment can no longer change its Environment.
type DeploymentOutcomeAcknowledgement struct {
	DeploymentID   uuid.UUID
	OrgID          string
	EnvironmentID  uuid.UUID
	PluginID       string
	OperationRef   string
	ActorID        uuid.UUID
	Reason         string
	AcknowledgedAt time.Time
}

// AcknowledgeDeploymentOutcome records the acknowledgement exactly once. A
// repeated identical request returns the stored record with replayed=true.
// The caller must authorize actorID and commit tx.
func AcknowledgeDeploymentOutcome(ctx context.Context, tx Tx, orgID string, deploymentID, actorID uuid.UUID, reason string, now time.Time) (*DeploymentOutcomeAcknowledgement, bool, error) {
	if tx == nil {
		return nil, false, errors.New("transaction is required")
	}
	var environmentID uuid.UUID
	if err := tx.QueryRowContext(ctx, `SELECT d.de_id FROM deployments d JOIN deployment_environments e ON e.id=d.de_id
        WHERE e.org_id=$1 AND d.id=$2`, orgID, deploymentID).Scan(&environmentID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, NewErrNotFound(fmt.Sprintf("deployment '%s' not found", deploymentID))
		}
		return nil, false, errors.Wrap(err, "failed to read deployment for outcome acknowledgement")
	}
	// The same lock serializes replacement admission and late Runner results.
	if err := LockEnvironmentMutation(ctx, tx, environmentID); err != nil {
		return nil, false, err
	}

	existing := &DeploymentOutcomeAcknowledgement{DeploymentID: deploymentID, OrgID: orgID}
	err := tx.QueryRowContext(ctx, `SELECT environment_id, plugin_id, operation_ref, actor_id, reason, acknowledged_at
        FROM deployment_outcome_acknowledgements WHERE deployment_id=$1 AND org_id=$2`, deploymentID, orgID).Scan(
		&existing.EnvironmentID, &existing.PluginID, &existing.OperationRef, &existing.ActorID, &existing.Reason, &existing.AcknowledgedAt)
	if err == nil {
		if existing.ActorID != actorID || existing.Reason != reason {
			return nil, false, NewErrConflict("deployment outcome was already acknowledged by a different request")
		}
		return existing, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, errors.Wrap(err, "failed to read deployment outcome acknowledgement")
	}

	ack := &DeploymentOutcomeAcknowledgement{DeploymentID: deploymentID, OrgID: orgID, EnvironmentID: environmentID, ActorID: actorID, Reason: reason}
	if err := tx.QueryRowContext(ctx, `SELECT plugin_id, operation_ref FROM external_operation_deployments
        WHERE deployment_id=$1 AND org_id=$2`, deploymentID, orgID).Scan(&ack.PluginID, &ack.OperationRef); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, false, errors.Wrap(err, "failed to read plugin operation receipt")
	}
	var head uuid.NullUUID
	var completed bool
	if err := tx.QueryRowContext(ctx, `SELECT e.last_state_deployment_id, d.completed_at IS NOT NULL
        FROM deployments d JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1`, deploymentID).Scan(&head, &completed); err != nil {
		return nil, false, errors.Wrap(err, "failed to read environment head")
	}
	evidence, err := ReadDeploymentStopEvidence(ctx, tx, deploymentID)
	if err != nil {
		return nil, false, err
	}
	budget, err := ReadDeploymentDispatchBudget(ctx, tx, deploymentID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	if err := evidence.acknowledgementConflict(completed, head.Valid && head.UUID == deploymentID, budget.Deadline, now); err != nil {
		return nil, false, err
	}

	if err := tx.QueryRowContext(ctx, `INSERT INTO deployment_outcome_acknowledgements
        (deployment_id, org_id, environment_id, plugin_id, operation_ref, actor_id, reason)
        VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING acknowledged_at`,
		ack.DeploymentID, ack.OrgID, ack.EnvironmentID, ack.PluginID, ack.OperationRef, ack.ActorID, ack.Reason,
	).Scan(&ack.AcknowledgedAt); err != nil {
		return nil, false, errors.Wrap(err, "failed to record deployment outcome acknowledgement")
	}
	// Requests that waited behind the fenced Deployment never start by themselves
	// once an operator acknowledges it; the operator deploys again on purpose.
	if _, err := SettleQueuedDeployments(ctx, tx, environmentID, DeploymentQueueRejected, CodeOutcomeUnreconciled,
		fmt.Sprintf("plugin-dispatched Deployment '%s' failed with an unreconciled outcome", deploymentID)); err != nil {
		return nil, false, err
	}
	return ack, false, nil
}

// acknowledgementConflict limits acknowledgements to the one Deployment that
// currently fences its Environment, after its Runner dispatch window closed.
func (e DeploymentStopEvidence) acknowledgementConflict(completed, head bool, deadline sql.NullTime, now time.Time) error {
	switch {
	case !e.PluginDispatched:
		return NewErrConflict("only a plugin-dispatched Deployment can block its Environment; there is nothing to acknowledge")
	case !e.Failed || !completed:
		return NewErrConflict("only a terminal failed Deployment can have its outcome acknowledged")
	case !head:
		return NewErrConflict("deployment is no longer the Environment head and blocks nothing")
	case !e.OutcomeUnreconciled():
		return NewErrConflict("deployment already has Runner stop evidence; there is nothing to acknowledge")
	case deadline.Valid && now.Before(deadline.Time):
		return NewErrConflict(fmt.Sprintf("the Runner dispatch window is open until %s", deadline.Time.UTC().Format(time.RFC3339)))
	}
	return nil
}
