package model

import (
	"context"

	"github.com/google/uuid"
)

// TryRecordRunnerEvent stores a runner event's idempotency key in the caller's
// transaction. The boolean is false when the same event was already applied.
// Recording and applying the event in one transaction closes the ACK-after-DB
// crash window inherent in at-least-once broker delivery.
func (d *databaser) TryRecordRunnerEvent(
	ctx context.Context,
	optionalTx Tx,
	eventID string,
	orgID string,
	runnerID string,
	deploymentID uuid.UUID,
	eventType string,
) (bool, error) {
	result, err := d.txOrDb(optionalTx).ExecContext(ctx, `
INSERT INTO runner_event_inbox (event_id, org_id, runner_id, deployment_id, event_type)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (event_id) DO NOTHING
`, eventID, orgID, runnerID, deploymentID, eventType)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}
