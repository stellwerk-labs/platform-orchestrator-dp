package model

import (
	"context"
	"database/sql"
)

const (
	ObservationResultCleanupBatchSize    = 500
	ObservationResultCleanupMaxBatchSize = 2000
)

// DeleteExpiredObservationResults clears at most one bounded payload batch and
// commits it independently. Compact receipts and live references remain intact.
func DeleteExpiredObservationResults(ctx context.Context, db Databaser, batchSize int) (int64, error) {
	if batchSize < 1 || batchSize > ObservationResultCleanupMaxBatchSize {
		return 0, NewErrBadRequest("observation result cleanup batch must be between 1 and 2000")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
WITH expired AS (
 SELECT receipt.org_id,receipt.plugin_id,receipt.request_id
 FROM environment_observation_receipts receipt
 WHERE receipt.result_expires_at<=now() AND receipt.result IS NOT NULL
   AND NOT EXISTS(SELECT 1 FROM deployments live WHERE live.id=receipt.deployment_id AND live.completed_at IS NULL)
 ORDER BY receipt.result_expires_at
 LIMIT $1 FOR UPDATE OF receipt SKIP LOCKED
)
UPDATE environment_observation_receipts receipt SET result=NULL FROM expired
WHERE receipt.org_id=expired.org_id AND receipt.plugin_id=expired.plugin_id AND receipt.request_id=expired.request_id`, batchSize)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}
