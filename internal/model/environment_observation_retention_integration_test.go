//go:build integration

package model

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/require"
)

func TestObservationResultCleanupIsBoundedAndPreservesFutureAndLiveRows(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	active, err := f.create(t, DeploymentModeObservation, nil)
	require.NoError(t, err)
	actor, grant := uuid.New(), uuid.New()
	result, err := json.Marshal(observation.ObservationResult{SchemaVersion: "1", Complete: true, Tool: "opentofu", ToolVersion: "1.10.0", ObservedChanges: []observation.ObservationChange{}, ProposedChanges: []observation.ObservationChange{}, OutputChangeCount: 0})
	require.NoError(t, err)
	const overflow = 3
	expiredCount := ObservationResultCleanupBatchSize + overflow
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(f.ctx, `WITH identities AS (SELECT gen_random_uuid() request_id,gen_random_uuid() run_id FROM generate_series(1,$1))
 INSERT INTO environment_observation_receipts(org_id,plugin_id,request_id,request_sha256,environment_uuid,actor_id,grant_id,grant_revision,receipt,result,result_expires_at)
 SELECT 'queue-org','dev.example.plugin',request_id,repeat('a',64),$2,$3,$4,1,
 jsonb_build_object('request_id',request_id,'disposition','accepted','reason','accepted','recorded_at',now(),'run_id',run_id),$5,now()-interval '1 minute' FROM identities`, expiredCount, f.env, actor, grant, result)
	require.NoError(t, err)
	futureID, liveID := uuid.New(), uuid.New()
	for _, item := range []struct {
		id         uuid.UUID
		deployment any
		expiry     time.Time
	}{
		{futureID, nil, time.Now().Add(time.Hour)},
		{liveID, active.Id, time.Now().Add(-time.Hour)},
	} {
		runID := uuid.New()
		if item.deployment != nil {
			runID = active.Id
		}
		receipt, err := json.Marshal(observation.ObservationReceipt{RequestId: item.id, Disposition: observation.ObservationReceiptDispositionAccepted, Reason: observation.ObservationReceiptReasonAccepted, RecordedAt: time.Now(), RunId: &runID})
		require.NoError(t, err)
		_, err = tx.ExecContext(
			f.ctx,
			`INSERT INTO environment_observation_receipts(org_id,plugin_id,request_id,request_sha256,environment_uuid,actor_id,grant_id,grant_revision,receipt,deployment_id,result,result_expires_at) VALUES('queue-org','dev.example.plugin',$1,repeat('a',64),$2,$3,$4,1,$5,$6,$7,$8)`,
			item.id,
			f.env,
			actor,
			grant,
			receipt,
			item.deployment,
			result,
			item.expiry,
		)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	count := func(predicate string) int {
		return readObservationCount(t, f, `SELECT count(*) FROM environment_observation_receipts WHERE `+predicate)
	}
	require.Equal(t, expiredCount+2, count("result IS NOT NULL"))
	deleted, err := DeleteExpiredObservationResults(f.ctx, f.store, ObservationResultCleanupBatchSize)
	require.NoError(t, err)
	require.EqualValues(t, ObservationResultCleanupBatchSize, deleted)
	require.Equal(t, overflow+2, count("result IS NOT NULL"), "one pass performs at most the explicit result batch")
	require.Equal(t, expiredCount+2, count("true"), "history receipts are never removed")
	deleted, err = DeleteExpiredObservationResults(f.ctx, f.store, ObservationResultCleanupBatchSize)
	require.NoError(t, err)
	require.EqualValues(t, overflow, deleted)
	require.Equal(t, 2, count("result IS NOT NULL"))
	require.Equal(t, expiredCount+2, count("true"))
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var preserved int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM environment_observation_receipts WHERE request_id IN ($1,$2) AND result IS NOT NULL`, futureID, liveID).Scan(&preserved))
	require.Equal(t, 2, preserved)
	current, _, _, _, err := f.store.GetDeployment(f.ctx, tx, "queue-org", active.Id, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, DeploymentStatusExecuting, current.Status)
}
