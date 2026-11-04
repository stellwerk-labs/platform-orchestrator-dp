//go:build integration

package api

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestObservationResultCleanerDrainsDefaultCadenceAndPreservesOrdinaryOutputs(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	db := externalDeploymentTestDatabase(t, ctx, connection)
	envID, actor, grant := uuid.New(), uuid.New(), uuid.New()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	dep, err := db.CreateDeployment(
		ctx,
		tx,
		"retention-worker",
		"p",
		"e",
		model.CreateDeploymentParams{
			DeploymentEnvUuid: envID,
			Mode:              model.DeploymentModeDeploy,
			CreatedBy:         actor,
			RunnerId:          "runner",
			RunnerLogLevel:    "info",
			Manifest:          model.EncodedDeploymentManifest(`{"workloads":{}}`),
			Graph:             model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`),
			Tofu:              model.RawTofu("terraform {}"),
		},
	)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE deployments SET status='succeeded',completed_at=now()-interval '10 minutes',outputs=$2 WHERE id=$1`, dep.Id, []byte("ordinary-cleanup-canary"))
	require.NoError(t, err)
	const staged = 1500
	_, err = tx.ExecContext(ctx, `WITH identities AS (SELECT gen_random_uuid() request_id,gen_random_uuid() run_id FROM generate_series(1,$1))
 INSERT INTO environment_observation_receipts(org_id,plugin_id,request_id,request_sha256,environment_uuid,actor_id,grant_id,grant_revision,receipt,result,result_expires_at)
 SELECT 'retention-worker','dev.example.plugin',request_id,repeat('a',64),$2,$3,$4,1,
 jsonb_build_object('request_id',request_id,'disposition','accepted','reason','accepted','recorded_at',now(),'run_id',run_id),
 '{"schema_version":"1","complete":true,"tool":"opentofu","tool_version":"1.10.0","observed_changes":[],"proposed_changes":[],"output_change_count":0}'::jsonb,now()-interval '1 minute' FROM identities`, staged, envID, actor, grant)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	done := ScheduleObservationResultCleaning(ctx, 5*time.Second, model.ObservationResultCleanupBatchSize, zap.NewNop(), db)
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("observation cleaner did not stop after cancellation")
		}
	}()
	started := time.Now()
	require.Eventually(t, func() bool {
		current, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = current.Rollback() }()
		var count int
		require.NoError(t, current.QueryRowContext(ctx, `SELECT count(*) FROM environment_observation_receipts WHERE result IS NOT NULL`).Scan(&count))
		return count == 0
	}, 23*time.Second, 100*time.Millisecond)
	t.Logf("actual PostgreSQL default cadence cleared %d expired payloads in %s", staged, time.Since(started))
	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var outputs string
	var receipts int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT outputs FROM deployments WHERE id=$1`, dep.Id).Scan(&outputs))
	require.Equal(t, "ordinary-cleanup-canary", outputs, "the dedicated result loop must not invoke ordinary output cleanup")
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT count(*) FROM environment_observation_receipts`).Scan(&receipts))
	require.Equal(t, staged, receipts)
}
