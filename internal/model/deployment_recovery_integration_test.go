//go:build integration

package model

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestCapturedBaseRecoveryRejectsConcurrentHeadChangeUnderNormalLease(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	admin, _, scoped := migrationTestDatabase(t, ctx, connection)
	store, err := NewDatabaser(ctx, zap.NewNop(), scoped)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	const org, project, environment = "recovery-org", "recovery-project", "recovery-env"
	environmentID := uuid.New()
	params := CreateDeploymentParams{
		CreatedBy: uuid.New(), DeploymentEnvUuid: environmentID, Mode: DeploymentModeDeploy,
		Manifest: EncodedDeploymentManifest(`{"workloads":{}}`), Graph: EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`),
		Tofu: RawTofu("terraform {}"), RunnerId: "unused", RunnerLogLevel: "info",
	}
	create := func(status DeploymentStatus) *DeploymentSummary {
		tx, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		deployment, err := store.CreateDeployment(ctx, tx, org, project, environment, params)
		require.NoError(t, err)
		_, err = store.UpdateDeploymentStatusAndOutputs(ctx, tx, deployment.Id, UpdateDeploymentStatusAndOutputsParams{Status: status})
		require.NoError(t, err)
		if status == DeploymentStatusFailed {
			inserted, err := store.TryRecordRunnerEvent(ctx, tx, uuid.NewString(), org, params.RunnerId, deployment.Id, "deployment-result")
			require.NoError(t, err)
			require.True(t, inserted)
		}
		require.NoError(t, tx.Commit())
		return deployment
	}
	base := create(DeploymentStatusSucceeded)
	forward := create(DeploymentStatusFailed)
	recovery := params
	recovery.Mode = DeploymentModeRollback
	recovery.RollbackToId = opt.Of(base.Id)
	recovery.ExpectedCurrentDeploymentID = opt.Of(forward.Id)

	// A different ordinary command owns the normal lease and changes the head
	// after rollback planning but before the rollback's atomic creation check.
	competitor, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = competitor.Rollback() }()
	newHead, err := store.CreateDeployment(ctx, competitor, org, project, environment, params)
	require.NoError(t, err)
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, competitor, newHead.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded})
	require.NoError(t, err)
	waiter, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = waiter.Rollback() }()
	var pid int
	require.NoError(t, waiter.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid))
	result := make(chan error, 1)
	go func() {
		_, err := store.CreateDeployment(ctx, waiter, org, project, environment, recovery)
		result <- err
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := admin.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')", pid).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, competitor.Commit())
	select {
	case err := <-result:
		_, conflict := IsErrConflict(err)
		require.True(t, conflict, "concurrent successful head must reject captured-base recovery: %v", err)
		require.Contains(t, err.Error(), "no longer the current head")
	case <-ctx.Done():
		t.Fatal("recovery did not finish after the real lease waiter was released")
	}
	require.NoError(t, waiter.Rollback())
	latest, _, _, _, err := store.GetLastDeployment(ctx, nil, org, project, environment, GetLastDeploymentParams{StateChangeOnly: true})
	require.NoError(t, err)
	require.Equal(t, newHead.Id, latest.Id)
	check, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = check.Rollback() }()
	var rollbacks, leases int
	require.NoError(t, check.QueryRowContext(ctx, "SELECT count(*) FROM deployments WHERE mode='rollback'").Scan(&rollbacks))
	require.NoError(t, check.QueryRowContext(ctx, "SELECT count(*) FROM environment_deployment_leases").Scan(&leases))
	require.Zero(t, rollbacks)
	require.Zero(t, leases, "rejected recovery must roll back its lease acquisition")
}
