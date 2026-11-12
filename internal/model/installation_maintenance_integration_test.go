//go:build integration

package model

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceDrainsDurableQueueAndRejectsEveryNewDeploymentMode(t *testing.T) {
	f := newQueueModelFixture(t)
	running, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)

	item, _, err := f.enqueue(t, func(n *NewDeploymentQueueItem) { n.IdempotencyKeyDigest = opt.Of("accepted-key") })
	require.NoError(t, err)
	state, err := UpdateInstallationMaintenance(f.ctx, f.store, uuid.New(), 1, true)
	require.NoError(t, err)
	require.True(t, state.Enabled)
	require.EqualValues(t, 1, state.Queued)
	_, _, err = f.enqueue(t, nil)
	require.ErrorIs(t, err, ErrSystemMaintenance)
	replay, replayed, err := f.enqueue(t, func(n *NewDeploymentQueueItem) {
		n.IdempotencyKeyDigest = opt.Of("accepted-key")
		n.CreatedBy = item.CreatedBy
	})
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, item.ID, replay.ID)
	for _, mode := range []DeploymentMode{DeploymentModeDeploy, DeploymentModeRollback, DeploymentModeDeployPlan, DeploymentModeRollbackPlan, DeploymentModeDestroy, DeploymentModeObservation} {
		_, err := f.create(t, mode, nil)
		require.ErrorIs(t, err, ErrSystemMaintenance, string(mode))
	}
	_, err = f.create(t, DeploymentModeDeploy, func(p *CreateDeploymentParams) { p.QueueItemID = opt.Of(uuid.New()) })
	require.ErrorIs(t, err, ErrSystemMaintenance)
	f.inTx(t, func(tx TxWithCommit) {
		_, err := f.store.UpdateDeploymentStatusAndOutputs(f.ctx, tx, running.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
		require.NoError(t, err, "Runner completion must remain available in maintenance")
	})
	_, err = f.create(t, DeploymentModeDeploy, func(p *CreateDeploymentParams) { p.QueueItemID = opt.Of(item.ID); p.CreatedBy = item.CreatedBy })
	require.NoError(t, err, "accepted queue promotion must drain")
	_, err = UpdateInstallationMaintenance(f.ctx, f.store, uuid.New(), 1, false)
	require.ErrorIs(t, err, ErrSettingsRevision)
	_, err = UpdateInstallationMaintenance(f.ctx, f.store, uuid.New(), 2, false)
	require.NoError(t, err)
}

func TestMaintenanceEnableWaitsForEarlierAcceptanceAndFencesLaterAcceptance(t *testing.T) {
	f := newQueueModelFixture(t)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, requireDeploymentAcceptance(f.ctx, tx, uuid.Nil, "queue-org", "p", "e", f.env, uuid.New(), DeploymentModeDeploy))
	result := make(chan error, 1)
	go func() { _, err := UpdateInstallationMaintenance(f.ctx, f.store, uuid.New(), 1, true); result <- err }()
	select {
	case err := <-result:
		t.Fatalf("maintenance crossed an uncommitted admission: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, tx.Commit())
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("maintenance toggle remained blocked")
	}
	_, _, err = f.enqueue(t, nil)
	require.ErrorIs(t, err, ErrSystemMaintenance)
}

func TestMaintenanceCountsRunnerWorkAfterWatchdogFailure(t *testing.T) {
	f := newQueueModelFixture(t)
	running, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	f.inTx(t, func(tx TxWithCommit) {
		_, err := tx.ExecContext(f.ctx, `INSERT INTO runner_job_reservations(deployment_id,org_id,runner_id) SELECT d.id,e.org_id,d.runner_id FROM deployments d JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1 ON CONFLICT DO NOTHING`, running.Id)
		require.NoError(t, err)
		_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET status='failed' WHERE id=$1`, running.Id)
		require.NoError(t, err)
	})
	state, err := UpdateInstallationMaintenance(f.ctx, f.store, uuid.New(), 1, true)
	require.NoError(t, err)
	require.Zero(t, state.Executing)
	require.EqualValues(t, 1, state.Runners, "a watchdog failure is not Runner stop evidence")
	f.inTx(t, func(tx TxWithCommit) {
		_, err := tx.ExecContext(f.ctx, `UPDATE runner_job_reservations SET released_at=clock_timestamp() WHERE deployment_id=$1`, running.Id)
		require.NoError(t, err)
	})
	state, err = ReadInstallationMaintenance(f.ctx, f.store)
	require.NoError(t, err)
	require.Zero(t, state.Runners)
}
