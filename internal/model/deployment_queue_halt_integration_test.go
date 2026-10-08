//go:build integration

package model

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func (f queueModelFixture) end(t *testing.T, deploymentID uuid.UUID, status DeploymentStatus) {
	t.Helper()
	_, err := f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, deploymentID, UpdateDeploymentStatusAndOutputsParams{Status: status})
	require.NoError(t, err)
}

func (f queueModelFixture) haltingPredecessor(t *testing.T, item *DeploymentQueueItem) opt.Opt[uuid.UUID] {
	t.Helper()
	var predecessor opt.Opt[uuid.UUID]
	f.inTx(t, func(tx TxWithCommit) {
		current, err := GetDeploymentQueueItem(f.ctx, tx, "queue-org", item.ID)
		require.NoError(t, err)
		predecessor, err = HaltingDeploymentQueuePredecessor(f.ctx, tx, current)
		require.NoError(t, err)
	})
	return predecessor
}

func (f queueModelFixture) pending(t *testing.T) PendingEnvironmentChanges {
	t.Helper()
	pending, err := f.store.ReadPendingEnvironmentChanges(f.ctx, nil, "queue-org", "p", "e", f.env)
	require.NoError(t, err)
	return pending
}

func TestHaltingDeploymentQueuePredecessorIsTheFailureTheItemWaitedFor(t *testing.T) {
	f := newQueueModelFixture(t)
	halt := func(r *NewDeploymentQueueItem) { r.HaltOnFailure = true }
	require.Equal(t, PendingEnvironmentChanges{}, f.pending(t))

	// On a fresh Environment a plan run is recorded as its state head; it is never a predecessor.
	plan, err := f.create(t, DeploymentModeDeployPlan, nil)
	require.NoError(t, err)
	behindPlan, _, err := f.enqueue(t, halt)
	require.NoError(t, err)
	require.False(t, behindPlan.EnqueuedAfterDeploymentID.IsSet(), "the plan was still running")
	require.Equal(t, PendingEnvironmentChanges{Queued: 1}, f.pending(t), "a plan run changes nothing")
	f.end(t, plan.Id, DeploymentStatusFailed)
	require.False(t, f.haltingPredecessor(t, behindPlan).IsSet())
	f.inTx(t, func(tx TxWithCommit) {
		_, err := SettleQueuedDeployments(f.ctx, tx, f.env, DeploymentQueueCancelled, DeploymentQueueSettlement{ReasonCode: QueueReasonCancelled})
		require.NoError(t, err)
	})

	running, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	waiting, _, err := f.enqueue(t, halt)
	require.NoError(t, err)
	plain, _, err := f.enqueue(t, nil)
	require.NoError(t, err)
	require.False(t, waiting.EnqueuedAfterDeploymentID.IsSet(), "it waits for the running Deployment")
	require.False(t, plain.HaltOnFailure)
	require.Equal(t, PendingEnvironmentChanges{Running: 1, Queued: 2}, f.pending(t))
	f.end(t, running.Id, DeploymentStatusFailed)
	require.Equal(t, opt.Of(running.Id), f.haltingPredecessor(t, waiting))
	require.False(t, f.haltingPredecessor(t, plain).IsSet(), "only halt_on_failure items halt")
	require.Equal(t, PendingEnvironmentChanges{Queued: 2}, f.pending(t))

	// Enqueued after the failure ended: the item did not wait for it.
	late, _, err := f.enqueue(t, halt)
	require.NoError(t, err)
	require.Equal(t, opt.Of(running.Id), late.EnqueuedAfterDeploymentID)
	require.False(t, f.haltingPredecessor(t, late).IsSet())

	// A later Deployment supersedes the failure for everyone, then fails itself for everyone.
	f.inTx(t, func(tx TxWithCommit) {
		require.NoError(t, SettleDeploymentQueueItem(f.ctx, tx, waiting.ID, DeploymentQueueRejected, DeploymentQueueSettlement{ReasonCode: QueueReasonPreviousDeploymentFailed}))
	})
	next, err := f.create(t, DeploymentModeDeploy, func(p *CreateDeploymentParams) { p.QueueItemID = opt.Of(plain.ID) })
	require.NoError(t, err)
	f.inTx(t, func(tx TxWithCommit) {
		require.NoError(t, SettleDeploymentQueueItem(f.ctx, tx, plain.ID, DeploymentQueueDispatched, DeploymentQueueSettlement{DeploymentID: opt.Of(next.Id)}))
	})
	f.end(t, next.Id, DeploymentStatusSucceeded)
	require.False(t, f.haltingPredecessor(t, waiting).IsSet())
	require.False(t, f.haltingPredecessor(t, late).IsSet())
	again, err := f.create(t, DeploymentModeRollback, func(p *CreateDeploymentParams) {
		p.QueueItemID = opt.Of(late.ID)
		p.RollbackToId = opt.Of(next.Id)
	})
	require.NoError(t, err)
	f.end(t, again.Id, DeploymentStatusFailed)
	require.Equal(t, opt.Of(again.Id), f.haltingPredecessor(t, late), "a rollback is a predecessor like any mutation")
}

func TestDeploymentQueueHaltMigrationIsAdditiveAndDownRefusesQueuedHaltRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	_, _, scoped := migrationTestDatabase(t, ctx, connection)
	db, err := sql.Open("postgres", scoped)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer func() { require.NoError(t, db.Close()) }()
	source, err := fs.Sub(embedMigrations, "migrations")
	require.NoError(t, err)
	provider, err := goose.NewProvider(goose.DialectPostgres, db, source, goose.WithDisableGlobalRegistry(true), goose.WithGoMigrations(goose.NewGoMigration(2, &goose.GoFunc{RunTx: hstandardoutbox.MigrateUp01}, &goose.GoFunc{RunTx: hstandardoutbox.MigrateDown01})))
	require.NoError(t, err)
	_, err = provider.UpTo(ctx, 33)
	require.NoError(t, err)
	env, legacy := uuid.New(), uuid.New()
	insert := `INSERT INTO environment_deployment_queue (id, org_id, project_id, env_id, environment_uuid, mode, intent, intent_sha256, created_by, expires_at)
		VALUES ($1, 'legacy', 'p', 'e', $2, 'deploy', '{}', 'digest', $3, now() + interval '1 hour')`
	_, err = db.ExecContext(ctx, insert, legacy, env, uuid.New())
	require.NoError(t, err)

	_, err = provider.UpTo(ctx, 34)
	require.NoError(t, err)
	var halt bool
	var after uuid.NullUUID
	var state string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT halt_on_failure, enqueued_after_deployment_id, state FROM environment_deployment_queue WHERE id=$1`, legacy).Scan(&halt, &after, &state))
	require.False(t, halt, "waiting requests keep going after a failure, as they did")
	require.False(t, after.Valid)
	require.Equal(t, "queued", state)
	_, err = db.ExecContext(ctx, `UPDATE environment_deployment_queue SET enqueued_after_deployment_id=$2 WHERE id=$1`, legacy, uuid.New())
	require.Error(t, err, "only halt_on_failure items record what they did not wait for")

	halted := uuid.New()
	_, err = db.ExecContext(ctx, `INSERT INTO environment_deployment_queue (id, org_id, project_id, env_id, environment_uuid, mode, intent, intent_sha256, created_by, expires_at, halt_on_failure, enqueued_after_deployment_id)
		VALUES ($1, 'legacy', 'p', 'e', $2, 'deploy', '{}', 'digest', $3, now() + interval '1 hour', true, $4)`, halted, env, uuid.New(), uuid.New())
	require.NoError(t, err)
	_, err = provider.DownTo(ctx, 33)
	require.ErrorContains(t, err, "cannot discard halt_on_failure of queued Deployment requests")
	_, err = db.ExecContext(ctx, `UPDATE environment_deployment_queue SET state='cancelled', reason_code='cancelled', settled_at=now() WHERE id=$1`, halted)
	require.NoError(t, err)
	_, err = provider.DownTo(ctx, 33)
	require.NoError(t, err, "settled halt_on_failure items and plain waiting items do not block the Down migration")
	var columns int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'environment_deployment_queue'
		  AND column_name IN ('halt_on_failure', 'enqueued_after_deployment_id')`).Scan(&columns))
	require.Zero(t, columns)
	var rows int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM environment_deployment_queue`).Scan(&rows))
	require.Equal(t, 2, rows, "the Down migration keeps every item")
	_, err = provider.Up(ctx)
	require.NoError(t, err)
}
