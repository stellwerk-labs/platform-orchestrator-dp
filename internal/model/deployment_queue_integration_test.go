//go:build integration

package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

type queueModelFixture struct {
	ctx   context.Context
	store Databaser
	env   uuid.UUID
}

func newQueueModelFixture(t *testing.T) queueModelFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	t.Cleanup(cancel)
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	_, _, scoped := migrationTestDatabase(t, ctx, connection)
	store, err := NewDatabaser(ctx, zap.NewNop(), scoped)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return queueModelFixture{ctx: ctx, store: store, env: uuid.New()}
}

func (f queueModelFixture) create(t *testing.T, mode DeploymentMode, configure func(*CreateDeploymentParams)) (*DeploymentSummary, error) {
	t.Helper()
	params := CreateDeploymentParams{DeploymentEnvUuid: f.env, Mode: mode, CreatedBy: uuid.New(), RunnerId: "runner", RunnerLogLevel: "info",
		Manifest: EncodedDeploymentManifest(`{"workloads":{}}`), Graph: EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`), Tofu: RawTofu("terraform {}")}
	if configure != nil {
		configure(&params)
	}
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	dep, err := f.store.CreateDeployment(f.ctx, tx, "queue-org", "p", "e", params)
	if err != nil {
		return nil, err
	}
	require.NoError(t, tx.Commit())
	return dep, nil
}

func (f queueModelFixture) enqueue(t *testing.T, configure func(*NewDeploymentQueueItem)) (*DeploymentQueueItem, bool, error) {
	t.Helper()
	request := NewDeploymentQueueItem{OrgID: "queue-org", ProjectID: "p", EnvID: "e", EnvironmentUUID: f.env, Mode: DeploymentModeDeploy,
		Intent: json.RawMessage(`{"manifest":{"workloads":{}}}`), CreatedBy: uuid.New(), TTL: time.Hour}
	if configure != nil {
		configure(&request)
	}
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	item, replayed, err := EnqueueDeployment(f.ctx, tx, request, 3)
	if err != nil {
		return nil, false, err
	}
	require.NoError(t, tx.Commit())
	return item, replayed, nil
}

func (f queueModelFixture) inTx(t *testing.T, fn func(TxWithCommit)) {
	t.Helper()
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	fn(tx)
	require.NoError(t, tx.Commit())
}

func requireConflictCode(t *testing.T, err error, code string) {
	t.Helper()
	conflict, ok := IsErrConflict(err)
	require.True(t, ok, "expected a conflict, got %v", err)
	require.Equal(t, code, conflict.Code, conflict.Message)
}

// Queued work fences every other mutating creation path under the Environment lock.
func TestQueuedDeploymentsFenceEveryOtherMutation(t *testing.T) {
	f := newQueueModelFixture(t)
	base, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	_, err = f.create(t, DeploymentModeDeploy, nil)
	require.ErrorIs(t, err, ErrEnvironmentBusy, "the lease is a coded busy conflict")
	requireConflictCode(t, err, CodeEnvironmentBusy)
	first, replayed, err := f.enqueue(t, func(r *NewDeploymentQueueItem) { r.IdempotencyKeyDigest = opt.Of("key") })
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, 1, first.Position)
	second, _, err := f.enqueue(t, nil)
	require.NoError(t, err)
	require.Equal(t, 2, second.Position)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, base.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded})
	require.NoError(t, err)

	f.inTx(t, func(tx TxWithCommit) {
		require.ErrorIs(t, CheckEnvironmentAvailable(f.ctx, tx, "queue-org", "p", "e", f.env, uuid.Nil), ErrEnvironmentBusy)
		require.ErrorIs(t, CheckEnvironmentAvailable(f.ctx, tx, "queue-org", "p", "e", f.env, second.ID), ErrEnvironmentBusy)
		require.NoError(t, CheckEnvironmentAvailable(f.ctx, tx, "queue-org", "p", "e", f.env, first.ID))
	})
	for _, mode := range []DeploymentMode{DeploymentModeDeploy, DeploymentModeRollback, DeploymentModeDestroy} {
		_, err = f.create(t, mode, func(p *CreateDeploymentParams) {
			if mode == DeploymentModeRollback {
				p.RollbackToId = opt.Of(base.Id)
			}
		})
		require.ErrorIs(t, err, ErrEnvironmentBusy, "%s yields to queued work", mode)
	}
	_, err = f.create(t, DeploymentModeDeploy, func(p *CreateDeploymentParams) { p.QueueItemID = opt.Of(second.ID) })
	require.ErrorIs(t, err, ErrEnvironmentBusy, "only the head may pass")
	_, err = f.create(t, DeploymentModeDeploy, func(p *CreateDeploymentParams) {
		p.QueueItemID = opt.Of(first.ID)
		p.ExpectedBaseDeploymentID = opt.Of(uuid.New())
	})
	requireConflictCode(t, err, CodeDeploymentBaseChanged)

	readiness, err := f.store.GetEnvironmentDeploymentReadiness(f.ctx, nil, "queue-org", []EnvironmentRef{{ProjectID: "p", EnvID: "e"}})
	require.NoError(t, err)
	require.True(t, readiness[0].Queued)

	promoted, err := f.create(t, DeploymentModeDeploy, func(p *CreateDeploymentParams) {
		p.QueueItemID = opt.Of(first.ID)
		p.ExpectedBaseDeploymentID = opt.Of(base.Id)
	})
	require.NoError(t, err)
	f.inTx(t, func(tx TxWithCommit) {
		require.NoError(t, SettleDeploymentQueueItem(f.ctx, tx, first.ID, DeploymentQueueDispatched, DeploymentQueueSettlement{DeploymentID: opt.Of(promoted.Id)}))
		requireConflictCode(t, SettleDeploymentQueueItem(f.ctx, tx, first.ID, DeploymentQueueCancelled, DeploymentQueueSettlement{ReasonCode: QueueReasonCancelled}), "")
	})
	replay, replayed, err := f.enqueue(t, func(r *NewDeploymentQueueItem) { r.IdempotencyKeyDigest = opt.Of("key") })
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, first.ID, replay.ID)
	require.Equal(t, DeploymentQueueDispatched, replay.State)
	_, _, err = f.enqueue(t, func(r *NewDeploymentQueueItem) {
		r.IdempotencyKeyDigest = opt.Of("key")
		r.Intent = json.RawMessage(`{"manifest":{"workloads":{"other":{}}}}`)
	})
	requireConflictCode(t, err, "")
}

func TestDeploymentQueueDepthListingCancellationExpiryAndRetention(t *testing.T) {
	f := newQueueModelFixture(t)
	_, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	actor := uuid.New()
	items := make([]*DeploymentQueueItem, 3)
	for index := range items {
		items[index], _, err = f.enqueue(t, nil)
		require.NoError(t, err)
	}
	_, _, err = f.enqueue(t, nil)
	requireConflictCode(t, err, CodeDeploymentQueueFull)
	_, _, err = f.enqueue(t, func(r *NewDeploymentQueueItem) { r.Mode = DeploymentModeDeployPlan })
	_, isBadRequest := IsErrBadRequest(err)
	require.True(t, isBadRequest, "plans never wait in the queue")

	f.inTx(t, func(tx TxWithCommit) {
		page, next, err := ListDeploymentQueueItems(f.ctx, tx, "queue-org", "", 2, ListDeploymentQueueParams{EnvID: opt.Of("e")})
		require.NoError(t, err)
		require.Len(t, page, 2)
		require.NotEmpty(t, next)
		rest, next, err := ListDeploymentQueueItems(f.ctx, tx, "queue-org", next, 2, ListDeploymentQueueParams{})
		require.NoError(t, err)
		require.Empty(t, next)
		require.Equal(t, []uuid.UUID{items[0].ID, items[1].ID, items[2].ID}, []uuid.UUID{page[0].ID, page[1].ID, rest[0].ID})
		require.Equal(t, []int{1, 2, 3}, []int{page[0].Position, page[1].Position, rest[0].Position})
		_, _, err = ListDeploymentQueueItems(f.ctx, tx, "queue-org", "not-a-token", 2, ListDeploymentQueueParams{})
		_, isBadRequest := IsErrBadRequest(err)
		require.True(t, isBadRequest)
	})

	// A promoter holding the head makes cancellation fail fast instead of waiting.
	holder, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	claimed, err := ClaimDeploymentQueueHead(f.ctx, holder, f.env)
	require.NoError(t, err)
	require.Equal(t, items[0].ID, claimed.ID)
	contender, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	again, err := ClaimDeploymentQueueHead(f.ctx, contender, f.env)
	require.NoError(t, err)
	require.Nil(t, again, "a second promoter skips the claimed head instead of jumping the queue")
	_, _, err = CancelDeploymentQueueItem(f.ctx, contender, "queue-org", items[0].ID, actor, "")
	requireConflictCode(t, err, CodeQueueItemDispatching)
	require.NoError(t, contender.Rollback())
	require.NoError(t, holder.Rollback())

	f.inTx(t, func(tx TxWithCommit) {
		cancelled, replayed, err := CancelDeploymentQueueItem(f.ctx, tx, "queue-org", items[0].ID, actor, "obsolete")
		require.NoError(t, err)
		require.False(t, replayed)
		require.Equal(t, DeploymentQueueCancelled, cancelled.State)
		require.Equal(t, opt.Of(actor), cancelled.SettledBy)
		require.Zero(t, cancelled.Position)
	})
	f.inTx(t, func(tx TxWithCommit) {
		_, replayed, err := CancelDeploymentQueueItem(f.ctx, tx, "queue-org", items[0].ID, actor, "obsolete")
		require.NoError(t, err)
		require.True(t, replayed)
		_, _, err = CancelDeploymentQueueItem(f.ctx, tx, "queue-org", items[0].ID, uuid.New(), "obsolete")
		requireConflictCode(t, err, CodeQueueItemSettled)
		_, _, err = CancelDeploymentQueueItem(f.ctx, tx, "other-org", items[1].ID, actor, "")
		_, missing := IsErrNotFound(err)
		require.True(t, missing, "items are scoped to their organisation")
	})

	f.inTx(t, func(tx TxWithCommit) {
		_, err := tx.ExecContext(f.ctx, `UPDATE environment_deployment_queue SET created_at=now()-interval '2 hours', expires_at=now()-interval '1 second' WHERE id=$1`, items[2].ID)
		require.NoError(t, err)
	})
	holder, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = holder.ExecContext(f.ctx, `SELECT 1 FROM environment_deployment_queue WHERE id=$1 FOR UPDATE`, items[2].ID)
	require.NoError(t, err)
	f.inTx(t, func(tx TxWithCommit) {
		changed, err := ExpireDeploymentQueueItems(f.ctx, tx, 10)
		require.NoError(t, err)
		require.Empty(t, changed, "expiry skips an item a promoter holds")
	})
	require.NoError(t, holder.Rollback())
	f.inTx(t, func(tx TxWithCommit) {
		changed, err := ExpireDeploymentQueueItems(f.ctx, tx, 10)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{f.env}, changed)
		due, err := DueDeploymentQueueEnvironments(f.ctx, tx, 10)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{f.env}, due, "the remaining head is due")
		require.NoError(t, DeferDeploymentQueueItem(f.ctx, tx, items[1].ID, "Core unavailable", time.Hour))
		due, err = DueDeploymentQueueEnvironments(f.ctx, tx, 10)
		require.NoError(t, err)
		require.Empty(t, due, "a head in backoff waits for its retry time")
		pruned, err := PruneDeploymentQueue(f.ctx, tx, time.Now().Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, int64(2), pruned, "only settled items are pruned")
		_, err = SettleQueuedDeployments(f.ctx, tx, f.env, DeploymentQueueRejected, DeploymentQueueSettlement{ReasonCode: CodeOutcomeUnreconciled, DeploymentID: opt.Of(uuid.New())})
		require.Error(t, err, "no single Deployment is what every waiting item became")
		count, err := SettleQueuedDeployments(f.ctx, tx, f.env, DeploymentQueueCancelled, DeploymentQueueSettlement{ReasonCode: CodeEnvironmentDeleting, ReasonMessage: "deleting"})
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
		cancelled, err := GetDeploymentQueueItem(f.ctx, tx, "queue-org", items[1].ID)
		require.NoError(t, err)
		require.Equal(t, DeploymentQueueCancelled, cancelled.State)
		require.Equal(t, opt.Of(CodeEnvironmentDeleting), cancelled.ReasonCode)
		require.Equal(t, opt.Of("deleting"), cancelled.ReasonMessage)
		require.False(t, cancelled.ReasonDeploymentID.IsSet(), "deletion refers to no Deployment")
	})
	f.inTx(t, func(tx TxWithCommit) {
		require.NoError(t, f.store.DeleteDeploymentsForEnv(f.ctx, tx, "queue-org", "p", "e", true))
		var remaining int
		require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM environment_deployment_queue`).Scan(&remaining))
		require.Zero(t, remaining, "deleting the Environment deletes its queue history")
	})
}

func TestDeploymentQueueMigrationIsAdditiveAndDownRefusesQueuedRows(t *testing.T) {
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
	_, err = provider.UpTo(ctx, 32)
	require.NoError(t, err)
	env, dep := uuid.New(), uuid.New()
	seed, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = seed.ExecContext(ctx, `INSERT INTO deployment_environments(id,org_id,project_id,env_id,last_executing_deployment_id,last_executing_deployment_at,last_state_deployment_id,last_state_deployment_at) VALUES($1,'legacy','p','e',$2,now(),$2,now())`, env, dep)
	require.NoError(t, err)
	_, err = seed.ExecContext(ctx, `INSERT INTO deployments(id,de_id,mode,status,status_message,manifest,graph,tofu,runner_id,runner_log_level,created_by,created_at) VALUES($1,$2,'deploy','succeeded','done','{}','{}','terraform {}','runner','info',$3,now())`, dep, env, uuid.New())
	require.NoError(t, err)
	require.NoError(t, seed.Commit())
	_, err = provider.UpTo(ctx, 33)
	require.NoError(t, err)
	var existing int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM deployments`).Scan(&existing))
	require.Equal(t, 1, existing, "the migration only adds")

	item := uuid.New()
	_, err = db.ExecContext(ctx, `INSERT INTO environment_deployment_queue (id, org_id, project_id, env_id, environment_uuid, mode, intent, intent_sha256, created_by, expires_at)
		VALUES ($1, 'legacy', 'p', 'e', $2, 'deploy', '{}', 'digest', $3, now() + interval '1 hour')`, item, env, uuid.New())
	require.NoError(t, err)
	for _, invalid := range []string{
		`UPDATE environment_deployment_queue SET state='dispatched', settled_at=now() WHERE id=$1`,
		`UPDATE environment_deployment_queue SET state='cancelled', settled_at=now() WHERE id=$1`,
		`UPDATE environment_deployment_queue SET state='queued', deployment_id='` + dep.String() + `' WHERE id=$1`,
		`UPDATE environment_deployment_queue SET mode='destroy' WHERE id=$1`,
	} {
		_, err = db.ExecContext(ctx, invalid, item)
		require.Error(t, err, invalid)
	}
	_, err = provider.DownTo(ctx, 32)
	require.ErrorContains(t, err, "cannot discard queued Deployment requests")
	_, err = db.ExecContext(ctx, `UPDATE environment_deployment_queue SET state='dispatched', deployment_id=$2, settled_at=now() WHERE id=$1`, item, dep)
	require.NoError(t, err)
	_, err = provider.DownTo(ctx, 32)
	require.NoError(t, err)
	var present bool
	require.NoError(t, db.QueryRowContext(ctx, `SELECT to_regclass('environment_deployment_queue') IS NOT NULL`).Scan(&present))
	require.False(t, present)
	_, err = provider.Up(ctx)
	require.NoError(t, err)
}

func TestDeploymentQueueReasonDeploymentMigrationIsAdditive(t *testing.T) {
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
	_, err = provider.UpTo(ctx, 34)
	require.NoError(t, err)
	env, waiting, halted := uuid.New(), uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{waiting, halted} {
		_, err = db.ExecContext(ctx, `INSERT INTO environment_deployment_queue (id, org_id, project_id, env_id, environment_uuid, mode, intent, intent_sha256, created_by, expires_at, halt_on_failure)
			VALUES ($1, 'legacy', 'p', 'e', $2, 'deploy', '{}', 'digest', $3, now() + interval '1 hour', true)`, id, env, uuid.New())
		require.NoError(t, err)
	}
	_, err = db.ExecContext(ctx, `UPDATE environment_deployment_queue SET state='rejected', reason_code='previous_deployment_failed',
		reason_message='the previous Deployment failed', settled_at=now() WHERE id=$1`, halted)
	require.NoError(t, err)

	_, err = provider.UpTo(ctx, 35)
	require.NoError(t, err)
	var reasonDeployment uuid.NullUUID
	require.NoError(t, db.QueryRowContext(ctx, `SELECT reason_deployment_id FROM environment_deployment_queue WHERE id=$1`, halted).Scan(&reasonDeployment))
	require.False(t, reasonDeployment.Valid, "items settled before the upgrade keep only their reason message")
	_, err = db.ExecContext(ctx, `UPDATE environment_deployment_queue SET reason_deployment_id=$2 WHERE id=$1`, waiting, uuid.New())
	require.Error(t, err, "only an item with a reason refers to a Deployment")
	_, err = db.ExecContext(ctx, `UPDATE environment_deployment_queue SET reason_deployment_id=$2 WHERE id=$1`, halted, uuid.New())
	require.NoError(t, err)

	_, err = provider.DownTo(ctx, 34)
	require.NoError(t, err, "only settled items carry the column, so a waiting item does not block the Down migration")
	var columns int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'environment_deployment_queue' AND column_name = 'reason_deployment_id'`).Scan(&columns))
	require.Zero(t, columns)
	for id, want := range map[uuid.UUID]string{waiting: "queued", halted: "rejected"} {
		var state string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM environment_deployment_queue WHERE id=$1`, id).Scan(&state))
		require.Equal(t, want, state, "the Down migration keeps every item")
	}
	_, err = provider.Up(ctx)
	require.NoError(t, err)
}
