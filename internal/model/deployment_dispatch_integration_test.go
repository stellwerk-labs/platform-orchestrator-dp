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
	"go.uber.org/zap"
)

func TestDispatchBudgetStartsBeforeEffectAndNeverRefreshes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	_, _, scoped := migrationTestDatabase(t, ctx, connection)
	store, err := NewDatabaser(ctx, zap.NewNop(), scoped)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	dep, err := store.CreateDeployment(ctx, tx, "budget-org", "project", "env", CreateDeploymentParams{
		DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy, CreatedBy: uuid.New(), RunnerId: "runner", RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}")})
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE deployments SET created_at=now()-interval '8 hours' WHERE id=$1`, dep.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	dep, _, _, _, err = store.GetDeployment(ctx, nil, "budget-org", dep.Id, GetModeDefault)
	require.NoError(t, err)
	deadline, err := BeginDeploymentDispatch(ctx, store, dep)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(time.Hour), deadline, 2*time.Second)
	require.True(t, time.Now().After(dep.CreatedAt.Add(time.Hour)), "old admission timeout would already have failed this new first dispatch")
	require.NoError(t, store.Close())
	store, err = NewDatabaser(ctx, zap.NewNop(), scoped)
	require.NoError(t, err)
	again, err := BeginDeploymentDispatch(ctx, store, dep)
	require.NoError(t, err)
	require.Equal(t, deadline, again, "restart/redelivery cannot extend first-attempt budget")
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now()-interval '2 hours',deadline=now()-interval '1 hour' WHERE deployment_id=$1`, dep.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = BeginDeploymentDispatch(ctx, store, dep)
	require.ErrorContains(t, err, "exhausted")
}

func TestUnknownFailedDispatchBlocksReplacementAfterLeaseExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	_, _, scoped := migrationTestDatabase(t, ctx, connection)
	store, err := NewDatabaser(ctx, zap.NewNop(), scoped)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	params := CreateDeploymentParams{DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy, CreatedBy: uuid.New(), RunnerId: "runner", RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}")}
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	dep, err := store.CreateDeployment(ctx, tx, "unknown-org", "p", "e", params)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = BeginDeploymentDispatch(ctx, store, dep)
	require.NoError(t, err)
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, dep.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusFailed, StatusMessage: "watchdog"})
	require.NoError(t, err)
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE environment_deployment_leases SET expires_at=now()-interval '1 second' WHERE deployment_id=$1`, dep.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = store.CreateDeployment(ctx, tx, "unknown-org", "p", "e", params)
	require.ErrorContains(t, err, "unreconciled")
	require.NoError(t, tx.Rollback())
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	unknown, err := DeploymentOutcomeUnreconciled(ctx, tx, dep.Id)
	require.NoError(t, err)
	require.True(t, unknown)
	var total int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT count(*) FROM deployments`).Scan(&total))
	require.Equal(t, 1, total)
	require.NoError(t, tx.Rollback())
}

func TestDispatchEvidenceMigrationConservativeRestartAndGuardedDown(t *testing.T) {
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
	_, err = provider.UpTo(ctx, 30)
	require.NoError(t, err)
	env, dep := uuid.New(), uuid.New()
	seed, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = seed.Rollback() }()
	_, err = seed.ExecContext(ctx, `INSERT INTO deployment_environments(id,org_id,project_id,env_id,last_executing_deployment_id,last_executing_deployment_at,last_state_deployment_id,last_state_deployment_at) VALUES($1,'legacy','p','e',$2,now(),$2,now());`, env, dep)
	require.NoError(t, err)
	_, err = seed.ExecContext(
		ctx,
		`INSERT INTO deployments(id,de_id,mode,status,status_message,manifest,graph,tofu,runner_id,runner_log_level,created_by,created_at) VALUES($1,$2,'deploy','executing','running','{}','{}','terraform {}','runner','info',$3,now()-interval '8 hours')`,
		dep,
		env,
		uuid.New(),
	)
	require.NoError(t, err)
	require.NoError(t, seed.Commit())
	var legacyCreated time.Time
	require.NoError(t, db.QueryRowContext(ctx, `SELECT created_at FROM deployments WHERE id=$1`, dep).Scan(&legacyCreated))
	_, err = db.ExecContext(ctx, `SET TIME ZONE 'Europe/Berlin'`)
	require.NoError(t, err)
	_, err = provider.Up(ctx)
	require.NoError(t, err)
	first, err := ReadDeploymentDispatchBudget(ctx, db, dep)
	require.NoError(t, err)
	require.True(t, first.Legacy)
	require.True(t, legacyCreated.Equal(first.FirstAttemptAt.Time), "legacy UTC admission must not shift with the migrator session timezone")
	require.True(t, first.Deadline.Time.Before(time.Now()))
	_, err = provider.Up(ctx)
	require.NoError(t, err)
	second, err := ReadDeploymentDispatchBudget(ctx, db, dep)
	require.NoError(t, err)
	require.Equal(t, first, second)
	_, err = provider.Down(ctx)
	require.NoError(t, err, "empty new evidence allows schema30 fallback")
	_, err = provider.Up(ctx)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET legacy=false WHERE deployment_id=$1`, dep)
	require.NoError(t, err)
	_, err = provider.Down(ctx)
	require.ErrorContains(t, err, "cannot discard durable Runner")
}
