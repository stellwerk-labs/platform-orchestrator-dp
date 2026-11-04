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

func TestObservationMigrationPreservesUnknownOwnershipAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	_, _, scoped := migrationTestDatabase(t, ctx, connection)
	legacy, err := sql.Open("postgres", scoped)
	require.NoError(t, err)
	defer func() { _ = legacy.Close() }()
	source, err := fs.Sub(embedMigrations, "migrations")
	require.NoError(t, err)
	provider, err := goose.NewProvider(goose.DialectPostgres, legacy, source, goose.WithDisableGlobalRegistry(true), goose.WithGoMigrations(goose.NewGoMigration(2, &goose.GoFunc{RunTx: hstandardoutbox.MigrateUp01}, &goose.GoFunc{RunTx: hstandardoutbox.MigrateDown01})))
	require.NoError(t, err)
	_, err = provider.UpTo(ctx, 37)
	require.NoError(t, err)
	env, dep := uuid.New(), uuid.New()
	tx, err := legacy.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO deployment_environments(id,org_id,project_id,env_id,last_executing_deployment_id,last_executing_deployment_at,last_state_deployment_id,last_state_deployment_at) VALUES($1,'migration-org','p','e',$2,now(),$2,now())`, env, dep)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO deployments(id,de_id,created_at,completed_at,created_by,mode,status,status_message,manifest,graph,tofu,runner_id) VALUES($1,$2,now(),now(),$3,'deploy','failed','timeout',$4,$4,$5,'runner')`, dep, env, uuid.New(), []byte(`{}`), []byte("terraform {}"))
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO deployment_dispatch_budgets(deployment_id,deployment_revision,first_attempt_at,deadline,legacy) VALUES($1,1,now()-interval '2 hours',now()-interval '1 hour',true)`, dep)
	require.NoError(t, err)
	legacySuccess, successEnv := uuid.New(), uuid.New()
	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO deployment_environments(id,org_id,project_id,env_id,last_executing_deployment_id,last_executing_deployment_at,last_state_deployment_id,last_state_deployment_at) VALUES($1,'migration-org','p','legacy-success',$2,now(),$2,now())`,
		successEnv,
		legacySuccess,
	)
	require.NoError(t, err)
	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO deployments(id,de_id,created_at,completed_at,created_by,mode,status,status_message,manifest,graph,tofu,runner_id) VALUES($1,$2,now(),now(),$3,'deploy','succeeded','authenticated legacy success',$4,$4,$5,'runner')`,
		legacySuccess,
		successEnv,
		uuid.New(),
		[]byte(`{}`),
		[]byte("terraform {}"),
	)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO deployment_dispatch_budgets(deployment_id,deployment_revision,first_attempt_at,deadline,legacy) VALUES($1,1,now()-interval '2 hours',now()-interval '1 hour',true)`, legacySuccess)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = provider.Up(ctx)
	require.NoError(t, err)
	for range 2 {
		store, err := NewDatabaser(ctx, zap.NewNop(), scoped)
		require.NoError(t, err)
		require.NoError(t, store.Close())
		var count int
		require.NoError(t, legacy.QueryRowContext(ctx, `SELECT count(*) FROM runner_job_reservations WHERE deployment_id=$1 AND released_at IS NULL`, dep).Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, legacy.QueryRowContext(ctx, `SELECT count(*) FROM runner_job_reservations WHERE deployment_id=$1`, legacySuccess).Scan(&count))
		require.Zero(t, count, "legacy authenticated success cannot consume a persistent slot solely because old completion markers are absent")

	}
	_, err = provider.DownTo(ctx, 37)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot discard observation receipts or live Runner ownership")
	// A stopped legacy attempt can be released only by the authenticated operator
	// attestation above it, never because migration/restart saw an old deadline.
	tx, err = legacy.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, _, err = AcknowledgeDeploymentOutcome(ctx, tx, "migration-org", dep, uuid.New(), "authoritatively deleted legacy job", time.Now())
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	var released bool
	require.NoError(t, legacy.QueryRowContext(ctx, `SELECT released_at IS NOT NULL FROM runner_job_reservations WHERE deployment_id=$1`, dep).Scan(&released))
	require.True(t, released)
}
