//go:build integration

package model

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMigrationHistoryReconciliation(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "MODEL_TEST_DB_CONNECTION_STRING must name an isolated local PostgreSQL test database")

	t.Run("fresh database installs only generic external operation schema", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
		defer cancel()
		admin, schema, scopedConnection := migrationTestDatabase(t, ctx, connection)

		store, err := NewDatabaser(ctx, zap.NewNop(), scopedConnection)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		requireColumn(t, ctx, admin, schema, "deployments", "module_version_request_digest")
		requireTableAbsent(t, ctx, admin, schema, "rollout_encryption_material")
		requireTableAbsent(t, ctx, admin, schema, "rollout_deployment_bases")
		requireTablePresent(t, ctx, admin, schema, "environment_deployment_leases")
		requireTablePresent(t, ctx, admin, schema, "external_deployment_plan_snapshots")
		requireColumn(t, ctx, admin, schema, "external_deployment_plan_snapshots", "explicit_targets")
		requireTablePresent(t, ctx, admin, schema, "external_operation_material")
		requireColumn(t, ctx, admin, schema, "external_operation_deployments", "plan_snapshot_id")
		requireColumn(t, ctx, admin, schema, "external_operation_deployments", "base_deployment_id")
		requireTablePresent(t, ctx, admin, schema, "deployment_admission_receipts")
		requireColumn(t, ctx, admin, schema, "deployment_admission_receipts", "carry_forward_module_versions")
		requireColumn(t, ctx, admin, schema, "deployment_admission_receipts", "confirmed_restricted_version_uuids")
	})

	t.Run("exercised version 28 receives additive lifecycle evidence", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
		defer cancel()
		admin, schema, scopedConnection := migrationTestDatabase(t, ctx, connection)
		legacy, err := sql.Open("postgres", scopedConnection)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, legacy.Close()) })

		source, err := fs.Sub(embedMigrations, "migrations")
		require.NoError(t, err)
		provider, err := goose.NewProvider(goose.DialectPostgres, legacy, source,
			goose.WithDisableGlobalRegistry(true),
			goose.WithGoMigrations(goose.NewGoMigration(2,
				&goose.GoFunc{RunTx: hstandardoutbox.MigrateUp01},
				&goose.GoFunc{RunTx: hstandardoutbox.MigrateDown01})))
		require.NoError(t, err)
		_, err = provider.UpTo(ctx, 28)
		require.NoError(t, err)
		requireColumnAbsent(t, ctx, admin, schema, "deployment_admission_receipts", "carry_forward_module_versions")
		requireColumnAbsent(t, ctx, admin, schema, "deployment_admission_receipts", "confirmed_restricted_version_uuids")

		_, err = legacy.ExecContext(ctx, `INSERT INTO deployment_admission_receipts
			(org_id,admission_id,project_uuid,environment_uuid,actor_id,request_digest,mode,module_versions,core_epoch,disposition,deployment_id,terminal_outcome)
			VALUES ('migration-repair',$1,$2,$3,$4,$5,'deploy','{}'::jsonb,1,'committed',$6,'succeeded')`,
			uuid.New(), uuid.New(), uuid.New(), uuid.New(), strings.Repeat("a", 64), uuid.New())
		require.NoError(t, err)

		_, err = provider.Up(ctx)
		require.NoError(t, err)
		requireColumn(t, ctx, admin, schema, "deployment_admission_receipts", "carry_forward_module_versions")
		requireColumn(t, ctx, admin, schema, "deployment_admission_receipts", "confirmed_restricted_version_uuids")
		var carryForward, confirmed string
		require.NoError(t, legacy.QueryRowContext(ctx,
			`SELECT carry_forward_module_versions::text,confirmed_restricted_version_uuids::text FROM deployment_admission_receipts`,
		).Scan(&carryForward, &confirmed))
		require.Equal(t, "{}", carryForward)
		require.Equal(t, "[]", confirmed)
	})
}

func migrationTestDatabase(t *testing.T, ctx context.Context, connection string) (*sql.DB, string, string) {
	t.Helper()
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "migration_history_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	return admin, schema, connection + " search_path=" + schema
}

func requireColumn(t *testing.T, ctx context.Context, db *sql.DB, schema, table, column string) {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema=$1 AND table_name=$2 AND column_name=$3
		)
	`, schema, table, column).Scan(&exists))
	require.True(t, exists, "%s.%s.%s must exist", schema, table, column)
}

func requireColumnAbsent(t *testing.T, ctx context.Context, db *sql.DB, schema, table, column string) {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema=$1 AND table_name=$2 AND column_name=$3
		)
	`, schema, table, column).Scan(&exists))
	require.False(t, exists, "%s.%s.%s must be absent", schema, table, column)
}

func requireTableAbsent(t *testing.T, ctx context.Context, db *sql.DB, schema, table string) {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema=$1 AND table_name=$2
		)
	`, schema, table).Scan(&exists))
	require.False(t, exists, "%s.%s must not exist", schema, table)
}

func requireTablePresent(t *testing.T, ctx context.Context, db *sql.DB, schema, table string) {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema=$1 AND table_name=$2
		)
	`, schema, table).Scan(&exists))
	require.True(t, exists, "%s.%s must exist", schema, table)
}
