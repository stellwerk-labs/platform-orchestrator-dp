//go:build integration

package model

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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
		requireTableAbsent(t, ctx, admin, schema, "environment_deployment_leases")
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
