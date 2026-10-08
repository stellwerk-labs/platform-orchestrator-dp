//go:build integration

package model

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewDatabaserRefusesASchemaNewerThanItsMigrations(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "MODEL_TEST_DB_CONNECTION_STRING must name an isolated local PostgreSQL test database")
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()

	_, _, scopedConnection := migrationTestDatabase(t, ctx, connection)
	db := schemaConnection(t, scopedConnection)
	requireStarts(t, ctx, scopedConnection)
	applied := appliedMigrations(t, ctx, db)
	require.NotEmpty(t, applied, "a fresh database migrates")
	require.Contains(t, schemaTables(t, ctx, db), "deployments")
	latest := applied[len(applied)-1]
	require.Greater(t, latest, int64(len(applied)), "version numbers have unused gaps, so the latest version exceeds the number of migrations")
	newer := fmt.Sprintf("database schema version %d is newer than this binary's latest migration %d", latest+1, latest)

	t.Run("a database this binary migrated starts again", func(t *testing.T) {
		requireStarts(t, ctx, scopedConnection)
		require.Equal(t, applied, appliedMigrations(t, ctx, db))
	})

	t.Run("a database a newer release migrated is refused", func(t *testing.T) {
		_, _, scopedConnection := migrationTestDatabase(t, ctx, connection)
		db := schemaConnection(t, scopedConnection)
		requireStarts(t, ctx, scopedConnection)
		recordAppliedMigration(t, ctx, db, latest+1)

		_, err := NewDatabaser(ctx, zap.NewNop(), scopedConnection)
		require.EqualError(t, err, newer)
		require.Equal(t, append(slices.Clone(applied), latest+1), appliedMigrations(t, ctx, db), "a refused start changes nothing")

		_, err = db.ExecContext(ctx, `DELETE FROM goose_db_version WHERE version_id = $1`, latest+1)
		require.NoError(t, err)
		requireStarts(t, ctx, scopedConnection)
	})

	t.Run("the refusal comes before any migration", func(t *testing.T) {
		_, _, scopedConnection := migrationTestDatabase(t, ctx, connection)
		db := schemaConnection(t, scopedConnection)
		createMigrationVersionTable(t, ctx, db)
		recordAppliedMigration(t, ctx, db, latest+1)

		_, err := NewDatabaser(ctx, zap.NewNop(), scopedConnection)
		require.EqualError(t, err, newer)
		require.Equal(t, []string{"goose_db_version"}, schemaTables(t, ctx, db))
		require.Equal(t, []int64{latest + 1}, appliedMigrations(t, ctx, db))
	})

	t.Run("a version table without applied migrations migrates", func(t *testing.T) {
		_, _, scopedConnection := migrationTestDatabase(t, ctx, connection)
		db := schemaConnection(t, scopedConnection)
		createMigrationVersionTable(t, ctx, db)
		requireStarts(t, ctx, scopedConnection)
		require.Equal(t, applied, appliedMigrations(t, ctx, db))
	})
}

func requireStarts(t *testing.T, ctx context.Context, connection string) {
	t.Helper()
	store, err := NewDatabaser(ctx, zap.NewNop(), connection)
	require.NoError(t, err)
	require.NoError(t, store.Close())
}

// schemaConnection opens a connection whose search_path is the test's schema.
func schemaConnection(t *testing.T, connection string) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// appliedMigrations lists the versions in goose's table, without the version 0
// row goose writes when it creates the table.
func appliedMigrations(t *testing.T, ctx context.Context, db *sql.DB) []int64 {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT version_id FROM goose_db_version WHERE is_applied AND version_id > 0 ORDER BY version_id`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var versions []int64
	for rows.Next() {
		var version int64
		require.NoError(t, rows.Scan(&version))
		versions = append(versions, version)
	}
	require.NoError(t, rows.Err())
	return versions
}

func recordAppliedMigration(t *testing.T, ctx context.Context, db *sql.DB, version int64) {
	t.Helper()
	_, err := db.ExecContext(ctx, `INSERT INTO goose_db_version (version_id, is_applied) VALUES ($1, true)`, version)
	require.NoError(t, err)
}

// createMigrationVersionTable creates goose's version table as goose does
// before its first migration: with the version 0 row only.
func createMigrationVersionTable(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	_, err := goose.EnsureDBVersionContext(ctx, db)
	require.NoError(t, err)
}

func schemaTables(t *testing.T, ctx context.Context, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() ORDER BY table_name`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var tables []string
	for rows.Next() {
		var table string
		require.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	require.NoError(t, rows.Err())
	return tables
}
