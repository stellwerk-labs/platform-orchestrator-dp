package integrationtests

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModuleVersionArtifactMigrationRoundTrip(t *testing.T) {
	database := moduleMigrationDatabaseAt(t, 15)
	require.Equal(t, int64(15), moduleMigrationVersion(t, database))
	require.NoError(t, goose.DownToContext(t.Context(), database, "migrations", 14))
	require.Equal(t, int64(14), moduleMigrationVersion(t, database))
	assert.False(t, moduleColumnExists(t, database, "deployments", "module_artifacts"))
	require.NoError(t, goose.UpToContext(t.Context(), database, "migrations", 15))
	require.Equal(t, int64(15), moduleMigrationVersion(t, database))
	assert.True(t, moduleColumnExists(t, database, "deployments", "module_artifacts"))
}

func TestModuleVersionRequestDigestMigrationPreservesPreviousDeployment(t *testing.T) {
	// A completed pre-versioning deployment is represented in the real version-16
	// schema. Its manifest and status must survive both directions of migration 16.
	const orgID, projectID, envID = "migration-history", "request-migration", "previous"
	deploymentID, deID := uuid.New(), uuid.New()
	before := []byte(`{"workloads":{}}`)
	database := moduleMigrationDatabaseAt(t, 16)
	require.Equal(t, int64(16), moduleMigrationVersion(t, database))
	tx, err := database.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.ExecContext(t.Context(), `INSERT INTO deployment_environments
		(id,org_id,project_id,env_id,last_executing_deployment_id,last_executing_deployment_at,last_state_deployment_id,last_state_deployment_at)
		VALUES ($1,$2,$3,$4,$5,now(),$5,now())`, deID, orgID, projectID, envID, deploymentID)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), `INSERT INTO deployments
		(id,de_id,created_at,mode,completed_at,status,status_message,manifest,graph,tofu,runner_id)
		VALUES ($1,$2,now(),'deploy',now(),'succeeded','',$3,'{}','terraform {}','fixture-runner')`, deploymentID, deID, before)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, goose.DownToContext(t.Context(), database, "migrations", 15))
	require.Equal(t, int64(15), moduleMigrationVersion(t, database))
	require.False(t, moduleColumnExists(t, database, "deployments", "module_version_request_digest"))
	var after []byte
	var status string
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT status,manifest FROM deployments WHERE id=$1`, deploymentID).Scan(&status, &after))
	require.Equal(t, "succeeded", status)
	require.JSONEq(t, string(before), string(after))
	require.NoError(t, goose.UpToContext(t.Context(), database, "migrations", 16))
	require.Equal(t, int64(16), moduleMigrationVersion(t, database))
	var digest string
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT manifest,module_version_request_digest FROM deployments WHERE id=$1`, deploymentID).Scan(&after, &digest))
	require.JSONEq(t, string(before), string(after))
	require.Empty(t, digest, "old requests without Module Version intent retain their original idempotency identity")
	_, err = database.ExecContext(t.Context(), `UPDATE deployments SET module_version_request_digest=$2 WHERE id=$1`, deploymentID, strings.Repeat("a", 64))
	require.NoError(t, err)
	require.NoError(t, goose.DownToContext(t.Context(), database, "migrations", 15))
	require.Equal(t, int64(15), moduleMigrationVersion(t, database))
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT status,manifest FROM deployments WHERE id=$1`, deploymentID).Scan(&status, &after))
	require.Equal(t, "succeeded", status)
	require.JSONEq(t, string(before), string(after), "schema recovery must preserve the previous deployment")
}

func moduleMigrationDatabaseAt(t *testing.T, version int64) *sql.DB {
	t.Helper()
	MustDatabaser(t) // Register the embedded SQL and Go migrations used by the running DP.
	connection := os.Getenv("DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "module_migration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(t.Context(), `CREATE SCHEMA `+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		require.NoError(t, err)
	})
	database, err := sql.Open("postgres", connection+" search_path="+schema)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	require.NoError(t, goose.UpToContext(t.Context(), database, "migrations", version))
	return database
}

func moduleMigrationVersion(t *testing.T, database *sql.DB) int64 {
	t.Helper()
	version, err := goose.GetDBVersionContext(t.Context(), database)
	require.NoError(t, err)
	return version
}

type moduleQueryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func moduleColumnExists(t *testing.T, database moduleQueryRower, table, column string) bool {
	t.Helper()
	var count int
	require.NoError(t, database.QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
	`, table, column).Scan(&count))
	return count == 1
}
