package integrationtests

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"strings"
	"testing"

	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModuleVersionArtifactMigrationRoundTrip(t *testing.T) {
	database := MustDatabaseConn(t)
	tx, err := database.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tx.Rollback()) })

	migration, err := os.ReadFile("../internal/model/migrations/000015_module_version_artifacts.sql")
	require.NoError(t, err)
	up, down := moduleMigrationSections(t, string(migration))

	_, err = tx.ExecContext(t.Context(), down)
	require.NoError(t, err)
	assert.False(t, moduleColumnExists(t, tx, "deployments", "module_artifacts"))

	_, err = tx.ExecContext(t.Context(), up)
	require.NoError(t, err)
	assert.True(t, moduleColumnExists(t, tx, "deployments", "module_artifacts"))
}

func TestModuleVersionRequestDigestMigrationPreservesPreviousDeployment(t *testing.T) {
	cp, dp := MustControlPlaneClient(t), MustDataPlaneClient(t)
	orgID := MustCreateOrgId(t, MustInternalControlPlaneClient(t))
	project := MustCreateProject(t, cp, orgID, "request-migration")
	envType := MustCreateEnvType(t, cp, orgID, "development")
	MustCreateRunnerWithRule(t, cp, orgID, "migration-runner", "", "", nil)
	environment := MustCreateEnv(t, cp, orgID, envType.Id, project.Id, "previous")
	created, err := dp.CreateDeploymentWithResponse(t.Context(), orgID, &serverclient.CreateDeploymentParams{}, serverclient.DeploymentCreateBody{
		ProjectId: project.Id, EnvId: environment.Id, Mode: serverclient.DeploymentCreateBodyModeDeploy,
		Manifest: &serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{}},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, created.StatusCode(), string(created.Body))
	completed := MustWaitForDeploymentComplete(t, dp, orgID, created.JSON201.Id)
	require.Equal(t, "succeeded", completed.Status)
	database := MustDatabaseConn(t)
	tx, err := database.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback()) }()
	migration, err := os.ReadFile("../internal/model/migrations/000016_module_version_request_digest.sql")
	require.NoError(t, err)
	up, down := moduleMigrationSections(t, string(migration))
	_, err = tx.ExecContext(t.Context(), down)
	require.NoError(t, err)
	require.False(t, moduleColumnExists(t, tx, "deployments", "module_version_request_digest"))
	var before []byte
	require.NoError(t, tx.QueryRowContext(t.Context(), `SELECT manifest FROM deployments WHERE id=$1`, completed.Id).Scan(&before))
	_, err = tx.ExecContext(t.Context(), up)
	require.NoError(t, err)
	var after []byte
	var digest string
	require.NoError(t, tx.QueryRowContext(t.Context(), `SELECT manifest,module_version_request_digest FROM deployments WHERE id=$1`, completed.Id).Scan(&after, &digest))
	require.JSONEq(t, string(before), string(after))
	require.Empty(t, digest, "old requests without Module Version intent retain their original idempotency identity")
	_, err = tx.ExecContext(t.Context(), `UPDATE deployments SET module_version_request_digest=$2 WHERE id=$1`, completed.Id, strings.Repeat("a", 64))
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), down)
	require.NoError(t, err)
	var status string
	require.NoError(t, tx.QueryRowContext(t.Context(), `SELECT status,manifest FROM deployments WHERE id=$1`, completed.Id).Scan(&status, &after))
	require.Equal(t, "succeeded", status)
	require.JSONEq(t, string(before), string(after), "schema recovery must preserve the previous deployment")
}

func moduleMigrationSections(t *testing.T, migration string) (string, string) {
	t.Helper()
	const upMarker = "-- +goose Up"
	const downMarker = "-- +goose Down"
	parts := strings.Split(migration, downMarker)
	require.Len(t, parts, 2)
	return strings.TrimSpace(strings.TrimPrefix(parts[0], upMarker)), strings.TrimSpace(parts[1])
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
		WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
	`, table, column).Scan(&count))
	return count == 1
}
