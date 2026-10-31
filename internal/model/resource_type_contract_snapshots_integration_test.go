//go:build integration

package model

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestResourceTypeContractSnapshotMigrationValidatesWithoutInventingMetadata(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	_, _, scoped := migrationTestDatabase(t, t.Context(), connection)
	requireStarts(t, t.Context(), scoped)
	db := schemaConnection(t, scoped)
	for _, input := range []struct {
		value string
		valid bool
	}{
		{`null`, true}, {`[]`, true}, {`[{"module_id":"legacy"}]`, true},
		{`[{"resource_type_version":2,"resource_type_version_uuid":"01234567-89ab-4cde-8f01-23456789abcd","output_schema":{}}]`, true},
		{`[{"output_schema":null}]`, true},
		{`[{"resource_type_version":2}]`, false},
		{`[{"resource_type_version_uuid":"01234567-89ab-4cde-8f01-23456789abcd"}]`, false},
		{`[{"resource_type_version":0,"resource_type_version_uuid":"01234567-89ab-4cde-8f01-23456789abcd"}]`, false},
		{`[{"resource_type_version":2,"resource_type_version_uuid":"00000000-0000-0000-0000-000000000000"}]`, false},
		{`[{"resource_type_version":2,"resource_type_version_uuid":"invalid"}]`, false},
		{`[{"output_schema":[]}]`, false},
	} {
		var valid bool
		require.NoError(t, db.QueryRowContext(t.Context(), `SELECT valid_resource_type_contract_snapshots($1::jsonb)`, input.value).Scan(&valid))
		require.Equal(t, input.valid, valid, input.value)
	}
	require.NoError(t, goose.DownToContext(t.Context(), db, "migrations", 35))
	requireStarts(t, t.Context(), scoped)
}

func TestResourceTypeContractSnapshotMigrationRetainsExecutionHistory(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	for _, kind := range []string{"deployment", "plan"} {
		t.Run(kind, func(t *testing.T) {
			_, _, scoped := migrationTestDatabase(t, t.Context(), connection)
			store, err := NewDatabaser(t.Context(), zap.NewNop(), scoped)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			db := schemaConnection(t, scoped)
			contract, number := uuid.New(), int64(2)
			output := map[string]any{}
			artifacts := []ModuleArtifactRequirement{{ModuleID: "storage", Version: "1.2.0", Source: "inline",
				ResourceTypeVersion: &number, ResourceTypeVersionUUID: &contract, OutputSchema: &output}}
			tx, err := store.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			if kind == "deployment" {
				_, err = store.CreateDeployment(t.Context(), tx, "org", "project", "development", CreateDeploymentParams{
					CreatedBy: uuid.New(), DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy,
					Manifest: EncodedDeploymentManifest(`{"workloads":{}}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}"),
					RunnerId: "unused-runner", RunnerLogLevel: "info", ModuleArtifacts: artifacts})
			} else {
				_, err = CreateOrGetExternalDeploymentPlanSnapshot(t.Context(), tx, ExternalDeploymentPlanSnapshot{
					ID: uuid.New(), OrgID: "org", PluginID: "dev.example.workflow", RequestID: "contract-plan", ActorID: uuid.New(),
					RequestBodySHA256: strings.Repeat("a", 64), ProjectID: "project", ProjectUUID: uuid.New(), EnvID: "development",
					EnvironmentUUID: uuid.New(), BaseDeploymentID: uuid.New(), Fingerprint: "sha256:" + strings.Repeat("b", 64),
					Manifest: json.RawMessage(`{}`), Graph: json.RawMessage(`{}`), Diff: json.RawMessage(`{}`), ModuleArtifacts: artifacts})
			}
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			// No Draft history was created. Remove the later empty migration first
			// so this assertion measures migration 36's own atomic refusal boundary.
			require.NoError(t, goose.DownToContext(t.Context(), db, "migrations", 36))
			before := appliedMigrations(t, t.Context(), db)
			require.ErrorContains(t, goose.DownToContext(t.Context(), db, "migrations", 35), "cannot downgrade while Resource Type contract execution snapshots exist")
			require.Equal(t, before, appliedMigrations(t, t.Context(), db), "refused downgrade must preserve the journal")
			requireStarts(t, t.Context(), scoped)
		})
	}
}
