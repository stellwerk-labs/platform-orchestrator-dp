//go:build integration

package model

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
)

func testDraftReference() *graphs.DraftTestReference {
	return &graphs.DraftTestReference{SnapshotUUID: uuid.New(), ModuleUUID: uuid.New(), DraftUUID: uuid.New(), DraftRevision: 1, ProjectUUID: uuid.New(), EnvironmentUUID: uuid.New()}
}

func TestDraftTestSnapshotStorageShapesAndDowngrade(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	_, _, scoped := migrationTestDatabase(t, t.Context(), connection)
	requireStarts(t, t.Context(), scoped)
	db := schemaConnection(t, scoped)
	reference := testDraftReference()
	rawReference, err := json.Marshal(reference)
	require.NoError(t, err)
	valid := func(query string, value any) bool {
		var result bool
		require.NoError(t, db.QueryRowContext(t.Context(), query, value).Scan(&result))
		return result
	}
	require.True(t, valid(`SELECT valid_draft_test_reference($1::jsonb)`, rawReference))
	require.False(t, valid(`SELECT valid_draft_test_reference($1::jsonb)`, `{"draft_revision":1}`))
	number := int64(1)
	contract := uuid.New()
	artifact := ModuleArtifactRequirement{ModuleID: "storage", ModuleUUID: reference.ModuleUUID, Version: reference.SnapshotUUID.String(), VersionUUID: reference.SnapshotUUID, Source: "inline", DraftTest: reference, ResourceTypeVersion: &number, ResourceTypeVersionUUID: &contract}
	good, err := json.Marshal([]ModuleArtifactRequirement{artifact})
	require.NoError(t, err)
	require.True(t, valid(`SELECT valid_draft_test_artifacts($1::jsonb)`, good))
	var bound bool
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT valid_draft_test_artifacts($1::jsonb,$2)`, good, reference.EnvironmentUUID).Scan(&bound))
	require.True(t, bound)
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT valid_draft_test_artifacts($1::jsonb,$2)`, good, uuid.New()).Scan(&bound))
	require.False(t, bound)
	artifact.SemanticVersion = "1.2.0"
	bad, err := json.Marshal([]ModuleArtifactRequirement{artifact})
	require.NoError(t, err)
	require.False(t, valid(`SELECT valid_draft_test_artifacts($1::jsonb)`, bad))
	artifact.SemanticVersion = ""
	artifact.VersionUUID = uuid.New()
	bad, err = json.Marshal([]ModuleArtifactRequirement{artifact})
	require.NoError(t, err)
	require.False(t, valid(`SELECT valid_draft_test_artifacts($1::jsonb)`, bad))
	require.True(t, valid(`SELECT valid_draft_test_artifacts($1::jsonb)`, `[{"module_id":"legacy"}]`))
	store, err := NewDatabaser(t.Context(), zap.NewNop(), scoped)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	tx, err := store.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	wanted := DeploymentAdmissionReceipt{
		OrgID:              "org",
		AdmissionID:        uuid.New(),
		ProjectUUID:        reference.ProjectUUID,
		EnvironmentUUID:    reference.EnvironmentUUID,
		ActorID:            uuid.New(),
		RequestDigest:      strings.Repeat("a", 64),
		Mode:               DeploymentModeDeploy,
		ModuleVersions:     map[string]uuid.UUID{reference.ModuleUUID.String(): reference.SnapshotUUID},
		DraftTestSnapshots: map[string]uuid.UUID{reference.ModuleUUID.String(): reference.SnapshotUUID},
		Disposition:        "pending",
	}
	_, err = PrepareDeploymentAdmissionReceipt(t.Context(), tx, wanted)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	tx, err = store.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	retained, err := GetDeploymentAdmissionReceipt(t.Context(), tx, "org", wanted.AdmissionID)
	require.NoError(t, err)
	require.Equal(t, wanted.DraftTestSnapshots, retained.DraftTestSnapshots)
	require.NoError(t, tx.Commit())
	tx, err = store.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	recovered, err := ListUnsettledDeploymentAdmissionReceipts(t.Context(), tx, 10, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	require.Equal(t, wanted.DraftTestSnapshots, recovered[0].DraftTestSnapshots)
	require.NoError(t, tx.Commit())
	before := appliedMigrations(t, t.Context(), db)
	require.ErrorContains(t, goose.DownToContext(t.Context(), db, "migrations", 36), "cannot downgrade while Draft test execution history exists")
	require.Equal(t, before, appliedMigrations(t, t.Context(), db))
	requireStarts(t, t.Context(), scoped)
}

func TestDraftTestSnapshotStorageEmptyDowngradeAndLegacyEncoding(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	_, _, scoped := migrationTestDatabase(t, t.Context(), connection)
	requireStarts(t, t.Context(), scoped)
	db := schemaConnection(t, scoped)
	var valid bool
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT valid_draft_test_graph(retained_graph_document($1))`, []byte("historical opaque graph")).Scan(&valid))
	require.True(t, valid)
	require.NoError(t, goose.DownToContext(t.Context(), db, "migrations", 36))
	requireStarts(t, t.Context(), scoped)
}

func TestDraftTestSnapshotsCannotEnterAddOnPlans(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	for _, kind := range []string{"artifact", "graph"} {
		t.Run(kind, func(t *testing.T) {
			_, _, scoped := migrationTestDatabase(t, t.Context(), connection)
			store, err := NewDatabaser(t.Context(), zap.NewNop(), scoped)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			reference := testDraftReference()
			number, contract := int64(1), uuid.New()
			graph := json.RawMessage(`{}`)
			artifacts := []ModuleArtifactRequirement{}
			if kind == "artifact" {
				artifacts = append(
					artifacts,
					ModuleArtifactRequirement{ModuleID: "storage", ModuleUUID: reference.ModuleUUID, Version: reference.SnapshotUUID.String(), VersionUUID: reference.SnapshotUUID, Source: "inline", DraftTest: reference, ResourceTypeVersion: &number, ResourceTypeVersionUUID: &contract},
				)
			} else {
				raw, err := json.Marshal(map[string]any{"nodes": map[string]any{"node": map[string]any{"config": map[string]any{"draft_test": reference}}}})
				require.NoError(t, err)
				graph = raw
			}
			tx, err := store.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			_, err = CreateOrGetExternalDeploymentPlanSnapshot(t.Context(), tx, ExternalDeploymentPlanSnapshot{
				ID:                uuid.New(),
				OrgID:             "org",
				PluginID:          "dev.example.workflow",
				RequestID:         "no-test-targets",
				ActorID:           uuid.New(),
				RequestBodySHA256: strings.Repeat("a", 64),
				ProjectID:         "project",
				ProjectUUID:       reference.ProjectUUID,
				EnvID:             "development",
				EnvironmentUUID:   reference.EnvironmentUUID,
				BaseDeploymentID:  uuid.New(),
				Fingerprint:       "sha256:" + strings.Repeat("b", 64),
				Manifest:          json.RawMessage(`{}`),
				Graph:             graph,
				Diff:              json.RawMessage(`{}`),
				ModuleArtifacts:   artifacts,
			})
			require.ErrorContains(t, err, "external_plan_no_draft_tests")
		})
	}
}
