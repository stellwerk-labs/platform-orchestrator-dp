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
	"github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stretchr/testify/require"
)

func TestDraftValidationRestoreSearchPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	_, schema, scoped := migrationTestDatabase(t, ctx, connection)
	db := schemaConnection(t, scoped)
	source, err := fs.Sub(embedMigrations, "migrations")
	require.NoError(t, err)
	provider, err := goose.NewProvider(goose.DialectPostgres, db, source,
		goose.WithDisableGlobalRegistry(true), goose.WithGoMigrations(goose.NewGoMigration(2,
			&goose.GoFunc{RunTx: hstandardoutbox.MigrateUp01}, &goose.GoFunc{RunTx: hstandardoutbox.MigrateDown01})))
	require.NoError(t, err)
	_, err = provider.UpTo(ctx, 39)
	require.NoError(t, err)
	reference := testDraftReference()
	number, contract := int64(1), uuid.New()
	artifact := ModuleArtifactRequirement{ModuleID: "restore", ModuleUUID: reference.ModuleUUID,
		Version: reference.SnapshotUUID.String(), VersionUUID: reference.SnapshotUUID, Source: "inline",
		DraftTest: reference, ResourceTypeVersion: &number, ResourceTypeVersionUUID: &contract}
	artifacts, err := json.Marshal([]ModuleArtifactRequirement{artifact})
	require.NoError(t, err)
	graphDocument := map[string]any{"nodes": map[string]any{"restore": map[string]any{"config": map[string]any{
		"draft_test": reference, "version_id": reference.SnapshotUUID, "resource_type_version": number, "resource_type_version_uuid": contract,
	}}}}
	graph, err := json.Marshal(graphDocument)
	require.NoError(t, err)

	// pg_restore COPY qualifies its table, but nested PL/pgSQL calls used to inherit
	// the deliberately empty session path and fail even with the function present.
	for _, fixture := range []struct {
		name             string
		artifacts, graph []byte
	}{
		{"artifact", artifacts, []byte(`{}`)}, {"graph", []byte(`[]`), graph},
	} {
		t.Run("version39_"+fixture.name, func(t *testing.T) {
			err := copyDraftDeployment(ctx, db, schema, reference.EnvironmentUUID, fixture.artifacts, fixture.graph, "")
			var pg *pq.Error
			require.ErrorAs(t, err, &pg)
			require.Equal(t, pq.ErrorCode("42883"), pg.Code)
			require.Contains(t, pg.Message, "valid_draft_test_reference")
		})
	}
	// Preserve a real nonempty successful Draft deployment through the forward fix.
	require.NoError(t, copyDraftDeployment(ctx, db, schema, reference.EnvironmentUUID, artifacts, graph, schema))

	// The migration must preserve all function identity/security/body metadata and
	// other per-function settings, not replace validators or relax any CHECK.
	_, err = db.ExecContext(ctx, `ALTER FUNCTION valid_draft_test_artifacts(jsonb,uuid) SET statement_timeout TO '11s'`)
	require.NoError(t, err)
	metadata := func() string {
		var value string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_agg(to_jsonb(p)-'proconfig' ORDER BY p.oid)::text
   FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
   WHERE n.nspname=$1 AND p.proname IN ('valid_draft_test_artifacts','valid_draft_test_graph')`, schema).Scan(&value))
		return value
	}
	before := metadata()
	applied, err := provider.UpTo(ctx, 40)
	require.NoError(t, err)
	require.Len(t, applied, 1)
	require.Equal(t, int64(40), applied[0].Source.Version)
	require.Equal(t, before, metadata())
	applied, err = provider.UpTo(ctx, 40)
	require.NoError(t, err)
	require.Empty(t, applied)
	var timeoutRetained bool
	require.NoError(t, db.QueryRowContext(ctx, `SELECT 'statement_timeout=11s'=ANY(proconfig)
  FROM pg_proc WHERE oid=$1::regprocedure`, pq.QuoteIdentifier(schema)+`.valid_draft_test_artifacts(jsonb,uuid)`).Scan(&timeoutRetained))
	require.True(t, timeoutRetained)
	require.NoError(t, copyDraftDeployment(ctx, db, schema, reference.EnvironmentUUID, artifacts, graph, ""))

	// Caller-controlled schemas cannot substitute an always-true validator. Builtin
	// calls must also continue to resolve through pg_catalog before that schema.
	_, shadow, _ := migrationTestDatabase(t, ctx, connection)
	_, err = db.ExecContext(ctx, `CREATE FUNCTION `+pq.QuoteIdentifier(shadow)+`.valid_draft_test_reference(jsonb)
  RETURNS boolean IMMUTABLE LANGUAGE sql AS 'SELECT true'`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE FUNCTION `+pq.QuoteIdentifier(shadow)+`.jsonb_typeof(jsonb)
  RETURNS text IMMUTABLE LANGUAGE sql AS $$SELECT 'not-a-builtin'::text$$`)
	require.NoError(t, err)
	require.NoError(t, copyDraftDeployment(ctx, db, schema, reference.EnvironmentUUID, artifacts, graph, shadow+","+schema))
	reference.DraftRevision = 0
	invalidArtifacts, err := json.Marshal([]ModuleArtifactRequirement{artifact})
	require.NoError(t, err)
	invalidGraph, err := json.Marshal(graphDocument)
	require.NoError(t, err)
	for _, fixture := range []struct {
		artifacts, graph []byte
		constraint       string
	}{
		{invalidArtifacts, []byte(`{}`), "deployment_draft_test_artifacts_valid"},
		{[]byte(`[]`), invalidGraph, "deployment_draft_test_graph_valid"},
	} {
		for _, path := range []string{"", shadow + "," + schema} {
			err = copyDraftDeployment(ctx, db, schema, reference.EnvironmentUUID, fixture.artifacts, fixture.graph, path)
			var pg *pq.Error
			require.ErrorAs(t, err, &pg)
			require.Equal(t, pq.ErrorCode("23514"), pg.Code)
			require.Equal(t, fixture.constraint, pg.Constraint)
		}
	}
	// Valid provenance remains bound to its exact Environment, including COPY.
	reference.DraftRevision = 1
	err = copyDraftDeployment(ctx, db, schema, uuid.New(), artifacts, graph, "")
	var pg *pq.Error
	require.ErrorAs(t, err, &pg)
	require.Equal(t, pq.ErrorCode("23514"), pg.Code)

	// Logical Down retains this compatible safety metadata, and normal Up is safe.
	_, err = provider.DownTo(ctx, 39)
	require.NoError(t, err)
	require.NoError(t, copyDraftDeployment(ctx, db, schema, reference.EnvironmentUUID, artifacts, graph, ""))
	_, err = provider.UpTo(ctx, 40)
	require.NoError(t, err)
	var count int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM deployments`).Scan(&count))
	require.Equal(t, 4, count)

	// The pinned transition remains meaningful when later migrations exist, while
	// the latest schema must continue to accept restore COPY and reject bad data.
	_, err = provider.Up(ctx)
	require.NoError(t, err)
	require.NoError(t, copyDraftDeployment(ctx, db, schema, reference.EnvironmentUUID, artifacts, graph, ""))
	err = copyDraftDeployment(ctx, db, schema, reference.EnvironmentUUID, invalidArtifacts, []byte(`{}`), "")
	require.ErrorAs(t, err, &pg)
	require.Equal(t, pq.ErrorCode("23514"), pg.Code)
	require.Equal(t, "deployment_draft_test_artifacts_valid", pg.Constraint)
}

func copyDraftDeployment(ctx context.Context, db *sql.DB, schema string, environment uuid.UUID, artifacts, graph []byte, path string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `SELECT set_config('search_path',$1,true)`, path); err != nil {
		return err
	}
	id := uuid.New()
	if _, err = tx.ExecContext(ctx, `INSERT INTO `+pq.QuoteIdentifier(schema)+`.deployment_environments
  (id,org_id,project_id,env_id,last_executing_deployment_id,last_executing_deployment_at,last_state_deployment_id,last_state_deployment_at)
  VALUES($1::uuid,'restore-org','restore-project',$1::uuid::text,$2,now(),$2,now()) ON CONFLICT (id) DO NOTHING`, environment, id); err != nil {
		return err
	}
	statement, err := tx.PrepareContext(ctx, pq.CopyInSchema(schema, "deployments",
		"id", "de_id", "created_at", "completed_at", "created_by", "mode", "status", "status_message", "manifest", "graph", "tofu", "runner_id", "module_artifacts"))
	if err != nil {
		return err
	}
	defer func() { _ = statement.Close() }()
	if _, err = statement.ExecContext(ctx, id, environment, time.Now().UTC(), time.Now().UTC(), uuid.New(), "deploy", "succeeded", "restored actual Draft snapshot", []byte(`{}`), graph, []byte("terraform {}"), "restore-runner", string(artifacts)); err != nil {
		return err
	}
	if _, err = statement.ExecContext(ctx); err != nil {
		return err
	}
	if err = statement.Close(); err != nil {
		return err
	}
	return tx.Commit()
}
