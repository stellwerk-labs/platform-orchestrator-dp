//go:build integration

package api

import (
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/require"
)

func TestProviderUsageCoversPartialStateAndMissingInventory(t *testing.T) {
	ctx := t.Context()
	db := externalDeploymentTestDatabase(t, ctx, os.Getenv("MODEL_TEST_DB_CONNECTION_STRING"))
	const org, project, environment, runner = "fixture", "retirement", "development", "fixture-runner"
	envUUID := uuid.New()
	emptyGraph, err := graphs.ToJson(&graph.Graph[*graphs.GraphNodeModuleConfig]{})
	require.NoError(t, err)
	variant := strings.Repeat("a", 64)
	boundGraph, err := graphs.ToJson(&graph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[graph.ResourceCoordinate]graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			{Type: "fixture", Class: "default", Id: "shared.fixture"}: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{
				DefinitionId: "fixture", VersionId: "1.0.0", ProviderFullIdToHashVariantMapping: map[string]string{"random.fixture": variant},
			}},
		},
	})
	require.NoError(t, err)
	const emptyTofu = "terraform { required_providers {} }"
	alias := graphs.LocalProviderName("random", "fixture", variant)
	boundTofu := "terraform {\n required_providers {\n " + alias + " = {source = \"hashicorp/random\", version = \"= 3.7.2\"}\n }\n}\n provider \"" + alias + "\" {alias = \"" + alias + "\"}\n"
	create := func(mode model.DeploymentMode, status model.DeploymentStatus, encoded []byte, tofu string) {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		d, err := db.CreateDeployment(ctx, tx, org, project, environment, model.CreateDeploymentParams{
			CreatedBy: uuid.New(), DeploymentEnvUuid: envUUID, Mode: mode, RunnerId: runner, RunnerLogLevel: "info",
			Manifest: model.EncodedDeploymentManifest("{}"), Graph: encoded, Tofu: model.RawTofu(tofu),
		})
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		_, err = db.UpdateDeploymentStatusAndOutputs(ctx, nil, d.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: status})
		require.NoError(t, err)
	}
	proof, err := scanProviderUsage(ctx, db, org, nil)
	require.NoError(t, err)
	require.Empty(t, proof.Providers)
	_, err = scanProviderUsage(ctx, db, org, []uuid.UUID{envUUID})
	require.ErrorContains(t, err, "required Environment state")
	create(model.DeploymentModeDeployPlan, model.DeploymentStatusSucceeded, boundGraph, boundTofu)
	proof, err = scanProviderUsage(ctx, db, org, nil)
	require.NoError(t, err)
	require.Empty(t, proof.Providers, "a new never-admitted plan has no managed resources")
	_, err = scanProviderUsage(ctx, db, org, []uuid.UUID{envUUID})
	require.ErrorContains(t, err, "no successful state boundary", "a first plan cannot replace missing required state proof")
	create(model.DeploymentModeDeploy, model.DeploymentStatusFailed, emptyGraph, emptyTofu)
	_, err = scanProviderUsage(ctx, db, org, []uuid.UUID{envUUID})
	require.ErrorContains(t, err, "no successful state boundary", "a first failed apply cannot repair missing required history")
	create(model.DeploymentModeDeploy, model.DeploymentStatusSucceeded, boundGraph, boundTofu)
	check := func(expected []string) {
		t.Helper()
		proof, err := scanProviderUsage(ctx, db, org, []uuid.UUID{envUUID})
		require.NoError(t, err)
		require.True(t, proof.Complete)
		require.False(t, proof.Busy)
		require.Equal(t, org, proof.OrgId)
		require.ElementsMatch(t, []uuid.UUID{envUUID}, proof.EnvironmentUuids)
		require.Equal(t, expected, proof.Providers)
	}
	check([]string{"random.fixture"})
	create(model.DeploymentModeDeployPlan, model.DeploymentStatusSucceeded, emptyGraph, emptyTofu)
	check([]string{"random.fixture"})
	create(model.DeploymentModeDeploy, model.DeploymentStatusFailed, emptyGraph, boundTofu)
	check([]string{"random.fixture"})
	create(model.DeploymentModeDeploy, model.DeploymentStatusSucceeded, emptyGraph, boundTofu)
	check([]string{})
	create(model.DeploymentModeDeploy, model.DeploymentStatusSucceeded, boundGraph, boundTofu)
	create(model.DeploymentModeDestroy, model.DeploymentStatusSucceeded, boundGraph, boundTofu)
	check([]string{})

	// A foreign state head can satisfy the ID foreign key while hiding this
	// Environment from the ordinary listing's scope join.
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	other, err := db.CreateDeployment(ctx, tx, org, project, "other", model.CreateDeploymentParams{
		CreatedBy: uuid.New(), DeploymentEnvUuid: uuid.New(), Mode: model.DeploymentModeDeploy,
		Manifest: model.EncodedDeploymentManifest("{}"), Graph: emptyGraph, Tofu: model.RawTofu(emptyTofu), RunnerId: runner, RunnerLogLevel: "info",
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "UPDATE deployment_environments SET last_state_deployment_id=$1 WHERE id=$2", other.Id, envUUID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = scanProviderUsage(ctx, db, org, []uuid.UUID{envUUID})
	require.ErrorContains(t, err, "managed state head")
}
