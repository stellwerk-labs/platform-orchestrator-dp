//go:build integration

package model

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestEnvironmentDeploymentReadinessUsesLiveLeaseAndSuccessfulBase(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	_, _, scopedConnection := migrationTestDatabase(t, ctx, connection)
	store, err := NewDatabaser(ctx, zap.NewNop(), scopedConnection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	orgID, projectID, envID := "inventory-org", "inventory-project", "inventory-env"
	environmentUUID, moduleUUID, versionUUID := uuid.New(), uuid.New(), uuid.New()
	create := func() *DeploymentSummary {
		tx, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		deployment, err := store.CreateDeployment(ctx, tx, orgID, projectID, envID, CreateDeploymentParams{
			CreatedBy: uuid.New(), DeploymentEnvUuid: environmentUUID, Mode: DeploymentModeDeploy,
			Manifest: EncodedDeploymentManifest(`{"workloads":{}}`), Graph: EncodedDeploymentGraph(`{"nodes":{},"edges":{},"workloads":{},"shared_resources":{},"leaf_resources":{}}`),
			Tofu: RawTofu("terraform {}"), RunnerId: "unused", RunnerLogLevel: "info", IdempotencyKeyDigest: opt.Empty[string](),
			ModuleArtifacts: []ModuleArtifactRequirement{{
				ModuleID: "postgres", ModuleUUID: moduleUUID, Version: "v2", VersionUUID: versionUUID,
				SemanticVersion: "2.0.0", ArtifactDigest: "sha256:abc", Source: "registry.invalid/module",
			}},
		})
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		return deployment
	}
	lookup := func() EnvironmentDeploymentReadiness {
		items, err := store.GetEnvironmentDeploymentReadiness(ctx, nil, orgID, []EnvironmentRef{{ProjectID: projectID, EnvID: envID}})
		require.NoError(t, err)
		require.Len(t, items, 1)
		return items[0]
	}

	first := create()
	busy := lookup()
	require.Equal(t, first.Id, busy.LatestDeploymentID.Must())
	require.True(t, busy.ActiveLeaseUntil.IsSet())
	require.False(t, busy.EffectiveBaseDeploymentID.IsSet())

	expiration, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = expiration.Rollback() }()
	_, err = expiration.ExecContext(ctx, `UPDATE environment_deployment_leases SET expires_at=now() - interval '1 second' WHERE deployment_id=$1`, first.Id)
	require.NoError(t, err)
	require.NoError(t, expiration.Commit())
	require.False(t, lookup().ActiveLeaseUntil.IsSet())

	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, first.Id, UpdateDeploymentStatusAndOutputsParams{
		Status: DeploymentStatusSucceeded, StatusMessage: "done", Metrics: DeploymentMetrics{},
	})
	require.NoError(t, err)
	ready := lookup()
	require.Equal(t, first.Id, ready.EffectiveBaseDeploymentID.Must())
	require.Equal(t, moduleUUID, ready.ActiveModules[0].ModuleUUID)
	require.Equal(t, versionUUID, ready.ActiveModules[0].VersionUUID)

	second := create()
	require.True(t, lookup().ActiveLeaseUntil.IsSet())
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, second.Id, UpdateDeploymentStatusAndOutputsParams{
		Status: DeploymentStatusFailed, StatusMessage: "failed", Metrics: DeploymentMetrics{},
	})
	require.NoError(t, err)
	unreconciled := lookup()
	require.Equal(t, second.Id, unreconciled.LatestDeploymentID.Must())
	require.False(t, unreconciled.ActiveLeaseUntil.IsSet())
	require.Equal(t, first.Id, unreconciled.EffectiveBaseDeploymentID.Must())
}

func TestExternalDeploymentPlanSnapshotIsImmutableAndRequestDeduplicated(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	_, _, scopedConnection := migrationTestDatabase(t, ctx, connection)
	store, err := NewDatabaser(ctx, zap.NewNop(), scopedConnection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	candidate := ExternalDeploymentPlanSnapshot{
		OrgID: "snapshot-org", PluginID: "dev.example.rollouts", RequestID: "plan-1", ActorID: uuid.New(),
		RequestBodySHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ProjectID:         "project", ProjectUUID: uuid.New(), EnvID: "production", EnvironmentUUID: uuid.New(), BaseDeploymentID: uuid.New(),
		Fingerprint: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Manifest:    json.RawMessage(`{"workloads":{"api":{"outputs":{"password":"retained-secret"}}}}`),
		Graph:       json.RawMessage(`{"nodes":{}}`), Diff: json.RawMessage(`{"changes":[]}`),
	}
	created, err := CreateOrGetExternalDeploymentPlanSnapshot(ctx, tx, candidate)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NotEqual(t, uuid.Nil, created.ID)
	require.Contains(t, string(created.Manifest), "retained-secret")

	replayTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	replayed, err := CreateOrGetExternalDeploymentPlanSnapshot(ctx, replayTx, candidate)
	require.NoError(t, err)
	require.Equal(t, created.ID, replayed.ID)
	require.NoError(t, replayTx.Commit())

	conflictTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	candidate.ActorID = uuid.New()
	_, err = CreateOrGetExternalDeploymentPlanSnapshot(ctx, conflictTx, candidate)
	_, conflict := IsErrConflict(err)
	require.True(t, conflict)
	require.NoError(t, conflictTx.Rollback())
}
