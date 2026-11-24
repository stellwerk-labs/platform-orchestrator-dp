//go:build integration

package model

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPipelineCanonicalFenceSurvivesExpiredLeaseAndForceFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	definition, err := ResolvePipeline(nil, DeploymentModeDeploy)
	require.NoError(t, err)
	params := CreateDeploymentParams{DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy, CreatedBy: uuid.New(), RunnerId: "runner", RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}"), Pipeline: definition, PipelineBackend: "kubernetes"}
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	dep, err := store.CreateDeployment(ctx, tx, "unknown-org", "p", "e", params)
	require.NoError(t, err)
	require.True(t, dep.HasPipeline)
	run, err := ReadDeploymentPipeline(ctx, tx, dep.Id)
	require.NoError(t, err)
	require.Equal(t, "accepted", run.Progress.Phase)
	require.Equal(t, *definition, run.Definition)
	require.NoError(t, tx.Commit())
	dep, _, _, _, err = store.GetDeployment(ctx, nil, "unknown-org", dep.Id, GetModeDefault)
	require.NoError(t, err)
	require.True(t, dep.HasPipeline)
	_, err = store.(*databaser).ExecContext(ctx, `UPDATE environment_deployment_leases SET expires_at=now()-interval '1 second' WHERE deployment_id=$1`, dep.Id)
	require.NoError(t, err)
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, dep.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusFailed, StatusMessage: "operator forced failure"})
	require.NoError(t, err)
	params.Pipeline = nil // Even legacy admission cannot bypass a native execution's ownership.
	require.ErrorContains(t, replaceHead(ctx, store, params), "unsettled pipeline")
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, SettleDeploymentPipeline(ctx, tx, dep.Id, DeploymentStatusFailed))
	require.NoError(t, tx.Commit())
	require.NoError(t, replaceHead(ctx, store, params))
}

func TestPipelineProgressCannotReopenSettledDeployment(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	definition, err := ResolvePipeline(nil, DeploymentModeDeploy)
	require.NoError(t, err)
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	dep, err := store.CreateDeployment(ctx, tx, "progress-org", "p", "e", CreateDeploymentParams{DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy, CreatedBy: uuid.New(), RunnerId: "runner", RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}"), Pipeline: definition, PipelineBackend: "kubernetes"})
	require.NoError(t, err)
	progress := PipelineProgress{ID: dep.Id.String(), EnvironmentID: dep.DeploymentEnvUuid.String(), Phase: "post_failed", Revision: 5, InfrastructureApplied: true}
	require.NoError(t, UpdatePipelineProgress(ctx, tx, progress))
	older := progress
	older.Revision, older.Phase, older.InfrastructureApplied = 3, "planning", false
	require.NoError(t, UpdatePipelineProgress(ctx, tx, older))
	run, err := ReadDeploymentPipeline(ctx, tx, dep.Id)
	require.NoError(t, err)
	require.Equal(t, "post_failed", run.Progress.Phase)
	require.True(t, run.Progress.InfrastructureApplied)
	require.NoError(t, SettleDeploymentPipeline(ctx, tx, dep.Id, DeploymentStatusSucceeded))
	progress.Revision, progress.Phase = 6, "failed" // Terminal pipeline failure can follow proven infrastructure success.
	require.NoError(t, UpdatePipelineProgress(ctx, tx, progress))
	older.Revision = 7
	require.NoError(t, UpdatePipelineProgress(ctx, tx, older))
	run, err = ReadDeploymentPipeline(ctx, tx, dep.Id)
	require.NoError(t, err)
	require.Equal(t, "failed", run.Progress.Phase)
	require.True(t, run.Progress.InfrastructureApplied)
	require.NoError(t, SettleDeploymentPipeline(ctx, tx, dep.Id, DeploymentStatusSucceeded))
	run, err = ReadDeploymentPipeline(ctx, tx, dep.Id)
	require.NoError(t, err)
	require.Equal(t, "failed", run.Progress.Phase)
	require.NoError(t, tx.Commit())
}

func TestFailedManualPlanDoesNotRetainMutationOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	definition, err := ResolvePipeline(nil, DeploymentModeDeployPlan)
	require.NoError(t, err)
	params := CreateDeploymentParams{DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeployPlan, CreatedBy: uuid.New(), RunnerId: "runner", RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}"), Pipeline: definition, PipelineBackend: "kubernetes"}
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	dep, err := store.CreateDeployment(ctx, tx, "unknown-org", "p", "e", params)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, dep.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusFailed, StatusMessage: "operator forced failed manual Plan"})
	require.NoError(t, err)
	params.Mode = DeploymentModeDeploy
	params.Pipeline, err = ResolvePipeline(nil, params.Mode)
	require.NoError(t, err)
	require.NoError(t, replaceHead(ctx, store, params), "read-only Plan has no infrastructure mutation ownership to retain")
}
