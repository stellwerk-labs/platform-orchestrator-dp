//go:build integration

package model

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/nativelogs"
	"github.com/stretchr/testify/require"
)

func TestPipelineLogReferencesSurviveProgressReplacementAndSettlement(t *testing.T) {
	ctx := t.Context()
	store := dispatchTestStore(t, ctx)
	definition, err := ResolvePipeline(nil, DeploymentModeDeploy)
	require.NoError(t, err)
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	dep, err := store.CreateDeployment(
		ctx,
		tx,
		"log-projection-org",
		"p",
		"e",
		CreateDeploymentParams{
			DeploymentEnvUuid: uuid.New(),
			Mode:              DeploymentModeDeploy,
			CreatedBy:         uuid.New(),
			RunnerId:          "runner",
			RunnerLogLevel:    "info",
			Manifest:          EncodedDeploymentManifest(`{}`),
			Graph:             EncodedDeploymentGraph(`{}`),
			Tofu:              RawTofu("terraform {}"),
			Pipeline:          definition,
			PipelineBackend:   "kubernetes",
		},
	)
	require.NoError(t, err)
	first, second := pipelineLogDescriptor(1), pipelineLogDescriptor(2)
	require.NoError(t, AddPipelineLogReference(ctx, tx, dep.Id, second))
	require.NoError(t, AddPipelineLogReference(ctx, tx, dep.Id, first))
	run, err := ReadDeploymentPipeline(ctx, tx, dep.Id)
	require.NoError(t, err)
	require.Equal(t, "accepted", run.Progress.Phase, "log receipt cannot advance execution")
	before, err := json.Marshal(run.Progress.LogReferences)
	require.NoError(t, err)
	for _, progress := range []PipelineProgress{
		{ID: dep.Id.String(), Phase: "applying", Revision: 5},
		{ID: dep.Id.String(), Phase: "planning", Revision: 3},
		{ID: dep.Id.String(), Phase: "failed", Revision: 6, LogReferences: []nativelogs.Descriptor{pipelineLogDescriptor(99)}},
	} {
		require.NoError(t, UpdatePipelineProgress(ctx, tx, progress))
		run, err = ReadDeploymentPipeline(ctx, tx, dep.Id)
		require.NoError(t, err)
		after, err := json.Marshal(run.Progress.LogReferences)
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
	require.NoError(t, SettleDeploymentPipeline(ctx, tx, dep.Id, DeploymentStatusFailed))
	require.NoError(t, UpdatePipelineProgress(ctx, tx, PipelineProgress{ID: dep.Id.String(), Phase: "planning", Revision: 100}))
	require.NoError(t, AddPipelineLogReference(ctx, tx, dep.Id, first))
	run, err = ReadDeploymentPipeline(ctx, tx, dep.Id)
	require.NoError(t, err)
	require.Equal(t, "failed", run.Progress.Phase)
	require.False(t, run.Progress.InfrastructureApplied)
	after, err := json.Marshal(run.Progress.LogReferences)
	require.NoError(t, err)
	require.Equal(t, before, after)
	for revision := int64(3); revision <= 130; revision++ {
		require.NoError(t, AddPipelineLogReference(ctx, tx, dep.Id, pipelineLogDescriptor(revision)))
	}
	run, err = ReadDeploymentPipeline(ctx, tx, dep.Id)
	require.NoError(t, err)
	require.Len(t, run.Progress.LogReferences, 128)
	require.True(t, run.Progress.LogReferencesLimited)
	require.Equal(t, "failed", run.Progress.Phase)
	require.NoError(t, UpdatePipelineProgress(ctx, tx, PipelineProgress{ID: dep.Id.String(), Phase: "failed", Revision: 101}))
	run, err = ReadDeploymentPipeline(ctx, tx, dep.Id)
	require.NoError(t, err)
	require.Len(t, run.Progress.LogReferences, 128)
	require.True(t, run.Progress.LogReferencesLimited)
	require.NoError(t, tx.Commit())
}
