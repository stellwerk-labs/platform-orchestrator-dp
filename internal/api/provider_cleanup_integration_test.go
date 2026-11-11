//go:build integration

package api

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestProviderCleanupHistoryUsesMutatingStateHead(t *testing.T) {
	ctx := t.Context()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "MODEL_TEST_DB_CONNECTION_STRING must name an isolated local PostgreSQL test database")
	db := externalDeploymentTestDatabase(t, ctx, connection)
	const org, project, env, runner = "fixture", "cleanup", "development", "fixture-runner"
	envUUID := uuid.New()
	create := func(mode model.DeploymentMode, tofu string, expected uuid.UUID) (*model.DeploymentSummary, error) {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		deployment, err := db.CreateDeployment(ctx, tx, org, project, env, model.CreateDeploymentParams{
			CreatedBy: uuid.New(), DeploymentEnvUuid: envUUID, Mode: mode,
			Manifest: model.EncodedDeploymentManifest("{}"), Graph: model.EncodedDeploymentGraph("{}"), Tofu: model.RawTofu(tofu),
			RunnerId: runner, RunnerLogLevel: "info", ExpectedBaseDeploymentID: opt.Of(expected),
		})
		if err == nil {
			require.NoError(t, tx.Commit())
		}
		return deployment, err
	}
	complete := func(mode model.DeploymentMode, status model.DeploymentStatus, tofu string, expected uuid.UUID) uuid.UUID {
		t.Helper()
		deployment, err := create(mode, tofu, expected)
		require.NoError(t, err)
		_, err = db.UpdateDeploymentStatusAndOutputs(ctx, nil, deployment.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: status})
		require.NoError(t, err)
		return deployment.Id
	}

	base := complete(model.DeploymentModeDeploy, model.DeploymentStatusSucceeded, "successful base", uuid.Nil)
	_, err := create(model.DeploymentModeDeploy, "compiled before any deployment", uuid.Nil)
	conflict, ok := model.IsErrConflict(err)
	require.True(t, ok, "%v", err)
	require.Equal(t, model.CodeDeploymentBaseChanged, conflict.Code)

	failed := complete(model.DeploymentModeDeploy, model.DeploymentStatusFailed, "partial apply", base)
	cleanup := complete(model.DeploymentModeRollback, model.DeploymentStatusFailed, "failed cleanup", failed)
	complete(model.DeploymentModeDeployPlan, model.DeploymentStatusSucceeded, "plan-only result", cleanup)

	head, snapshot, err := currentProviderState(ctx, db, nil, org, project, env, envUUID)
	require.NoError(t, err)
	require.Equal(t, cleanup, head.Id)
	history, _, err := providerStateHistory(ctx, db, nil, head, snapshot)
	require.NoError(t, err)
	require.Len(t, history, 3)
	require.Equal(t, "failed cleanup", string(history[0].Tofu))
	require.Equal(t, "partial apply", string(history[1].Tofu))
	require.Equal(t, "successful base", string(history[2].Tofu))
	require.True(t, history[0].MayBePartial)
	require.True(t, history[1].MayBePartial)
	require.False(t, history[2].MayBePartial)

	destroyed := complete(model.DeploymentModeDestroy, model.DeploymentStatusSucceeded, "destroyed", cleanup)
	failedAfterDestroy := complete(model.DeploymentModeDeploy, model.DeploymentStatusFailed, "new failed apply", destroyed)
	head, snapshot, err = currentProviderState(ctx, db, nil, org, project, env, envUUID)
	require.NoError(t, err)
	require.Equal(t, failedAfterDestroy, head.Id)
	history, _, err = providerStateHistory(ctx, db, nil, head, snapshot)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, "new failed apply", string(history[0].Tofu))
}
