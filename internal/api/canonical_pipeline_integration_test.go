//go:build integration

package api

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func TestCanonicalPipelineProgressAndFrozenCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	store := deploymentAdmissionTestDatabase(t, ctx, connection)
	definition, err := model.ResolvePipeline(nil, model.DeploymentModeDeploy)
	require.NoError(t, err)
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	dep, err := store.CreateDeployment(
		ctx,
		tx,
		"canonical-org",
		"project",
		"env",
		model.CreateDeploymentParams{
			DeploymentEnvUuid: uuid.New(),
			Mode:              model.DeploymentModeDeploy,
			CreatedBy:         uuid.New(),
			RunnerId:          "site",
			RunnerLogLevel:    "info",
			Manifest:          model.EncodedDeploymentManifest(`{}`),
			Graph:             model.EncodedDeploymentGraph(`{}`),
			Tofu:              model.RawTofu("terraform {}"),
			Pipeline:          definition,
			PipelineBackend:   "kubernetes",
		},
	)
	require.NoError(t, err)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	server := &Server{Database: store, RunnerTokenSalt: "owned-canonical-fixture"}
	dispatcher := PipelineDispatcher{Server: server, Key: key}
	frozen, err := dispatcher.signCommand(dep, *definition, []byte("original accepted closure"), json.RawMessage(`{}`), "kubernetes", nil, nil, "")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now(),deadline=now()+interval '24 hours' WHERE deployment_id=$1`, dep.Id)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, dep.Id, frozen)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, store.ReserveRunnerDispatch(ctx, dep, 1))
	replay, err := dispatcher.frozenCommand(ctx, dep)
	require.NoError(t, err)
	require.Equal(t, frozen, replay, "redelivery must reuse durable bytes without rebuilding inputs or consulting Core")
	progress := model.PipelineProgress{ID: dep.Id.String(), EnvironmentID: dep.DeploymentEnvUuid.String(), Phase: "post_failed", Revision: 3, InfrastructureApplied: true}
	payload, err := json.Marshal(map[string]any{"id": 4, "progress": progress})
	require.NoError(t, err)
	event := hmessaging.EventEnvelope{OrganizationID: dep.OrgId, RunnerID: dep.RunnerId, DeploymentID: dep.Id.String(), Payload: payload}
	wrongSite := event
	wrongSite.RunnerID = "different-site"
	require.ErrorIs(t, server.ApplyPipelineProgressEnvelope(ctx, wrongSite), ErrRunnerEventTargetInvalid)
	require.NoError(t, server.ApplyPipelineProgressEnvelope(ctx, event))
	require.NoError(t, server.ApplyPipelineProgressEnvelope(ctx, event))
	saved, _, _, _, err := store.GetDeployment(ctx, nil, dep.OrgId, dep.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, model.DeploymentStatusExecuting, saved.Status, "post failure does not settle infrastructure")
	summaries, err := server.pipelineSummaries(ctx, []uuid.UUID{dep.Id, uuid.New()})
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	require.Equal(t, "post_failed", summaries[dep.Id].Phase)
	require.True(t, summaries[dep.Id].InfrastructureApplied)
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	origin, err := readDeploymentPluginOrigin(ctx, tx, dep.OrgId, dep.Id)
	require.NoError(t, err)
	require.Nil(t, origin)
	operation := uuid.NewString()
	_, err = model.CreateExternalOperationDeploymentReceipt(ctx, tx, model.ExternalOperationDeploymentReceipt{
		ExternalOperationDeploymentKey: model.ExternalOperationDeploymentKey{OrgID: dep.OrgId, PluginID: "dev.example.rollouts", RequestID: "original-dispatch"},
		OperationRef:                   operation,
		Fence:                          1,
		AssertionAction:                "external-deployment.admit",
		ServiceAction:                  "external-deployment.admit",
		RequestBodySHA256:              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ActorID:                        dep.CreatedBy.String(),
		ProjectID:                      dep.ProjectId,
		EnvID:                          dep.EnvId,
		DeploymentID:                   dep.Id,
	})
	require.NoError(t, err)
	origin, err = readDeploymentPluginOrigin(ctx, tx, dep.OrgId, dep.Id)
	require.NoError(t, err)
	require.Equal(t, "dev.example.rollouts", origin.PluginID)
	require.Equal(t, operation, *origin.OperationRef)
	require.Nil(t, origin.ObservationRunID)
	other, err := readDeploymentPluginOrigin(ctx, tx, "other-org", dep.Id)
	require.NoError(t, err)
	require.Nil(t, other)
	require.NoError(t, tx.Commit())
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `ALTER TABLE deployments RENAME TO deployments_temporarily_unavailable`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	err = server.ApplyPipelineProgressEnvelope(ctx, event)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrRunnerEventTargetInvalid, "database read failure must retry instead of dead-lettering progress")
}

func TestNativeObservationReplaysFrozenBytesWithoutPipelineMetadata(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	store := deploymentAdmissionTestDatabase(t, t.Context(), connection)
	tx, err := store.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	dep, err := store.CreateDeployment(
		t.Context(),
		tx,
		"observer-org",
		"project",
		"env",
		model.CreateDeploymentParams{
			DeploymentEnvUuid: uuid.New(),
			Mode:              model.DeploymentModeObservation,
			CreatedBy:         uuid.New(),
			RunnerId:          "site",
			RunnerLogLevel:    "info",
			Manifest:          model.EncodedDeploymentManifest(`{}`),
			Graph:             model.EncodedDeploymentGraph(`{}`),
			Tofu:              model.RawTofu("terraform {}"),
		},
	)
	require.NoError(t, err)
	base, source, err := commandPlanAuthority(t.Context(), tx, dep)
	require.NoError(t, err, "dedicated observation must not require a deployment pipeline row")
	require.Nil(t, base)
	require.Nil(t, source)
	mutation := *dep
	mutation.Mode = model.DeploymentModeDeploy
	_, _, err = commandPlanAuthority(t.Context(), tx, &mutation)
	require.ErrorIs(t, err, sql.ErrNoRows, "ordinary mutation still requires frozen pipeline authority")
	frozen := []byte(`{"immutable":"read-only command"}`)
	_, err = tx.ExecContext(t.Context(), `UPDATE deployment_dispatch_budgets SET observation_command=$2 WHERE deployment_id=$1`, dep.Id, frozen)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	dispatcher := PipelineDispatcher{Server: &Server{Database: store}}
	replay, err := dispatcher.frozenObservationCommand(t.Context(), dep)
	require.NoError(t, err)
	require.Equal(t, frozen, replay)
	saved, _, _, _, err := store.GetDeployment(t.Context(), nil, dep.OrgId, dep.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.True(t, saved.HasNativeObservation)
	require.False(t, saved.HasPipeline)
}
