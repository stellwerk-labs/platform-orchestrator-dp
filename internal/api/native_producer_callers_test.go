package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	p "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func TestNativePipelineNegotiatesV2WithoutLegacyProtocol(t *testing.T) {
	runner := qualifiedNativeRunner(t)
	for _, mode := range []model.DeploymentMode{model.DeploymentModeDeploy, model.DeploymentModeDeployPlan, model.DeploymentModeRollback, model.DeploymentModeRollbackPlan, model.DeploymentModeDestroy} {
		definition, err := pipelineForRunner(runner, mode, nil)
		require.NoError(t, err)
		if mode == model.DeploymentModeDeploy {
			require.Equal(t, "apply", definition.Strategy)
			require.Empty(t, definition.Steps)
		}
		expected, err := model.ResolvePipeline(nil, mode)
		require.NoError(t, err)
		require.Equal(t, expected, definition)
	}
	definition, err := publishedPipeline(publishedRecipeFixture(t), model.DeploymentModeDeploy)
	require.NoError(t, err)
	pinned, err := pipelineForRunner(runner, model.DeploymentModeDeploy, definition)
	require.NoError(t, err)
	require.Contains(t, pinned.RequiredCapabilities, "database.local")
	config, err := runner.RunnerConfiguration.AsNativeAgentRunnerConfiguration()
	require.NoError(t, err)
	config.NativeExecution.Tools = nil
	require.NoError(t, runner.RunnerConfiguration.FromNativeAgentRunnerConfiguration(config))
	_, err = pipelineForRunner(runner, model.DeploymentModeDeploy, definition)
	require.ErrorContains(t, err, "not installed")
}

func TestNativeFirstEligibilityIsExplicitAndServerOwned(t *testing.T) {
	no, yes := false, true
	for _, eligible := range []*bool{nil, &no} {
		require.ErrorContains(t, requireNativeFirstEnvironment(cp.Environment{NativeFirstEligible: eligible}), "not eligible")
	}
	require.NoError(t, requireNativeFirstEnvironment(cp.Environment{NativeFirstEligible: &yes}))
}

func TestNativeManualPlanOptionsRequireAnOrdinaryAttemptOnlyForExecution(t *testing.T) {
	project, environment, actor := uuid.New(), uuid.New(), uuid.New()
	server := &Server{DeploymentAdmissions: &OrdinaryDeploymentAdmissionService{}}
	eligible := true
	env := cp.Environment{Uuid: environment, ProjectUuid: &project, NativeFirstEligible: &eligible}
	request := deploymentRequest{NativeTarget: nativeTargetFixture()}
	for _, mode := range []model.DeploymentMode{model.DeploymentModeDeployPlan, model.DeploymentModeRollbackPlan} {
		options := server.deploymentOptions(context.Background(), actor, "org", env, mode, request, "plan-key", false)
		require.NotNil(t, options.Admission)
		require.Equal(t, actor, options.Admission.ActorID)
		require.Equal(t, environment, options.Admission.EnvironmentUUID)
		require.Equal(t, mode, options.Admission.Mode)
		require.Nil(t, server.deploymentOptions(context.Background(), actor, "org", env, mode, request, "preview", true).Admission)
		require.Nil(t, server.deploymentOptions(context.Background(), actor, "org", env, mode, deploymentRequest{}, "legacy", false).Admission)
	}
}

func TestNativeProducerPublishedDefaultAndExplicitSelectionWithoutLegacyAdapter(t *testing.T) {
	runner := qualifiedNativeRunner(t)
	eligible := true
	env := cp.Environment{Id: "environment", ProjectId: "project", RunnerId: &runner.Id, NativeFirstEligible: &eligible}
	version := publishedRecipeFixture(t)
	choices := p.PipelineChoices{Policy: p.PipelinePolicy{Enabled: true, DefaultVersionId: &version.Id}, Versions: []p.PublishedPipelineVersion{version}}
	var reads atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := any(choices)
		if r.URL.Path == "/orgs/org/runners/native_pool" {
			reads.Add(1)
			payload = runner
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Errorf("unencodable Native producer response: %v", err)
			http.Error(w, "response encoding failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(append(raw, '\n')); err != nil {
			t.Errorf("Native producer response write failed: %v", err)
			return
		}
	}))
	defer peer.Close()
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	policies, err := p.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	server := &Server{ControlPlaneClient: client, PipelineClient: policies}
	for _, selection := range []*DeploymentPipelineSelection{nil, {Id: version.Id.String(), Version: version.Version}} {
		before := reads.Load()
		definition, target, resolved, err := server.resolveRequestedPipelineForRequest(context.Background(), uuid.New(), "org", &env, model.DeploymentModeDeploy, selection, nil)
		require.NoError(t, err)
		require.Equal(t, before+1, reads.Load())
		require.Equal(t, version.Id.String(), definition.ID)
		require.Equal(t, version.Digest, definition.Digest)
		require.Equal(t, *runner.Uuid, target.RunnerUuid)
		require.Equal(t, runner.Id, resolved.Id)
	}
}
