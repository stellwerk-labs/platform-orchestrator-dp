package runners

import (
	"testing"
	"time"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/runnercommon"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestCostCredentialUnavailableDoesNotFailRunnerCreation(t *testing.T) {
	config := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, config.FromK8sAgentRunnerConfiguration(platformorchestratorcp.K8sAgentRunnerConfiguration{}))
	core, logs := observer.New(zap.WarnLevel)
	factory := NewDefaultRunnerFactory(zap.New(core), nil, nil, nil, nil, "runner:test", "", nil, time.Minute, time.Hour)
	dep := &model.DeploymentSummary{CostPolicy: &model.DeploymentCostPolicy{Enabled: true, PolicyRevision: 4}}
	runner, err := factory.CreateRunner(t.Context(), platformorchestratorcp.InternalRunner{RunnerConfiguration: *config}, dep)
	require.NoError(t, err)
	remote, ok := runner.(*RemoteKubernetesRunner)
	require.True(t, ok)
	values := map[string]string{}
	for k, v := range runnercommon.GenerateEnvVarsForRun("", dep, remote.gateway) {
		values[k] = v
	}
	require.Equal(t, "false", values[runnercommon.CostEstimationEnabledVariable])
	require.Empty(t, values[runnercommon.InfracostAPIKeyVariable])
	require.True(t, dep.CostPolicy.Enabled)
	require.EqualValues(t, 4, dep.CostPolicy.PolicyRevision)
	require.Equal(t, "cost estimation skipped: credential unavailable", logs.All()[0].Message)
	require.NotContains(t, logs.All()[0].ContextMap(), "error")
	require.NotContains(t, logs.All()[0].ContextMap(), "api_key")
}
