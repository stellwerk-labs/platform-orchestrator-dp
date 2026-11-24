package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	p "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func qualifiedNativeRunner(t *testing.T) cp.Runner {
	t.Helper()
	id := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	protocols := []string{"stellwerk.pipeline/v2", "stellwerk.pipeline.plan-apply/v2"}
	capabilities := []string{"pipeline.recipe.v1", "local.exec", "artifacts.read", "database.local"}
	tools := []cp.RunnerExecutionTool{}
	for _, tool := range p.TrustedTools() {
		retry := cp.RunnerExecutionToolRetryReadOnly
		if tool.Phase == "post_apply" {
			retry = cp.RunnerExecutionToolRetryReceipt
		}
		tools = append(tools, cp.RunnerExecutionTool{Id: tool.Id, Name: tool.Name, Phase: cp.RunnerExecutionToolPhase(tool.Phase), Retry: retry, RequiredCapabilities: tool.RequiredCapabilities})
	}
	config := cp.NativeAgentRunnerConfiguration{
		Type:               "native-agent",
		Key:                "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAc5dgCx4ano39JT0XgTsHnts3jej+5xl7ZAwSIrKpef0=\n-----END PUBLIC KEY-----",
		MaxParallelJobs:    2,
		SupportedProtocols: &protocols,
		Capabilities:       &capabilities,
		NativeExecution:    &cp.RunnerExecutionCatalogue{ContractVersion: cp.StellwerkRunnerCataloguev1, RuntimeVersion: cp.StellwerkRunnerRolloutv2, ToolsDigest: strings.Repeat("a", 64), Tools: tools},
	}
	runner := cp.Runner{OrgId: "org", Id: "native_pool", Uuid: &id}
	require.NoError(t, runner.RunnerConfiguration.FromNativeAgentRunnerConfiguration(config))
	require.NoError(t, runner.StateStorageConfiguration.FromS3StorageConfiguration(cp.S3StorageConfiguration{Bucket: "state-bucket"}))
	return runner
}

func TestNativeObserverQualificationUsesTheDedicatedProtocolWithoutRecipeTools(t *testing.T) {
	runner := qualifiedNativeRunner(t)
	config, err := runner.RunnerConfiguration.AsNativeAgentRunnerConfiguration()
	require.NoError(t, err)
	protocols := append(*config.SupportedProtocols, "environment-observation/v1")
	capabilities := []string{}
	config.SupportedProtocols, config.Capabilities = &protocols, &capabilities
	config.NativeExecution.Tools = []cp.RunnerExecutionTool{}
	require.NoError(t, runner.RunnerConfiguration.FromNativeAgentRunnerConfiguration(config))
	_, err = nativeExecutionTarget(runner.OrgId, runner.Id, runner, model.DeploymentModeObservation, nil)
	require.ErrorContains(t, err, "dedicated read-only observer")
	capabilities = []string{"environment-observation/v1"}
	config.Capabilities = &capabilities
	require.NoError(t, runner.RunnerConfiguration.FromNativeAgentRunnerConfiguration(config))
	target, err := nativeExecutionTarget(runner.OrgId, runner.Id, runner, model.DeploymentModeObservation, nil)
	require.NoError(t, err)
	require.NotNil(t, target)
	require.Equal(t, *runner.Uuid, target.RunnerUuid)
	require.Equal(t, cp.NativeTargetPipelineV2, target.PipelineVersion)
	definition, err := model.ResolvePipeline(nil, model.DeploymentModeDeploy)
	require.NoError(t, err)
	_, err = nativeExecutionTarget(runner.OrgId, runner.Id, runner, model.DeploymentModeObservation, definition)
	require.ErrorContains(t, err, "without a customer recipe")
	config.SupportedProtocols = &[]string{"stellwerk.pipeline/v2", "stellwerk.pipeline.plan-apply/v2"}
	require.NoError(t, runner.RunnerConfiguration.FromNativeAgentRunnerConfiguration(config))
	_, err = nativeExecutionTarget(runner.OrgId, runner.Id, runner, model.DeploymentModeObservation, nil)
	require.ErrorContains(t, err, "dedicated read-only observer")
}

func TestNativeQualificationBindsOnePublicSnapshotAndExcludesMutablePolicy(t *testing.T) {
	runner := qualifiedNativeRunner(t)
	definition, err := publishedPipeline(publishedRecipeFixture(t), model.DeploymentModeDeploy)
	require.NoError(t, err)
	before, err := json.Marshal(definition)
	require.NoError(t, err)
	target, err := nativeExecutionTarget("org", runner.Id, runner, model.DeploymentModeDeploy, definition)
	require.NoError(t, err)
	raw, err := json.Marshal(runner.StateStorageConfiguration)
	require.NoError(t, err)
	kind, digest, err := cp.StateStorageConfigurationDigest(raw)
	require.NoError(t, err)
	require.Equal(t, &cp.NativeExecutionTargetReference{RunnerId: runner.Id, RunnerUuid: *runner.Uuid, RuntimeVersion: cp.NativeTargetRuntimeV2, PipelineVersion: cp.NativeTargetPipelineV2, ToolsDigest: strings.Repeat("a", 64), StateBackendType: kind, StateConfigurationDigest: digest}, target)
	after, err := json.Marshal(definition)
	require.NoError(t, err)
	require.Equal(t, before, after, "qualification must not mutate the frozen recipe")
	config, err := runner.RunnerConfiguration.AsNativeAgentRunnerConfiguration()
	require.NoError(t, err)
	config.MaxParallelJobs = 1
	config.Key = cp.K8sAgentRunnerKey(strings.Replace(string(config.Key), "Ac5dg", "Ab5dg", 1))
	require.NoError(t, runner.RunnerConfiguration.FromNativeAgentRunnerConfiguration(config))
	description := "new display"
	runner.Description = &description
	again, err := nativeExecutionTarget("org", runner.Id, runner, model.DeploymentModeDeploy, definition)
	require.NoError(t, err)
	require.Equal(t, target, again)
	for _, id := range []string{"p_", strings.Repeat("a", 128)} {
		r := qualifiedNativeRunner(t)
		r.Id = id
		target, err := nativeExecutionTarget("org", id, r, model.DeploymentModeDeploy, nil)
		require.NoError(t, err)
		require.Equal(t, id, target.RunnerId)
	}
	tooLong := qualifiedNativeRunner(t)
	tooLong.Id = strings.Repeat("a", 129)
	_, err = nativeExecutionTarget("org", tooLong.Id, tooLong, model.DeploymentModeDeploy, nil)
	require.Error(t, err)
	var legacy cp.Runner
	got, err := nativeExecutionTarget("org", "legacy", legacy, model.DeploymentModeDeploy, nil)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestNativeQualificationRefusesIncompleteAndIncompatibleInventory(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*cp.Runner, *cp.NativeAgentRunnerConfiguration)
	}{
		{"organization", func(r *cp.Runner, _ *cp.NativeAgentRunnerConfiguration) { r.OrgId = "other" }},
		{"registration", func(r *cp.Runner, _ *cp.NativeAgentRunnerConfiguration) { r.Uuid = nil }},
		{"slug profile", func(r *cp.Runner, _ *cp.NativeAgentRunnerConfiguration) { r.Id = "a" }},
		{"disabled", func(_ *cp.Runner, c *cp.NativeAgentRunnerConfiguration) {
			c.NativeExecution = nil
			c.Capabilities = nil
			c.SupportedProtocols = nil
		}},
		{"capacity", func(_ *cp.Runner, c *cp.NativeAgentRunnerConfiguration) { c.MaxParallelJobs = 0 }},
		{"old runtime", func(_ *cp.Runner, c *cp.NativeAgentRunnerConfiguration) {
			c.NativeExecution.RuntimeVersion = cp.StellwerkRunnerRolloutv1
		}},
		{"bad digest", func(_ *cp.Runner, c *cp.NativeAgentRunnerConfiguration) {
			c.NativeExecution.ToolsDigest = "sha256:" + strings.Repeat("a", 64)
		}},
		{"old pipeline", func(_ *cp.Runner, c *cp.NativeAgentRunnerConfiguration) {
			v := []string{model.PipelineProtocol}
			c.SupportedProtocols = &v
		}},
		{"missing capability", func(_ *cp.Runner, c *cp.NativeAgentRunnerConfiguration) {
			v := []string{"pipeline.recipe.v1", "local.exec"}
			c.Capabilities = &v
		}},
		{"missing tool", func(_ *cp.Runner, c *cp.NativeAgentRunnerConfiguration) { c.NativeExecution.Tools = nil }},
		{"wrong phase", func(_ *cp.Runner, c *cp.NativeAgentRunnerConfiguration) {
			c.NativeExecution.Tools[0].Phase = "post_apply"
		}},
		{"unsafe retry", func(_ *cp.Runner, c *cp.NativeAgentRunnerConfiguration) {
			c.NativeExecution.Tools[0].Retry = cp.RunnerExecutionToolRetryUnsafe
		}},
		{"state target", func(r *cp.Runner, _ *cp.NativeAgentRunnerConfiguration) {
			require.NoError(t, json.Unmarshal([]byte(`{"type":"postgres"}`), &r.StateStorageConfiguration))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := qualifiedNativeRunner(t)
			c, err := r.RunnerConfiguration.AsNativeAgentRunnerConfiguration()
			require.NoError(t, err)
			test.change(&r, &c)
			require.NoError(t, r.RunnerConfiguration.FromNativeAgentRunnerConfiguration(c))
			d, err := model.ResolvePipeline(&model.PipelineSelection{ID: model.PipelineReviewed, Version: 1}, model.DeploymentModeDeploy)
			require.NoError(t, err)
			_, err = nativeExecutionTarget("org", "native_pool", r, model.DeploymentModeDeploy, d)
			require.Error(t, err)
		})
	}
	r := qualifiedNativeRunner(t)
	_, err := nativeExecutionTarget("org", r.Id, r, model.DeploymentMode("unsupported"), nil)
	require.Error(t, err)
	d, err := model.ResolvePipeline(nil, model.DeploymentModeDeploy)
	require.NoError(t, err)
	d.Cost = &model.PipelineCost{Enabled: true}
	_, err = nativeExecutionTarget("org", r.Id, r, model.DeploymentModeDeploy, d)
	require.ErrorContains(t, err, "cost estimation")
}
