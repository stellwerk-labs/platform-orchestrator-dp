package kubernetes

import (
	"context"
	"strconv"
	"testing"

	model "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/runnercommon"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestGetJobSpec_DefaultValues(t *testing.T) {
	jobConfig := platformorchestratorcp.K8sRunnerJobConfig{
		ServiceAccount: "test-sa",
	}
	d := &model.DeploymentSummary{
		OrgId:                     "org-123",
		Id:                        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Mode:                      model.DeploymentModeDeploy,
		EncryptedOutputsRecipient: opt.Of("outputs-key"),
		EncryptedLogsRecipient:    opt.Of("logs-key"),
	}
	spec, err := GetJobSpec(
		context.Background(),
		jobConfig,
		"runner-image:latest",
		"",
		d,
	)
	require.NoError(t, err, "unexpected error")
	assert.Equal(t, "test-sa", spec.Template.Spec.ServiceAccountName, "ServiceAccountName mismatch")
	assert.Len(t, spec.Template.Spec.Containers, 1, "container count mismatch")
	container := spec.Template.Spec.Containers[0]
	assert.Equal(t, "runner-image:latest", container.Image, "container image mismatch")
	assert.Equal(t, "ORG_ID", container.Env[0].Name)
	assert.Equal(t, "org-123", container.Env[0].Value)
	environment := map[string]string{}
	for _, env := range container.Env {
		environment[env.Name] = env.Value
	}
	assert.Equal(t, "outputs-key", environment["ENCRYPTING_KEY"])
	assert.Equal(t, "logs-key", environment["ENCRYPTING_LOGS_KEY"])
	for _, obsolete := range []string{"TOKEN", "LOGS_URL", "PLATFORM_ORCHESTRATOR_BASE_URL", "PLATFORM_ORCHESTRATOR_API_PREFIX"} {
		assert.NotContains(t, environment, obsolete)
	}
}

func TestGetJobSpec_WithMetadataOutputKey(t *testing.T) {
	jobConfig := platformorchestratorcp.K8sRunnerJobConfig{
		ServiceAccount: "test-sa",
	}
	d := &model.DeploymentSummary{
		OrgId: "org-123",
		Id:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Mode:  model.DeploymentModeDeploy,
	}
	spec, err := GetJobSpec(
		context.Background(),
		jobConfig,
		"runner-image:latest",
		"my-metadata-key",
		d,
	)
	require.NoError(t, err)

	envVars := map[string]string{}
	for _, e := range spec.Template.Spec.Containers[0].Env {
		envVars[e.Name] = e.Value
	}
	assert.Equal(t, "my-metadata-key", envVars["METADATA_KEY"])
}

func TestGetJobSpec_WithGatewayConfiguration(t *testing.T) {
	deployment := &model.DeploymentSummary{
		OrgId: "org-123",
		Id:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Mode:  model.DeploymentModeDeploy,
	}
	spec, err := GetJobSpec(
		context.Background(),
		platformorchestratorcp.K8sRunnerJobConfig{ServiceAccount: "test-sa"},
		"runner-image:latest",
		"",
		deployment,
		runnercommon.GatewayConfiguration{URL: "https://gateway.example.test", RunnerTokenSalt: "salt"},
	)
	require.NoError(t, err)

	environment := map[string]string{}
	for _, env := range spec.Template.Spec.Containers[0].Env {
		environment[env.Name] = env.Value
	}
	assert.Equal(t, "https://gateway.example.test", environment["RUNNER_GATEWAY_URL"])
	assert.NotEmpty(t, environment["TOKEN"])
	assert.NotContains(t, environment, "NATS_URL")
}

func TestGetJobSpec_OmitsMetadataKeyWhenEmpty(t *testing.T) {
	jobConfig := platformorchestratorcp.K8sRunnerJobConfig{
		ServiceAccount: "test-sa",
	}
	d := &model.DeploymentSummary{
		OrgId: "org-123",
		Id:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Mode:  model.DeploymentModeDeploy,
	}
	spec, err := GetJobSpec(
		context.Background(),
		jobConfig,
		"runner-image:latest",
		"",
		d,
	)
	require.NoError(t, err)

	for _, e := range spec.Template.Spec.Containers[0].Env {
		assert.NotEqual(t, "METADATA_KEY", e.Name, "METADATA_KEY should not be present when metadataOutputKey is empty")
	}
}

func TestGetJobSpec_WithPodTemplatePatch(t *testing.T) {
	jobConfig := platformorchestratorcp.K8sRunnerJobConfig{
		ServiceAccount: "test-sa",
		PodTemplate: &map[string]interface{}{
			"spec": map[string]interface{}{
				"restartPolicy": "Always",
			},
		},
	}
	d := &model.DeploymentSummary{
		OrgId:                     "org-123",
		Id:                        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Mode:                      model.DeploymentModeDeploy,
		EncryptedOutputsRecipient: opt.Of("outputs-key"),
		EncryptedLogsRecipient:    opt.Of("logs-key"),
	}
	spec, err := GetJobSpec(
		context.Background(),
		jobConfig,
		"runner-image:latest",
		"",
		d,
	)
	require.NoError(t, err)
	assert.Equal(t, corev1.RestartPolicyAlways, spec.Template.Spec.RestartPolicy)

	environment := map[string]string{}
	for _, env := range spec.Template.Spec.Containers[0].Env {
		environment[env.Name] = env.Value
	}
	assert.NotContains(t, environment, "LOGS_URL")
}

func TestGetJobSpec_InvalidPodTemplatePatch(t *testing.T) {
	jobConfig := platformorchestratorcp.K8sRunnerJobConfig{
		ServiceAccount: "test-sa",
		PodTemplate: &map[string]interface{}{
			"spec": map[string]interface{}{
				"containers": "not-a-list",
			},
		},
	}
	d := &model.DeploymentSummary{
		OrgId:                     "org-123",
		Id:                        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Mode:                      model.DeploymentModeDeploy,
		EncryptedOutputsRecipient: opt.Of("outputs-key"),
		EncryptedLogsRecipient:    opt.Of("logs-key"),
	}
	_, err := GetJobSpec(
		context.Background(),
		jobConfig,
		"runner-image:latest",
		"",
		d,
	)
	require.Error(t, err)
}

func TestObservationPodTemplateCannotOverrideReadOnlyTransport(t *testing.T) {
	for _, test := range []struct {
		name    string
		patch   map[string]interface{}
		allowed bool
	}{
		{name: "default", allowed: true},
		{name: "resources", patch: map[string]interface{}{"name": "main", "resources": map[string]interface{}{"requests": map[string]interface{}{"memory": "512Mi"}}}, allowed: true},
		{name: "deploy", patch: map[string]interface{}{"name": "main", "env": []interface{}{map[string]interface{}{"name": "MODE", "value": "deploy"}}}},
		{name: "wrong protocol", patch: map[string]interface{}{"name": "main", "env": []interface{}{map[string]interface{}{"name": "OBSERVATION_PROTOCOL", "value": "unknown"}}}},
		{name: "removed marker", patch: map[string]interface{}{"name": "main", "env": []interface{}{map[string]interface{}{"name": "OBSERVATION_PROTOCOL", "$patch": "delete"}}}},
		{name: "custom command", patch: map[string]interface{}{"name": "main", "command": []interface{}{"terraform", "apply"}}},
		{name: "custom args", patch: map[string]interface{}{"name": "main", "args": []interface{}{"apply"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := platformorchestratorcp.K8sRunnerJobConfig{ServiceAccount: "default"}
			if test.patch != nil {
				config.PodTemplate = &map[string]interface{}{"spec": map[string]interface{}{"containers": []interface{}{test.patch}}}
			}
			spec, err := GetJobSpec(t.Context(), config, "runner-image", "", &model.DeploymentSummary{Mode: model.DeploymentModeObservation})
			if test.allowed {
				require.NoError(t, err)
				values := map[string]string{}
				for _, env := range spec.Template.Spec.Containers[0].Env {
					values[env.Name] = env.Value
				}
				require.Equal(t, "plan_only", values["MODE"])
				require.Equal(t, "environment-observation/v1", values["OBSERVATION_PROTOCOL"])
			} else {
				require.Error(t, err)
				require.Nil(t, spec)
			}
		})
	}
}

func TestCostPolicyOverridesPodTemplateCostEnvironmentOnly(t *testing.T) {
	for _, key := range []string{"", "fixture-key"} {
		template := map[string]interface{}{"spec": map[string]interface{}{"containers": []interface{}{map[string]interface{}{"name": RunnerContainerName, "envFrom": []interface{}{map[string]interface{}{"secretRef": map[string]interface{}{"name": "override"}}}, "env": []interface{}{
			map[string]interface{}{"name": runnercommon.CostEstimationEnabledVariable, "value": "true"},
			map[string]interface{}{"name": runnercommon.InfracostAPIKeyVariable, "valueFrom": map[string]interface{}{"secretKeyRef": map[string]interface{}{"name": "override", "key": "api-key"}}},
			map[string]interface{}{"name": "TF_LOG", "value": "TRACE"},
		}}}}}
		dep := &model.DeploymentSummary{CostPolicy: &model.DeploymentCostPolicy{Enabled: true}}
		cfg := runnercommon.GatewayConfiguration{CostEstimation: &runnercommon.CostEstimationConfiguration{Enabled: true, APIKey: key}}
		spec, err := GetJobSpec(t.Context(), platformorchestratorcp.K8sRunnerJobConfig{PodTemplate: &template}, "runner:test", "", dep, cfg)
		require.NoError(t, err)
		values := map[string]string{}
		for _, env := range spec.Template.Spec.Containers[0].Env {
			if runnercommon.IsCostPolicyVariable(env.Name) {
				require.Nil(t, env.ValueFrom)
				require.NotContains(t, values, env.Name)
			}
			values[env.Name] = env.Value
		}
		require.Equal(t, strconv.FormatBool(key != ""), values[runnercommon.CostEstimationEnabledVariable])
		require.Contains(t, values, runnercommon.InfracostAPIKeyVariable)
		require.Equal(t, key, values[runnercommon.InfracostAPIKeyVariable])
		require.Equal(t, "TRACE", values["TF_LOG"])
		require.Len(t, spec.Template.Spec.Containers[0].EnvFrom, 1)
	}
}

func TestCostAPIKeyExcludedFromMalformedPodTemplateErrors(t *testing.T) {
	template := map[string]interface{}{"spec": map[string]interface{}{"containers": []interface{}{map[string]interface{}{"name": RunnerContainerName, "env": []interface{}{"invalid-scalar"}}}}}
	dep := &model.DeploymentSummary{CostPolicy: &model.DeploymentCostPolicy{Enabled: true}}
	cfg := runnercommon.GatewayConfiguration{CostEstimation: &runnercommon.CostEstimationConfiguration{Enabled: true, APIKey: "private-fixture-key-never-in-error"}}
	_, err := GetJobSpec(t.Context(), platformorchestratorcp.K8sRunnerJobConfig{PodTemplate: &template}, "runner:test", "", dep, cfg)
	require.Error(t, err)
	require.NotContains(t, err.Error(), cfg.CostEstimation.APIKey)
	require.NotContains(t, err.Error(), runnercommon.InfracostAPIKeyVariable)
}
