package kubernetes

import (
	"context"
	"testing"

	model "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/runnercommon"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
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
