package kubernetes

import (
	"context"
	"testing"

	model "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

const runnerLogsBucketSignedUrl = "https://storage.googleapis.com/platform-orchestrator-runner-logs/token"

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
		"https://external-dp",
		"runner-image:latest",
		"token-abc",
		runnerLogsBucketSignedUrl,
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
	assert.Equal(t, "TOKEN", container.Env[3].Name)
	assert.Equal(t, "token-abc", container.Env[3].Value)
	assert.Equal(t, "ENCRYPTING_KEY", container.Env[4].Name)
	assert.Equal(t, "outputs-key", container.Env[4].Value)
	assert.Equal(t, "ENCRYPTING_LOGS_KEY", container.Env[5].Name)
	assert.Equal(t, "logs-key", container.Env[5].Value)
	assert.Equal(t, "PLATFORM_ORCHESTRATOR_BASE_URL", container.Env[6].Name)
	assert.Equal(t, "https://external-dp", container.Env[6].Value)
	assert.Equal(t, "PLATFORM_ORCHESTRATOR_API_PREFIX", container.Env[7].Name)
	assert.Equal(t, "https://external-dp", container.Env[7].Value)
	assert.Equal(t, "LOGS_URL", container.Env[8].Name)
	assert.Equal(t, runnerLogsBucketSignedUrl, container.Env[8].Value)
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
		"https://external-dp",
		"runner-image:latest",
		"token-abc",
		"",
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
		"https://external-dp",
		"runner-image:latest",
		"token-abc",
		"",
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
		"https://external-dp",
		"runner-image:latest",
		"token-abc",
		"",
		"",
		d,
	)
	require.NoError(t, err)
	assert.Equal(t, corev1.RestartPolicyAlways, spec.Template.Spec.RestartPolicy)

	container := spec.Template.Spec.Containers[0]
	assert.Equal(t, "LOGS_URL", container.Env[8].Name)
	assert.Empty(t, container.Env[8].Value)
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
		"https://external-dp",
		"runner-image:latest",
		"token-abc",
		runnerLogsBucketSignedUrl,
		"",
		d,
	)
	require.Error(t, err)
}
