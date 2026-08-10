package runnercommon

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func collectEnvVars(metadataOutputKey string, d *model.DeploymentSummary) map[string]string {
	result := map[string]string{}
	for k, v := range GenerateEnvVarsForRun(metadataOutputKey, d) {
		result[k] = v
	}
	return result
}

func TestGenerateEnvVarsForRun_IncludesMetadataKey(t *testing.T) {
	d := &model.DeploymentSummary{
		OrgId: "org-1",
		Id:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Mode:  model.DeploymentModeDeploy,
	}
	envVars := collectEnvVars("my-meta-key", d)
	assert.Equal(t, "my-meta-key", envVars["METADATA_KEY"])
}

func TestGenerateEnvVarsForRun_OmitsMetadataKeyWhenEmpty(t *testing.T) {
	d := &model.DeploymentSummary{
		OrgId: "org-1",
		Id:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Mode:  model.DeploymentModeDeploy,
	}
	envVars := collectEnvVars("", d)
	_, ok := envVars["METADATA_KEY"]
	assert.False(t, ok, "METADATA_KEY should not be present when metadataOutputKey is empty")
}

func TestGenerateEnvVarsForRun_IncludesNATSRunnerContract(t *testing.T) {
	environmentID := uuid.New()
	deploymentID := uuid.New()
	deployment := &model.DeploymentSummary{
		OrgId: "org-1", RunnerId: "runner-1", DeploymentEnvUuid: environmentID,
		Id: deploymentID, Mode: model.DeploymentModeDeploy,
	}
	environment := map[string]string{}
	for key, value := range GenerateEnvVarsForRun("", deployment, NATSConfiguration{URL: "nats://broker:4222", Token: "runner-token"}) {
		environment[key] = value
	}
	assert.Equal(t, "runner-1", environment["RUNNER_ID"])
	assert.Equal(t, environmentID.String(), environment["DEPLOYMENT_ENV_UUID"])
	assert.Equal(t, "nats://broker:4222", environment["NATS_URL"])
	assert.Equal(t, "runner-token", environment["NATS_TOKEN"])
	assert.Equal(t, "PO_RUNNER_BUNDLES", environment["NATS_BUNDLE_BUCKET"])
	assert.Equal(t, "org-1/"+deploymentID.String(), environment["NATS_BUNDLE_KEY"])
	for _, obsolete := range []string{"TOKEN", "LOGS_URL", "PLATFORM_ORCHESTRATOR_BASE_URL", "PLATFORM_ORCHESTRATOR_API_PREFIX"} {
		assert.NotContains(t, environment, obsolete)
	}
}
