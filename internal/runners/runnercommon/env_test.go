package runnercommon

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func collectEnvVars(externalDataplaneUrl, runnerToken, runnerLogsSignedUrl, metadataOutputKey string, d *model.DeploymentSummary) map[string]string {
	result := map[string]string{}
	for k, v := range GenerateEnvVarsForRun(externalDataplaneUrl, runnerToken, runnerLogsSignedUrl, metadataOutputKey, d) {
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
	envVars := collectEnvVars("https://dp", "token", "", "my-meta-key", d)
	assert.Equal(t, "my-meta-key", envVars["METADATA_KEY"])
}

func TestGenerateEnvVarsForRun_OmitsMetadataKeyWhenEmpty(t *testing.T) {
	d := &model.DeploymentSummary{
		OrgId: "org-1",
		Id:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Mode:  model.DeploymentModeDeploy,
	}
	envVars := collectEnvVars("https://dp", "token", "", "", d)
	_, ok := envVars["METADATA_KEY"]
	assert.False(t, ok, "METADATA_KEY should not be present when metadataOutputKey is empty")
}
