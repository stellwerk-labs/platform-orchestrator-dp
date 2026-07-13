package runnercommon

import (
	"iter"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func GenerateEnvVarsForRun(externalDataplaneUrl, runnerToken, runnerLogsSignedUrl, metadataOutputKey string, d *model.DeploymentSummary) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		yield("ORG_ID", d.OrgId)
		yield("DEPLOYMENT_ID", d.Id.String())
		yield("MODE", RunnerModeForDeployment(d.Mode))
		yield("TOKEN", runnerToken)
		if d.EncryptedOutputsRecipient.IsSet() {
			yield("ENCRYPTING_KEY", d.EncryptedOutputsRecipient.Must())
		}
		if d.EncryptedLogsRecipient.IsSet() {
			yield("ENCRYPTING_LOGS_KEY", d.EncryptedLogsRecipient.Must())
		}
		yield("PLATFORM_ORCHESTRATOR_BASE_URL", externalDataplaneUrl)
		yield("PLATFORM_ORCHESTRATOR_API_PREFIX", externalDataplaneUrl)
		yield("LOGS_URL", runnerLogsSignedUrl)
		yield("LOG_LEVEL", d.RunnerLogLevel)
		if metadataOutputKey != "" {
			yield("METADATA_KEY", metadataOutputKey)
		}
	}
}

// RunnerModeForDeployment returns the runner mode string supported by the runner image for the given deployment mode.
// This is used because there are more user-facing modes than internal runner modes since Terraform only technically
// supports deploy, plan, and destroy.
func RunnerModeForDeployment(deploymentMode model.DeploymentMode) string {
	switch deploymentMode {
	case model.DeploymentModeRollback:
		return string(model.DeploymentModeDeploy)
	case model.DeploymentModeRollbackPlan:
		return string(model.DeploymentModeDeployPlan)
	default:
		return string(deploymentMode)
	}
}
