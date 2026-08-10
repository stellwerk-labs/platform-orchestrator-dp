package runnercommon

import (
	"iter"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

type NATSConfiguration struct {
	URL   string
	Token string
}

func GenerateEnvVarsForRun(metadataOutputKey string, d *model.DeploymentSummary, natsConfigurations ...NATSConfiguration) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		yield("ORG_ID", d.OrgId)
		if d.RunnerId != "" {
			yield("RUNNER_ID", d.RunnerId)
		}
		yield("DEPLOYMENT_ID", d.Id.String())
		if d.DeploymentEnvUuid != uuid.Nil {
			yield("DEPLOYMENT_ENV_UUID", d.DeploymentEnvUuid.String())
		}
		yield("MODE", RunnerModeForDeployment(d.Mode))
		if d.EncryptedOutputsRecipient.IsSet() {
			yield("ENCRYPTING_KEY", d.EncryptedOutputsRecipient.Must())
		}
		if d.EncryptedLogsRecipient.IsSet() {
			yield("ENCRYPTING_LOGS_KEY", d.EncryptedLogsRecipient.Must())
		}
		yield("LOG_LEVEL", d.RunnerLogLevel)
		if len(natsConfigurations) > 0 && natsConfigurations[0].URL != "" {
			natsConfig := natsConfigurations[0]
			for _, variable := range []struct{ key, value string }{
				{"NATS_URL", natsConfig.URL}, {"NATS_TOKEN", natsConfig.Token},
				{"NATS_BUNDLE_BUCKET", "PO_RUNNER_BUNDLES"}, {"NATS_BUNDLE_KEY", d.OrgId + "/" + d.Id.String()},
			} {
				key, value := variable.key, variable.value
				if value != "" {
					yield(key, value)
				}
			}
		}
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
