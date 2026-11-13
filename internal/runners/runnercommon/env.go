package runnercommon

import (
	"iter"
	"strconv"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

type GatewayConfiguration struct {
	URL             string
	RunnerTokenSalt string
	// CostEstimation is transient dispatch material, never deployment model data.
	CostEstimation *CostEstimationConfiguration
}

type CostEstimationConfiguration struct {
	Enabled bool
	APIKey  string
}

func GenerateEnvVarsForRun(metadataOutputKey string, d *model.DeploymentSummary, gatewayConfigurations ...GatewayConfiguration) iter.Seq2[string, string] {
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
		for k, v := range CostPolicyEnv(d, gatewayConfigurations...) {
			yield(k, v)
		}
		if d.Mode == model.DeploymentModeObservation {
			yield(ObservationProtocolVariable, ObservationProtocolV1)
		}
		if d.EncryptedOutputsRecipient.IsSet() {
			yield("ENCRYPTING_KEY", d.EncryptedOutputsRecipient.Must())
		}
		if d.EncryptedLogsRecipient.IsSet() {
			yield("ENCRYPTING_LOGS_KEY", d.EncryptedLogsRecipient.Must())
		}
		yield("LOG_LEVEL", d.RunnerLogLevel)
		if len(gatewayConfigurations) > 0 && gatewayConfigurations[0].URL != "" {
			gatewayConfig := gatewayConfigurations[0]
			yield("RUNNER_GATEWAY_URL", gatewayConfig.URL)
			yield("TOKEN", util.GenerateHashedRunnerToken(gatewayConfig.RunnerTokenSalt, d.OrgId, d.Id.String()))
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
	case model.DeploymentModeObservation:
		return string(model.DeploymentModeDeployPlan)
	case model.DeploymentModeRollback:
		return string(model.DeploymentModeDeploy)
	case model.DeploymentModeRollbackPlan:
		return string(model.DeploymentModeDeployPlan)
	default:
		return string(deploymentMode)
	}
}

const CostEstimationEnabledVariable = "COST_ESTIMATION_ENABLED"
const InfracostAPIKeyVariable = "INFRACOST_API_KEY" // #nosec G101 -- Environment variable name, not a credential.

func IsCostPolicyVariable(name string) bool {
	return name == CostEstimationEnabledVariable || name == InfracostAPIKeyVariable
}

func CostPolicyEnv(d *model.DeploymentSummary, configurations ...GatewayConfiguration) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		if d.CostPolicy == nil {
			return
		}
		var cfg CostEstimationConfiguration
		if len(configurations) > 0 && configurations[0].CostEstimation != nil {
			cfg = *configurations[0].CostEstimation
		}
		enabled := d.CostPolicy.Enabled && cfg.Enabled && cfg.APIKey != ""
		key := ""
		if enabled {
			key = cfg.APIKey
		}
		yield(CostEstimationEnabledVariable, strconv.FormatBool(enabled))
		yield(InfracostAPIKeyVariable, key)
	}
}
