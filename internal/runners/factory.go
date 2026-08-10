package runners

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/logging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/runnercommon"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

// DefaultRunnerFactory implements RunnerFactory interface
type DefaultRunnerFactory struct {
	awsTemporaryAuthProvider           AwsTemporaryAuthProvider
	k8sClusterClientFactory            kubernetes.ClientFactory
	vlt                                vault.VaultClientInterface
	runnerImage                        string
	metadataOutputKey                  string
	logger                             *zap.Logger
	remoteRunnerCommandPublisher       RemoteRunnerCommandPublisher
	kubernetesRunnerPodSchedulingDelay time.Duration
	runnerCommandTTL                   time.Duration
	natsConfiguration                  runnercommon.NATSConfiguration
}

func NewDefaultRunnerFactory(
	logger *zap.Logger,
	awsTemporaryAuthProvider AwsTemporaryAuthProvider,
	k8sClusterClientFactory kubernetes.ClientFactory,
	vlt vault.VaultClientInterface,
	runnerImage,
	metadataOutputKey string,
	remoteRunnerCommandPublisher RemoteRunnerCommandPublisher,
	kubernetesRunnerPodSchedulingDelay time.Duration,
	runnerCommandTTL time.Duration,
	natsConfigurations ...runnercommon.NATSConfiguration,
) *DefaultRunnerFactory {
	var natsConfiguration runnercommon.NATSConfiguration
	if len(natsConfigurations) > 0 {
		natsConfiguration = natsConfigurations[0]
	}
	return &DefaultRunnerFactory{
		logger:                             logger,
		awsTemporaryAuthProvider:           awsTemporaryAuthProvider,
		k8sClusterClientFactory:            k8sClusterClientFactory,
		vlt:                                vlt,
		runnerImage:                        runnerImage,
		metadataOutputKey:                  metadataOutputKey,
		remoteRunnerCommandPublisher:       remoteRunnerCommandPublisher,
		kubernetesRunnerPodSchedulingDelay: kubernetesRunnerPodSchedulingDelay,
		runnerCommandTTL:                   runnerCommandTTL,
		natsConfiguration:                  natsConfiguration,
	}
}

func (f *DefaultRunnerFactory) CreateRunner(ctx context.Context, internalRunner platformorchestratorcp.InternalRunner, deploymentSummary *model.DeploymentSummary) (RunnerInterface, error) {
	logger := f.logger.With(logging.ZapOrgId(deploymentSummary.OrgId), logging.ZapRunnerId(internalRunner.Id), logging.ZapDeploymentId(deploymentSummary.Id.String()))

	runnerType, err := internalRunner.RunnerConfiguration.Discriminator()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get runner type")
	}

	switch runnerType {
	case string(platformorchestratorcp.RunnerTypeKubernetesAgent):
		return NewRemoteKubernetesRunner(
			f.runnerImage,
			logger,
			internalRunner,
			deploymentSummary,
			f.remoteRunnerCommandPublisher,
			f.runnerCommandTTL,
		), nil
	case string(platformorchestratorcp.RunnerTypeKubernetes), string(platformorchestratorcp.RunnerTypeKubernetesGke), string(platformorchestratorcp.RunnerTypeKubernetesEks):
		return NewKubernetesRunner(
			f.k8sClusterClientFactory,
			f.vlt,
			f.runnerImage,
			f.metadataOutputKey,
			logger,
			internalRunner,
			deploymentSummary,
			f.kubernetesRunnerPodSchedulingDelay,
			f.natsConfiguration,
		), nil
	case string(platformorchestratorcp.RunnerTypeServerlessEcs):
		return &ecsRunnerInstance{
			Runner:                internalRunner,
			TemporaryAuthProvider: f.awsTemporaryAuthProvider,
			RunnerImage:           f.runnerImage,
			MetadataOutputKey:     f.metadataOutputKey,
			Deployment:            deploymentSummary,
			NATSConfiguration:     f.natsConfiguration,
		}, nil
	default:
		return nil, errors.Errorf("unsupported runner type: %s", runnerType)
	}
}

type RunnerLogsDeleter func(ctx context.Context, envUuid string) error

type RunnerNATSConfiguration = runnercommon.NATSConfiguration
