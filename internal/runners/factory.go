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
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

// DefaultRunnerFactory implements RunnerFactory interface
type DefaultRunnerFactory struct {
	awsTemporaryAuthProvider           AwsTemporaryAuthProvider
	k8sClusterClientFactory            kubernetes.ClientFactory
	vlt                                vault.VaultClientInterface
	externalDataplaneUrl               string
	runnerImage                        string
	runnerTokenSalt                    string
	metadataOutputKey                  string
	logger                             *zap.Logger
	internalDataplaneHostname          string
	runnerLogsBucketSignedUrlGenerator RunnerLogsSignedUrlGenerator
	kubernetesRunnerPodSchedulingDelay time.Duration
}

func NewDefaultRunnerFactory(
	logger *zap.Logger,
	awsTemporaryAuthProvider AwsTemporaryAuthProvider,
	k8sClusterClientFactory kubernetes.ClientFactory,
	vlt vault.VaultClientInterface,
	externalDataplaneUrl,
	runnerImage,
	runnerTokenSalt,
	metadataOutputKey,
	internalDataplaneHostname string,
	runnerLogsBucketSignedUrlGenerator RunnerLogsSignedUrlGenerator,
	kubernetesRunnerPodSchedulingDelay time.Duration,
) *DefaultRunnerFactory {
	return &DefaultRunnerFactory{
		logger:                             logger,
		awsTemporaryAuthProvider:           awsTemporaryAuthProvider,
		k8sClusterClientFactory:            k8sClusterClientFactory,
		vlt:                                vlt,
		externalDataplaneUrl:               externalDataplaneUrl,
		runnerImage:                        runnerImage,
		runnerTokenSalt:                    runnerTokenSalt,
		metadataOutputKey:                  metadataOutputKey,
		internalDataplaneHostname:          internalDataplaneHostname,
		runnerLogsBucketSignedUrlGenerator: runnerLogsBucketSignedUrlGenerator,
		kubernetesRunnerPodSchedulingDelay: kubernetesRunnerPodSchedulingDelay,
	}
}

func (f *DefaultRunnerFactory) CreateRunner(ctx context.Context, internalRunner platformorchestratorcp.InternalRunner, deploymentSummary *model.DeploymentSummary) (RunnerInterface, error) {
	logger := f.logger.With(logging.ZapOrgId(deploymentSummary.OrgId), logging.ZapRunnerId(internalRunner.Id), logging.ZapDeploymentId(deploymentSummary.Id.String()))

	runnerLogsBucketSignedUrl, err := f.runnerLogsBucketSignedUrlGenerator(ctx, deploymentSummary.DeploymentEnvUuid.String()+"/"+deploymentSummary.Id.String(), deploymentSummary.EncryptedLogsRecipient.Or(""))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get runner logs bucket signed URL")
	}

	runnerType, err := internalRunner.RunnerConfiguration.Discriminator()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get runner type")
	}

	switch runnerType {
	case string(platformorchestratorcp.RunnerTypeKubernetesAgent):
		return NewRemoteKubernetesRunner(
			f.externalDataplaneUrl,
			f.runnerImage,
			f.runnerTokenSalt,
			logger,
			internalRunner,
			deploymentSummary,
			f.internalDataplaneHostname,
			runnerLogsBucketSignedUrl,
			f.kubernetesRunnerPodSchedulingDelay,
		), nil
	case string(platformorchestratorcp.RunnerTypeKubernetes), string(platformorchestratorcp.RunnerTypeKubernetesGke), string(platformorchestratorcp.RunnerTypeKubernetesEks):
		return NewKubernetesRunner(
			f.k8sClusterClientFactory,
			f.vlt,
			f.externalDataplaneUrl,
			f.runnerImage,
			f.runnerTokenSalt,
			f.metadataOutputKey,
			logger,
			internalRunner,
			deploymentSummary,
			runnerLogsBucketSignedUrl,
			f.kubernetesRunnerPodSchedulingDelay,
		), nil
	case string(platformorchestratorcp.RunnerTypeServerlessEcs):
		return &ecsRunnerInstance{
			Runner:                internalRunner,
			TemporaryAuthProvider: f.awsTemporaryAuthProvider,
			ExternalDataplaneUrl:  f.externalDataplaneUrl,
			RunnerImage:           f.runnerImage,
			RunnerTokenSalt:       f.runnerTokenSalt,
			RunnerLogsSignedUrl:   runnerLogsBucketSignedUrl,
			MetadataOutputKey:     f.metadataOutputKey,
			Deployment:            deploymentSummary,
		}, nil
	default:
		return nil, errors.Errorf("unsupported runner type: %s", runnerType)
	}
}

type RunnerLogsSignedUrlGenerator func(ctx context.Context, deploymentUuid, encryptedLogsRecipient string) (string, error)

type RunnerLogsDeleter func(ctx context.Context, envUuid string) error
