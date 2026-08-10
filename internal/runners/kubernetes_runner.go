package runners

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	usererror "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/runnercommon"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
)

// KubernetesRunner handles Kubernetes and GKE runners
type KubernetesRunner struct {
	k8sClusterClientFactory kubernetes.ClientFactory
	vlt                     vault.VaultClientInterface
	runnerImage             string
	metadataOutputKey       string
	logger                  *zap.Logger
	internalRunner          platformorchestratorcp.InternalRunner
	deploymentSummary       *model.DeploymentSummary
	podSchedulingDelay      time.Duration
	natsConfiguration       runnercommon.NATSConfiguration
}

func NewKubernetesRunner(
	k8sClusterClientFactory kubernetes.ClientFactory,
	vlt vault.VaultClientInterface,
	runnerImage string,
	metadataOutputKey string,
	logger *zap.Logger,
	internalRunner platformorchestratorcp.InternalRunner,
	deploymentSummary *model.DeploymentSummary,
	podSchedulingDelay time.Duration,
	natsConfigurations ...runnercommon.NATSConfiguration,
) *KubernetesRunner {
	var natsConfiguration runnercommon.NATSConfiguration
	if len(natsConfigurations) > 0 {
		natsConfiguration = natsConfigurations[0]
	}
	return &KubernetesRunner{
		k8sClusterClientFactory: k8sClusterClientFactory,
		vlt:                     vlt,
		runnerImage:             runnerImage,
		metadataOutputKey:       metadataOutputKey,
		logger:                  logger,
		internalRunner:          internalRunner,
		deploymentSummary:       deploymentSummary,
		podSchedulingDelay:      podSchedulingDelay,
		natsConfiguration:       natsConfiguration,
	}
}

func (k *KubernetesRunner) Start(ctx context.Context) error {
	jobCfg, err := getJobConfiguration(k.internalRunner.RunnerConfiguration)
	if err != nil {
		return errors.Wrap(err, "failed to get job configuration from runner configuration")
	}

	k8sClient, err := k.k8sClusterClientFactory.GetClusterClient(ctx, k.internalRunner, k.vlt)
	if err != nil {
		return errors.Wrap(err, "failed to obtain access to the runner cluster")
	}

	job, err := k8sClient.CreateJob(ctx, jobCfg, k.runnerImage, k.metadataOutputKey, k.deploymentSummary, k.natsConfiguration)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return usererror.NewUserErrorWithDetails("failed to run the job, a resource was not found, this can be caused by a misconfiguration of the runner", err)
		}
		if k8serrors.IsForbidden(err) {
			return usererror.NewUserErrorWithDetails("failed to run the job due to a permission issue", err)
		}
		return errors.Wrap(err, "failed to run the job")
	}

	k.logger.Sugar().Infow("runner job created", zap.String("runner_job_id", job.GetName()))
	return nil
}

func (k *KubernetesRunner) IsRunning(ctx context.Context) (bool, error) {
	jobCfg, err := getJobConfiguration(k.internalRunner.RunnerConfiguration)
	if err != nil {
		return false, errors.Wrap(err, "failed to get job configuration from runner configuration")
	}

	k8sClient, err := k.k8sClusterClientFactory.GetClusterClient(ctx, k.internalRunner, k.vlt)
	if err != nil {
		return false, errors.Wrap(err, "failed to obtain access to the runner cluster")
	}

	return k8sClient.IsJobRunning(ctx, jobCfg.Namespace, k.deploymentSummary.Id.String())
}

func (k *KubernetesRunner) CheckStatus(ctx context.Context) (*RunnerStatus, error) {
	jobCfg, err := getJobConfiguration(k.internalRunner.RunnerConfiguration)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get job configuration from runner configuration")
	}

	k8sClient, err := k.k8sClusterClientFactory.GetClusterClient(ctx, k.internalRunner, k.vlt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain access to the runner cluster")
	}

	jobStatus, err := k8sClient.CheckJobStatus(ctx, jobCfg.Namespace, k.deploymentSummary.Id.String())
	if err != nil {
		if errors.Is(err, kubernetes.ErrNotFound) {
			// Job not found, it might not have been scheduled yet
			// we must introduce a delay before considering it stuck as we experienced some system can take long time to schedule the job
			return &RunnerStatus{
				IsCompleted: false,
				IsStuck:     time.Since(k.deploymentSummary.CreatedAt) > k.podSchedulingDelay,
			}, nil
		}
		return nil, errors.Wrap(err, "failed to check job status")
	}

	jobStartDate := ref.DerefOr(jobStatus.StartTime, v1.NewTime(k.deploymentSummary.CreatedAt))
	if jobStatus.Failed > 0 || jobStatus.Succeeded > 0 {
		return &RunnerStatus{
			IsCompleted: true,
			IsStuck:     false,
		}, nil
	}

	var podNotReady bool
	var objectToFetchEventsAbout = k.deploymentSummary.Id.String()
	if jobStatus.Active > 0 && ref.DerefOr(jobStatus.Ready, 0) == 0 {
		if podJob, err := k8sClient.GetPodJob(ctx, jobCfg.Namespace, k.deploymentSummary.Id.String()); err != nil {
			if errors.Is(err, kubernetes.ErrNotFound) {
				podNotReady = true
			} else if errors.Is(err, kubernetes.ErrK8sActionForbidden) {
				return &RunnerStatus{
					IsCompleted: false,
					IsStuck:     false,
					Message:     "job is running but the runner configuration does not allow to read pods in the target namespace, please check the runner configuration",
				}, nil
			} else {
				return nil, errors.Wrap(err, "failed to check pod job status")
			}
		} else if podJob != nil && podJob.Status.Phase == corev1.PodPending || podJob.Status.Phase == corev1.PodUnknown {
			podNotReady = true
			objectToFetchEventsAbout = podJob.Name
		}
	}

	if (jobStatus.Active == 0 && jobStatus.Failed == 0 && jobStatus.Succeeded == 0) || podNotReady {
		// Job is not found or not started yet
		// we must introduce a delay before considering it stuck as we experienced some system can take long time to schedule the job
		if time.Since(jobStartDate.Time) < k.podSchedulingDelay {
			return &RunnerStatus{
				IsCompleted: false,
				IsStuck:     false,
			}, nil
		} else {
			var message string
			// Job or pod has not started for a long time, we should consider it stuck and parse the reason from the events
			if warningEvents, err := k8sClient.GetObjectWarningEvents(ctx, jobCfg.Namespace, objectToFetchEventsAbout); err != nil {
				k.logger.Warn("failed to get job warning events", zap.Error(err))
				if errors.Is(err, kubernetes.ErrK8sActionForbidden) {
					message = "job has not started and the runner configuration does not allow to read job events and pods in the target namespace, please check the runner configuration"
				}
			} else {
				message = strings.Join(warningEvents, "\n")
			}

			return &RunnerStatus{
				IsCompleted: false,
				IsStuck:     true,
				Message:     message,
			}, nil
		}
	}
	return &RunnerStatus{IsCompleted: true}, nil
}

func getJobConfiguration(cfg platformorchestratorcp.RunnerConfiguration) (platformorchestratorcp.K8sRunnerJobConfig, error) {
	runnerType, _ := cfg.Discriminator()
	switch runnerType {
	case string(platformorchestratorcp.RunnerTypeKubernetes):
		k8sCfg, _ := cfg.AsK8sRunnerConfiguration()
		return k8sCfg.Job, nil
	case string(platformorchestratorcp.RunnerTypeKubernetesGke):
		gkeCfg, _ := cfg.AsK8sGkeRunnerConfiguration()
		return gkeCfg.Job, nil
	case string(platformorchestratorcp.RunnerTypeKubernetesEks):
		eksCfg, _ := cfg.AsK8sEksRunnerConfiguration()
		return eksCfg.Job, nil
	case string(platformorchestratorcp.RunnerTypeKubernetesAgent):
		k8sAgentCfg, _ := cfg.AsK8sAgentRunnerConfiguration()
		return k8sAgentCfg.Job, nil
	default:
		return platformorchestratorcp.K8sRunnerJobConfig{}, errors.New("unknown runner type")
	}
}
