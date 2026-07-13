//go:generate go tool mockgen -destination=mocks/k8s.go -package mock_k8s github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes KubernetesInterface

package kubernetes

import (
	"context"
	"errors"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

var ErrNotFound = errors.New("job not found")
var ErrK8sActionForbidden = errors.New("kubernetes action forbidden")

type KubernetesInterface interface {
	IsJobRunning(ctx context.Context, namespace, deploymentId string) (bool, error)
	CreateJob(ctx context.Context, rnConfiguration platformorchestratorcp.K8sRunnerJobConfig, externalDataplaneUrl, runnerImage, token, runnerLogsSignedUrl, metadataOutputKey string, d *model.DeploymentSummary) (*v1.Job, error)
	CheckJobStatus(ctx context.Context, namespace, deploymentId string) (*v1.JobStatus, error)
	GetPodJob(ctx context.Context, namespace, jobName string) (*corev1.Pod, error)
	GetObjectWarningEvents(ctx context.Context, namespace string, jobName string) ([]string, error)
}

// ClientFactory creates Kubernetes clients for different cluster configurations
type ClientFactory interface {
	GetClusterClient(ctx context.Context, r platformorchestratorcp.InternalRunner, vlt vault.VaultClientInterface) (KubernetesInterface, error)
}
