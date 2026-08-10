package kubernetes

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/runnercommon"
)

const (
	DefaultMemoryRequest = "1Gi"
	RunnerContainerName  = "main"
	DefaultRunnerJobTTL  = time.Minute * 60
	TofuDirMountPath     = "/opt/runner/tofu"
	TofuDirVolumeName    = "tofu"
)

type kubernetesClient struct {
	client kubernetes.Interface
}

func NewKubernetesClient(client kubernetes.Interface) KubernetesInterface {
	return &kubernetesClient{
		client: client,
	}
}

func (c *kubernetesClient) IsJobRunning(ctx context.Context, namespace string, jobName string) (bool, error) {
	if _, err := c.client.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "failed to retrieve job")
	}
	return true, nil
}

func (c *kubernetesClient) CreateJob(ctx context.Context, jobCfg platformorchestratorcp.K8sRunnerJobConfig, runnerImage, metadataOutputKey string, d *model.DeploymentSummary, natsConfigurations ...runnercommon.NATSConfiguration) (*v1.Job, error) {
	jobSpec, err := GetJobSpec(ctx, jobCfg, runnerImage, metadataOutputKey, d, natsConfigurations...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get job spec")
	}
	return c.client.BatchV1().Jobs(jobCfg.Namespace).Create(ctx, &v1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      d.Id.String(),
			Namespace: jobCfg.Namespace,
		},
		Spec: *jobSpec,
	}, metav1.CreateOptions{})
}

func (c *kubernetesClient) CheckJobStatus(ctx context.Context, namespace string, jobName string) (*v1.JobStatus, error) {
	if job, err := c.client.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, errors.Wrap(err, "failed to retrieve job")
	} else {
		return &job.Status, nil
	}
}

func (c *kubernetesClient) GetObjectWarningEvents(ctx context.Context, namespace string, objName string) ([]string, error) {
	eventList, err := c.client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("involvedObject.name=%s", objName),
	})
	if err != nil {
		if k8serrors.IsForbidden(err) {
			return nil, ErrK8sActionForbidden
		}
		return nil, errors.Wrapf(err, "failed to retrieve events for object %q", objName)
	}

	slices.SortFunc(eventList.Items, func(event1, event2 corev1.Event) int {
		return cmp.Compare(event2.CreationTimestamp.Unix(), event1.CreationTimestamp.Unix())
	})
	var warnings []string
	for _, event := range eventList.Items {
		if event.Type == corev1.EventTypeWarning {
			warnings = append(warnings, fmt.Sprintf("%s - %s - %s", event.CreationTimestamp.UTC(), event.Reason, event.Message))
		}
	}
	return warnings, nil
}

func (c *kubernetesClient) GetPodJob(ctx context.Context, namespace, jobName string) (*corev1.Pod, error) {
	if podList, err := c.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	}); err != nil {
		if k8serrors.IsForbidden(err) {
			return nil, ErrK8sActionForbidden
		} else if k8serrors.IsNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, errors.Wrapf(err, "failed to list pods for job '%s'", jobName)
	} else {
		if podList == nil || len(podList.Items) == 0 {
			return nil, ErrNotFound
		}
		// We expect only one pod per job
		return &podList.Items[0], nil
	}
}
