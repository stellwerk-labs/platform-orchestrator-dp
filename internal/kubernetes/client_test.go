package kubernetes

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

const (
	testNamespace    = "test-namespace"
	testJobName      = "test-job-12345"
	testDeploymentId = "test-deployment-id"
	testOrgId        = "test-org"
	testProjectId    = "test-project"
	testEnvId        = "test-env"
	testRunnerId     = "test-runner"
	testRunnerImage  = "test-runner:latest"
	testDataplaneUrl = "http://test-dataplane"
	testToken        = "test-token"
	testLogsUrl      = "http://test-logs"
)

func createTestDeploymentSummary() *model.DeploymentSummary {
	id := uuid.MustParse("12345678-1234-5678-9012-123456789012")
	return &model.DeploymentSummary{
		Id:                        id,
		OrgId:                     testOrgId,
		ProjectId:                 testProjectId,
		EnvId:                     testEnvId,
		RunnerId:                  testRunnerId,
		Mode:                      model.DeploymentModeDeploy,
		RunnerLogLevel:            "info",
		EncryptedOutputsRecipient: opt.Of("test-encrypt-key"),
		EncryptedLogsRecipient:    opt.Of("test-logs-key"),
	}
}

func createTestJobConfig() platformorchestratorcp.K8sRunnerJobConfig {
	return platformorchestratorcp.K8sRunnerJobConfig{
		Namespace:      testNamespace,
		ServiceAccount: "test-service-account",
	}
}

func TestNewKubernetesClient(t *testing.T) {
	fakeClient := fake.NewClientset()
	client := NewKubernetesClient(fakeClient)

	assert.NotNil(t, client)
	assert.IsType(t, &kubernetesClient{}, client)
}

func TestKubernetesClient_IsJobRunning_JobExists(t *testing.T) {
	fakeClient := fake.NewClientset()

	job := &v1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testJobName,
			Namespace: testNamespace,
		},
	}
	_, err := fakeClient.BatchV1().Jobs(testNamespace).Create(context.Background(), job, metav1.CreateOptions{})
	require.NoError(t, err)

	client := NewKubernetesClient(fakeClient)

	running, err := client.IsJobRunning(context.Background(), testNamespace, testJobName)
	require.NoError(t, err)
	assert.True(t, running)
}

func TestKubernetesClient_IsJobRunning_JobNotFound(t *testing.T) {
	fakeClient := fake.NewClientset()
	client := NewKubernetesClient(fakeClient)

	running, err := client.IsJobRunning(context.Background(), testNamespace, "non-existent-job")
	require.NoError(t, err)
	assert.False(t, running)
}

func TestKubernetesClient_IsJobRunning_Error(t *testing.T) {
	fakeClient := fake.NewClientset()

	fakeClient.PrependReactor("get", "jobs", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		return true, nil, fmt.Errorf("simulated k8s error")
	})

	client := NewKubernetesClient(fakeClient)

	running, err := client.IsJobRunning(context.Background(), testNamespace, testJobName)
	require.Error(t, err)
	assert.False(t, running)
	assert.Contains(t, err.Error(), "failed to retrieve job")
}

func TestKubernetesClient_CreateJob_Success(t *testing.T) {
	fakeClient := fake.NewClientset()
	client := NewKubernetesClient(fakeClient)

	deploymentSummary := createTestDeploymentSummary()
	jobConfig := createTestJobConfig()

	job, err := client.CreateJob(context.Background(), jobConfig, testDataplaneUrl, testRunnerImage, testToken, testLogsUrl, "", deploymentSummary)
	require.NoError(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, deploymentSummary.Id.String(), job.Name)
	assert.Equal(t, testNamespace, job.Namespace)

	createdJob, err := fakeClient.BatchV1().Jobs(testNamespace).Get(context.Background(), deploymentSummary.Id.String(), metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, deploymentSummary.Id.String(), createdJob.Name)
}

func TestKubernetesClient_CreateJob_GetJobSpecError(t *testing.T) {
	fakeClient := fake.NewClientset()
	client := NewKubernetesClient(fakeClient)

	deploymentSummary := createTestDeploymentSummary()
	jobConfig := platformorchestratorcp.K8sRunnerJobConfig{
		Namespace:      testNamespace,
		ServiceAccount: "test-service-account",
		PodTemplate:    &map[string]interface{}{"invalid": "template"},
	}

	job, err := client.CreateJob(context.Background(), jobConfig, testDataplaneUrl, testRunnerImage, testToken, testLogsUrl, "", deploymentSummary)
	require.Error(t, err)
	assert.Nil(t, job)
	assert.Contains(t, err.Error(), "failed to get job spec")
}

func TestKubernetesClient_CreateJob_K8sCreateError(t *testing.T) {
	fakeClient := fake.NewClientset()

	fakeClient.PrependReactor("create", "jobs", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		return true, &v1.Job{}, fmt.Errorf("simulated k8s create error")
	})

	client := NewKubernetesClient(fakeClient)

	deploymentSummary := createTestDeploymentSummary()
	jobConfig := createTestJobConfig()

	_, err := client.CreateJob(context.Background(), jobConfig, testDataplaneUrl, testRunnerImage, testToken, testLogsUrl, "", deploymentSummary)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulated k8s create error")
}

func TestKubernetesClient_CheckJobStatus_JobExists(t *testing.T) {
	fakeClient := fake.NewClientset()

	job := &v1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testJobName,
			Namespace: testNamespace,
		},
		Status: v1.JobStatus{
			Active:    1,
			Succeeded: 0,
			Failed:    0,
			StartTime: &metav1.Time{Time: time.Now()},
		},
	}
	_, err := fakeClient.BatchV1().Jobs(testNamespace).Create(context.Background(), job, metav1.CreateOptions{})
	require.NoError(t, err)

	client := NewKubernetesClient(fakeClient)

	status, err := client.CheckJobStatus(context.Background(), testNamespace, testJobName)
	require.NoError(t, err)
	assert.NotNil(t, status)
	assert.Equal(t, int32(1), status.Active)
	assert.Equal(t, int32(0), status.Succeeded)
	assert.Equal(t, int32(0), status.Failed)
}

func TestKubernetesClient_CheckJobStatus_JobNotFound(t *testing.T) {
	fakeClient := fake.NewClientset()
	client := NewKubernetesClient(fakeClient)

	status, err := client.CheckJobStatus(context.Background(), testNamespace, "non-existent-job")
	require.Error(t, err)
	assert.Nil(t, status)
	assert.Equal(t, ErrNotFound, err)
}

func TestKubernetesClient_CheckJobStatus_K8sError(t *testing.T) {
	fakeClient := fake.NewClientset()

	fakeClient.PrependReactor("get", "jobs", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		return true, nil, fmt.Errorf("simulated k8s get error")
	})

	client := NewKubernetesClient(fakeClient)

	status, err := client.CheckJobStatus(context.Background(), testNamespace, testJobName)
	require.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "failed to retrieve job")
}

func TestKubernetesClient_GetPodJob_PodExists(t *testing.T) {
	fakeClient := fake.NewClientset()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: testNamespace,
			Labels: map[string]string{
				"job-name": testJobName,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	_, err := fakeClient.CoreV1().Pods(testNamespace).Create(context.Background(), pod, metav1.CreateOptions{})
	require.NoError(t, err)

	client := NewKubernetesClient(fakeClient)

	podResult, err := client.GetPodJob(context.Background(), testNamespace, testJobName)
	require.NoError(t, err)
	assert.NotNil(t, podResult)
	assert.Equal(t, corev1.PodRunning, podResult.Status.Phase)
}

func TestKubernetesClient_GetPodJob_NoPods(t *testing.T) {
	fakeClient := fake.NewClientset()
	client := NewKubernetesClient(fakeClient)

	pod, err := client.GetPodJob(context.Background(), testNamespace, testJobName)
	require.Error(t, err)
	assert.Nil(t, pod)
	assert.Equal(t, ErrNotFound, err)
}

func TestKubernetesClient_GetPodJob_ListError(t *testing.T) {
	fakeClient := fake.NewClientset()

	fakeClient.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		return true, nil, fmt.Errorf("simulated k8s list error")
	})

	client := NewKubernetesClient(fakeClient)

	pod, err := client.GetPodJob(context.Background(), testNamespace, testJobName)
	require.Error(t, err)
	assert.Nil(t, pod)
	assert.Contains(t, err.Error(), "failed to list pods for job")
}

func TestKubernetesClient_GetPodJob_ListNotFound(t *testing.T) {
	fakeClient := fake.NewClientset()

	fakeClient.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		return true, nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "")
	})

	client := NewKubernetesClient(fakeClient)

	pod, err := client.GetPodJob(context.Background(), testNamespace, testJobName)
	require.Error(t, err)
	assert.Nil(t, pod)
	assert.Equal(t, ErrNotFound, err)
}

func TestKubernetesClient_GetPodJob_ListForbidden(t *testing.T) {
	fakeClient := fake.NewClientset()

	fakeClient.PrependReactor("list", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		return true, nil, k8serrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", fmt.Errorf("access denied"))
	})

	client := NewKubernetesClient(fakeClient)

	pod, err := client.GetPodJob(context.Background(), testNamespace, testJobName)
	require.Error(t, err)
	assert.Nil(t, pod)
	assert.Equal(t, ErrK8sActionForbidden, err)
}

func TestKubernetesClient_GetJobWarningEvents_WithJobEvents(t *testing.T) {
	fakeClient := fake.NewClientset()

	event1 := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "warning-event-1",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.Time{Time: time.Now().Add(-2 * time.Minute)},
		},
		InvolvedObject: corev1.ObjectReference{
			Name: testJobName,
		},
		Type:    "Warning",
		Reason:  "FailedScheduling",
		Message: "insufficient resources",
	}

	event2 := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "warning-event-2",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
		},
		InvolvedObject: corev1.ObjectReference{
			Name: testJobName,
		},
		Type:    "Warning",
		Reason:  "FailedMount",
		Message: "volume not found",
	}

	// Create a normal event (should be filtered out)
	normalEvent := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "normal-event",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		InvolvedObject: corev1.ObjectReference{
			Name: testJobName,
		},
		Type:    "Normal",
		Reason:  "Started",
		Message: "job started successfully",
	}

	_, err := fakeClient.CoreV1().Events(testNamespace).Create(context.Background(), event1, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = fakeClient.CoreV1().Events(testNamespace).Create(context.Background(), event2, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = fakeClient.CoreV1().Events(testNamespace).Create(context.Background(), normalEvent, metav1.CreateOptions{})
	require.NoError(t, err)

	client := NewKubernetesClient(fakeClient)

	warnings, err := client.GetObjectWarningEvents(context.Background(), testNamespace, testJobName)
	require.NoError(t, err)
	assert.Len(t, warnings, 2)

	assert.Contains(t, warnings[0], "FailedMount")
	assert.Contains(t, warnings[0], "volume not found")
	assert.Contains(t, warnings[1], "FailedScheduling")
	assert.Contains(t, warnings[1], "insufficient resources")
}

func TestKubernetesClient_GetJobWarningEvents_WithPodEvents(t *testing.T) {
	fakeClient := fake.NewClientset()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: testNamespace,
			Labels: map[string]string{
				"job-name": testJobName,
			},
		},
	}
	_, err := fakeClient.CoreV1().Pods(testNamespace).Create(context.Background(), pod, metav1.CreateOptions{})
	require.NoError(t, err)

	podEvent := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-warning-event",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		InvolvedObject: corev1.ObjectReference{
			Name: "test-pod",
		},
		Type:    "Warning",
		Reason:  "FailedPullImage",
		Message: "image not found",
	}

	_, err = fakeClient.CoreV1().Events(testNamespace).Create(context.Background(), podEvent, metav1.CreateOptions{})
	require.NoError(t, err)

	client := NewKubernetesClient(fakeClient)

	warnings, err := client.GetObjectWarningEvents(context.Background(), testNamespace, testJobName)
	require.NoError(t, err)
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "FailedPullImage")
	assert.Contains(t, warnings[0], "image not found")
}

func TestKubernetesClient_GetObjectWarningEvents_NoEvents(t *testing.T) {
	fakeClient := fake.NewClientset()
	client := NewKubernetesClient(fakeClient)

	warnings, err := client.GetObjectWarningEvents(context.Background(), testNamespace, testJobName)
	require.NoError(t, err)
	assert.Empty(t, warnings)
}

func TestKubernetesClient_GetObjectWarningEvents_ListEventsError(t *testing.T) {
	fakeClient := fake.NewClientset()

	fakeClient.PrependReactor("list", "events", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		return true, nil, fmt.Errorf("simulated k8s list events error")
	})

	client := NewKubernetesClient(fakeClient)

	warnings, err := client.GetObjectWarningEvents(context.Background(), testNamespace, testJobName)
	require.Error(t, err)
	assert.Nil(t, warnings)
	assert.Contains(t, err.Error(), "failed to retrieve events for object")
}

func TestKubernetesClient_GetObjectWarningEvents_EventsForbiddenError(t *testing.T) {
	fakeClient := fake.NewClientset()

	fakeClient.PrependReactor("list", "events", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		return true, nil, k8serrors.NewForbidden(schema.GroupResource{Resource: "events"}, "", fmt.Errorf("access denied"))
	})

	client := NewKubernetesClient(fakeClient)

	warnings, err := client.GetObjectWarningEvents(context.Background(), testNamespace, testJobName)
	require.Error(t, err)
	assert.Nil(t, warnings)
	assert.Equal(t, ErrK8sActionForbidden, err)
}
