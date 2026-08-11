package runners

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	usererror "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes"
	mockk8s "github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/runnercommon"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
	mockvault "github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault/mocks"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
)

const (
	testRunnerImage                    = "ghcr.io/stellwerk-labs/platform-orchestrator-runner:v1.8.3"
	testOrgId                          = "test-org"
	testProjectId                      = "test-app"
	testEnvId                          = "test-env"
	testRunnerId                       = "default"
	kubernetesRunnerPodSchedulingDelay = 2 * time.Second
)

type mockClientFactory struct {
	k8s kubernetes.KubernetesInterface
}

func (m *mockClientFactory) GetClusterClient(_ context.Context, _ platformorchestratorcp.InternalRunner, _ vault.VaultClientInterface) (kubernetes.KubernetesInterface, error) {
	return m.k8s, nil
}

func createTestKubernetesRunner(t *testing.T, k8sClient kubernetes.KubernetesInterface, vaultClient vault.VaultClientInterface) (*KubernetesRunner, *model.DeploymentSummary, platformorchestratorcp.InternalRunner) {
	depId := uuid.New()
	deploymentSummary := &model.DeploymentSummary{
		OrgId:     testOrgId,
		ProjectId: testProjectId,
		EnvId:     testEnvId,
		Id:        depId,
		RunnerId:  testRunnerId,
		Status:    model.DeploymentStatusExecuting,
	}

	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Cluster: platformorchestratorcp.K8sRunnerK8sCluster{
			ClusterData: platformorchestratorcp.K8sRunnerK8sClusterClusterData{
				Server: "test-server",
			},
		},
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
	}))

	internalRunner := platformorchestratorcp.InternalRunner{
		Id:                  testRunnerId,
		RunnerConfiguration: *cfg,
	}

	runner := NewKubernetesRunner(
		&mockClientFactory{k8s: k8sClient},
		vaultClient,
		testRunnerImage,
		"",
		zaptest.NewLogger(t),
		internalRunner,
		deploymentSummary,
		kubernetesRunnerPodSchedulingDelay,
	)

	return runner, deploymentSummary, internalRunner
}

func TestKubernetesRunner_IsRunning_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	mockK8s.EXPECT().IsJobRunning(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).Return(true, nil)

	isRunning, err := runner.IsRunning(context.Background())
	require.NoError(t, err)
	assert.True(t, isRunning)
}

func TestKubernetesRunner_IsRunning_NotRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	mockK8s.EXPECT().IsJobRunning(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).Return(false, nil)

	isRunning, err := runner.IsRunning(context.Background())
	require.NoError(t, err)
	assert.False(t, isRunning)
}

func TestKubernetesRunner_IsRunning_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	mockK8s.EXPECT().IsJobRunning(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).Return(false, errors.New("k8s error"))

	isRunning, err := runner.IsRunning(context.Background())
	require.Error(t, err)
	assert.False(t, isRunning)
	assert.Contains(t, err.Error(), "k8s error")
}

func TestKubernetesRunner_Start_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)
	runner.gatewayConfiguration = runnercommon.GatewayConfiguration{URL: "https://gateway.example.test", RunnerTokenSalt: "salt"}

	expectedJob := &v1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: deploymentSummary.Id.String(),
		},
	}

	mockK8s.EXPECT().CreateJob(
		gomock.Any(),
		platformorchestratorcp.K8sRunnerJobConfig{Namespace: "platform-orchestrator-runner", ServiceAccount: "platform-orchestrator-runner"},
		testRunnerImage,
		"",
		deploymentSummary,
		runnercommon.GatewayConfiguration{URL: "https://gateway.example.test", RunnerTokenSalt: "salt"},
	).Return(expectedJob, nil)

	err := runner.Start(context.Background())
	require.NoError(t, err)
}

func TestKubernetesRunner_Start_PassesMetadataOutputKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	depId := uuid.New()
	deploymentSummary := &model.DeploymentSummary{
		OrgId:     testOrgId,
		ProjectId: testProjectId,
		EnvId:     testEnvId,
		Id:        depId,
		RunnerId:  testRunnerId,
		Status:    model.DeploymentStatusExecuting,
	}

	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
	}))
	internalRunner := platformorchestratorcp.InternalRunner{
		Id:                  testRunnerId,
		RunnerConfiguration: *cfg,
	}

	runner := NewKubernetesRunner(
		&mockClientFactory{k8s: mockK8s},
		mockVault,
		testRunnerImage,
		"platform_orchestrator_metadata",
		zaptest.NewLogger(t),
		internalRunner,
		deploymentSummary,
		kubernetesRunnerPodSchedulingDelay,
	)

	mockK8s.EXPECT().CreateJob(
		gomock.Any(),
		platformorchestratorcp.K8sRunnerJobConfig{Namespace: "platform-orchestrator-runner", ServiceAccount: "platform-orchestrator-runner"},
		testRunnerImage,
		"platform_orchestrator_metadata",
		deploymentSummary,
		runnercommon.GatewayConfiguration{},
	).Return(&v1.Job{ObjectMeta: metav1.ObjectMeta{Name: deploymentSummary.Id.String()}}, nil)

	err := runner.Start(context.Background())
	require.NoError(t, err)
}

func TestKubernetesRunner_Start_CreateJobError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	mockK8s.EXPECT().CreateJob(
		gomock.Any(),
		platformorchestratorcp.K8sRunnerJobConfig{Namespace: "platform-orchestrator-runner", ServiceAccount: "platform-orchestrator-runner"},
		testRunnerImage,
		"",
		deploymentSummary,
		runnercommon.GatewayConfiguration{},
	).Return(nil, errors.New("failed to create job"))

	err := runner.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to run the job")
}

func TestKubernetesRunner_Start_CreateJobError_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	mockK8s.EXPECT().CreateJob(
		gomock.Any(),
		platformorchestratorcp.K8sRunnerJobConfig{Namespace: "platform-orchestrator-runner", ServiceAccount: "platform-orchestrator-runner"},
		testRunnerImage,
		"",
		deploymentSummary,
		runnercommon.GatewayConfiguration{},
	).Return(nil, k8serrors.NewNotFound(schema.GroupResource{}, ""))

	err := runner.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to run the job, a resource was not found, this can be caused by a misconfiguration of the runner")
	var userErr *usererror.UserError
	require.True(t, errors.As(err, &userErr))
}

func TestKubernetesRunner_Start_CreateJobError_Forbidden(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	mockK8s.EXPECT().CreateJob(
		gomock.Any(),
		platformorchestratorcp.K8sRunnerJobConfig{Namespace: "platform-orchestrator-runner", ServiceAccount: "platform-orchestrator-runner"},
		testRunnerImage,
		"",
		deploymentSummary,
		runnercommon.GatewayConfiguration{},
	).Return(nil, k8serrors.NewForbidden(schema.GroupResource{}, "", errors.New("test")))

	err := runner.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to run the job due to a permission issue")
	var userErr *usererror.UserError
	require.True(t, errors.As(err, &userErr))
}

func TestKubernetesRunner_Start_GetClusterClientError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	depId := uuid.New()
	deploymentSummary := &model.DeploymentSummary{
		OrgId:     testOrgId,
		ProjectId: testProjectId,
		EnvId:     testEnvId,
		Id:        depId,
		RunnerId:  testRunnerId,
	}

	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Cluster: platformorchestratorcp.K8sRunnerK8sCluster{
			ClusterData: platformorchestratorcp.K8sRunnerK8sClusterClusterData{
				Server: "test-server",
			},
		},
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
	}))

	internalRunner := platformorchestratorcp.InternalRunner{
		Id:                  testRunnerId,
		RunnerConfiguration: *cfg,
	}

	runner := &KubernetesRunner{
		k8sClusterClientFactory: &failingClientFactory{},
		vlt:                     mockVault,
		runnerImage:             testRunnerImage,
		logger:                  zaptest.NewLogger(t),
		internalRunner:          internalRunner,
		deploymentSummary:       deploymentSummary,
	}

	err := runner.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to obtain access to the runner cluster")
}

func TestKubernetesRunner_Start_InvalidJobConfiguration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	depId := uuid.New()
	deploymentSummary := &model.DeploymentSummary{
		OrgId:     testOrgId,
		ProjectId: testProjectId,
		EnvId:     testEnvId,
		Id:        depId,
		RunnerId:  testRunnerId,
	}

	cfg := platformorchestratorcp.RunnerConfiguration{}

	internalRunner := platformorchestratorcp.InternalRunner{
		Id:                  testRunnerId,
		RunnerConfiguration: cfg,
	}

	runner := NewKubernetesRunner(
		&mockClientFactory{k8s: nil},
		mockVault,
		testRunnerImage,
		"",
		zaptest.NewLogger(t),
		internalRunner,
		deploymentSummary,
		kubernetesRunnerPodSchedulingDelay,
	)

	err := runner.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get job configuration")
}

type failingClientFactory struct{}

func (f *failingClientFactory) GetClusterClient(_ context.Context, _ platformorchestratorcp.InternalRunner, _ vault.VaultClientInterface) (kubernetes.KubernetesInterface, error) {
	return nil, errors.New("failed to connect to cluster")
}

func TestKubernetesRunner_GetJobConfiguration_Success(t *testing.T) {
	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "test-namespace",
			ServiceAccount: "test-service-account",
		},
	}))

	jobCfg, err := getJobConfiguration(*cfg)
	require.NoError(t, err)
	assert.Equal(t, "test-namespace", jobCfg.Namespace)
	assert.Equal(t, "test-service-account", jobCfg.ServiceAccount)
}

func TestKubernetesRunner_GetJobConfiguration_InvalidConfig(t *testing.T) {
	cfg := platformorchestratorcp.RunnerConfiguration{}

	_, err := getJobConfiguration(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown runner type")
}

func TestKubernetesRunner_GetJobConfiguration_RemoteConfig(t *testing.T) {
	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sAgentRunnerConfiguration(platformorchestratorcp.K8sAgentRunnerConfiguration{
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace: "test-namespace",
		},
	}))

	jobCfg, err := getJobConfiguration(*cfg)
	require.NoError(t, err)
	assert.Equal(t, "test-namespace", jobCfg.Namespace)
}

func TestKubernetesRunner_CheckStatus_JobNotFound_WithinDelay(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)
	deploymentSummary.CreatedAt = time.Now().Add(-1 * time.Second) // Within delay

	mockK8s.EXPECT().CheckJobStatus(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(nil, kubernetes.ErrNotFound)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.False(t, status.IsCompleted)
	assert.False(t, status.IsStuck)
}

func TestKubernetesRunner_CheckStatus_JobNotFound_ExceedsDelay(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)
	deploymentSummary.CreatedAt = time.Now().Add(-5 * time.Second) // Exceeds delay

	mockK8s.EXPECT().CheckJobStatus(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(nil, kubernetes.ErrNotFound)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.False(t, status.IsCompleted)
	assert.True(t, status.IsStuck)
}

func TestKubernetesRunner_CheckStatus_JobCompleted_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	jobStatus := &v1.JobStatus{
		Succeeded: 1,
		Failed:    0,
		Active:    0,
		StartTime: &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
	}

	mockK8s.EXPECT().CheckJobStatus(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(jobStatus, nil)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.True(t, status.IsCompleted)
	assert.False(t, status.IsStuck)
}

func TestKubernetesRunner_CheckStatus_JobCompleted_Failed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	jobStatus := &v1.JobStatus{
		Succeeded: 0,
		Failed:    1,
		Active:    0,
		StartTime: &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
	}

	mockK8s.EXPECT().CheckJobStatus(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(jobStatus, nil)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.True(t, status.IsCompleted)
	assert.False(t, status.IsStuck)
}

func TestKubernetesRunner_CheckStatus_JobActive_PodReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	jobStatus := &v1.JobStatus{
		Succeeded: 0,
		Failed:    0,
		Active:    1,
		Ready:     ref.Ref(int32(1)),
		StartTime: &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
	}

	mockK8s.EXPECT().CheckJobStatus(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(jobStatus, nil)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.True(t, status.IsCompleted)
	assert.False(t, status.IsStuck)
}

func TestKubernetesRunner_CheckStatus_JobActive_PodNotReady_WithinDelay(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	jobStatus := &v1.JobStatus{
		Succeeded: 0,
		Failed:    0,
		Active:    1,
		Ready:     ref.Ref(int32(0)),
		StartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Second)}, // Within delay
	}

	podStatus := &corev1.PodStatus{
		Phase: "Pending",
	}

	mockK8s.EXPECT().CheckJobStatus(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(jobStatus, nil)
	mockK8s.EXPECT().GetPodJob(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(&corev1.Pod{Status: *podStatus}, nil)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.False(t, status.IsCompleted)
	assert.False(t, status.IsStuck)
}

func TestKubernetesRunner_CheckStatus_JobActive_PodNotReady_ExceedsDelay(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	jobStatus := &v1.JobStatus{
		Succeeded: 0,
		Failed:    0,
		Active:    1,
		Ready:     ref.Ref(int32(0)),
		StartTime: &metav1.Time{Time: time.Now().Add(-5 * time.Second)}, // Exceeds delay
	}

	podStatus := &corev1.PodStatus{
		Phase: "Pending",
	}

	warningEvents := []string{"FailedScheduling: insufficient resources", "FailedMount: volume not found"}

	mockK8s.EXPECT().CheckJobStatus(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(jobStatus, nil)
	mockK8s.EXPECT().GetPodJob(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "platform-orchestrator-runner"}, Status: *podStatus}, nil)
	mockK8s.EXPECT().GetObjectWarningEvents(gomock.Any(), "platform-orchestrator-runner", "test-pod").
		Return(warningEvents, nil)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.False(t, status.IsCompleted)
	assert.True(t, status.IsStuck)
	assert.Contains(t, status.Message, "FailedScheduling: insufficient resources")
	assert.Contains(t, status.Message, "FailedMount: volume not found")
}

func TestKubernetesRunner_CheckStatus_PodNotFound_ExceedsDelay(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	jobStatus := &v1.JobStatus{
		Succeeded: 0,
		Failed:    0,
		Active:    1,
		Ready:     ref.Ref(int32(0)),
		StartTime: &metav1.Time{Time: time.Now().Add(-5 * time.Second)}, // Exceeds delay
	}

	warningEvents := []string{"FailedScheduling: insufficient resources", "FailedMount: volume not found"}

	mockK8s.EXPECT().CheckJobStatus(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(jobStatus, nil)
	mockK8s.EXPECT().GetPodJob(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(nil, kubernetes.ErrNotFound)
	mockK8s.EXPECT().GetObjectWarningEvents(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(warningEvents, nil)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.False(t, status.IsCompleted)
	assert.True(t, status.IsStuck)
	assert.Contains(t, status.Message, "FailedScheduling: insufficient resources")
	assert.Contains(t, status.Message, "FailedMount: volume not found")
}

func TestKubernetesRunner_CheckStatus_PodNotFound_GetPodJobError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	ready := int32(0)
	jobStatus := &v1.JobStatus{
		Succeeded: 0,
		Failed:    0,
		Active:    1,
		Ready:     &ready,
		StartTime: &metav1.Time{Time: time.Now().Add(-5 * time.Second)}, // Exceeds delay
	}

	mockK8s.EXPECT().CheckJobStatus(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(jobStatus, nil)
	mockK8s.EXPECT().GetPodJob(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(nil, errors.New("some other k8s error"))

	status, err := runner.CheckStatus(context.Background())
	require.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "failed to check pod job status")
}

func TestKubernetesRunner_CheckStatus_GetPodJob_Forbidden(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	ready := int32(0)
	jobStatus := &v1.JobStatus{
		Succeeded: 0,
		Failed:    0,
		Active:    1,
		Ready:     &ready,
		StartTime: &metav1.Time{Time: time.Now().Add(-5 * time.Second)}, // Exceeds delay
	}

	mockK8s.EXPECT().CheckJobStatus(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(jobStatus, nil)
	mockK8s.EXPECT().GetPodJob(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(nil, kubernetes.ErrK8sActionForbidden)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.False(t, status.IsCompleted)
	assert.False(t, status.IsStuck)
	assert.Contains(t, status.Message, "job is running but the runner configuration does not allow to read pods in the target namespace")
}

func TestKubernetesRunner_CheckStatus_GetObjectWarningEvents_Forbidden(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockK8s := mockk8s.NewMockKubernetesInterface(ctrl)
	mockVault := mockvault.NewMockVaultClientInterface(ctrl)

	runner, deploymentSummary, _ := createTestKubernetesRunner(t, mockK8s, mockVault)

	ready := int32(0)
	jobStatus := &v1.JobStatus{
		Succeeded: 0,
		Failed:    0,
		Active:    1,
		Ready:     &ready,
		StartTime: &metav1.Time{Time: time.Now().Add(-5 * time.Second)}, // Exceeds delay
	}

	podStatus := &corev1.PodStatus{
		Phase: "Pending",
	}

	mockK8s.EXPECT().CheckJobStatus(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(jobStatus, nil)
	mockK8s.EXPECT().GetPodJob(gomock.Any(), "platform-orchestrator-runner", deploymentSummary.Id.String()).
		Return(&corev1.Pod{Status: *podStatus, ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "platform-orchestrator-runner"}}, nil)
	mockK8s.EXPECT().GetObjectWarningEvents(gomock.Any(), "platform-orchestrator-runner", "test-pod").
		Return(nil, kubernetes.ErrK8sActionForbidden)

	status, err := runner.CheckStatus(context.Background())
	require.NoError(t, err)
	assert.False(t, status.IsCompleted)
	assert.True(t, status.IsStuck)
	assert.Contains(t, status.Message, "job has not started and the runner configuration does not allow to read job events and pods in the target namespace, please check the runner configuration")
}
