package createdephandler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/stellwerk-labs/golib/hrabbitmq/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardreliableoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"

	mockplatformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratorcp/mocks"
	usererrors "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	mockmodel "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"
	mockrunners "github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/mocks"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/genevents"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/stellwerk-labs/golib/hrabbitmq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wagslane/go-rabbitmq"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const (
	runnerImage           = "ghcr.io/stellwerk-labs/platform-orchestrator-runner:v1.8.3"
	tokenSalt             = "S4LT"
	orgId                 = "test-org"
	projectId             = "test-app"
	envId                 = "test-env"
	runnerId              = "default"
	runnerCfgVaultPath    = "/platform-orchestrator/orgs/test-org/runners/default"
	runnerCfgVaultVersion = 1
	externalDataplaneUrl  = "http://platform-orchestrator-dp:8080"
)

type ClientFactoryMock struct {
	K8s kubernetes.KubernetesInterface
}

var depId = uuid.New()

func prepUnitTest(t *testing.T) (*CreateDepHandler, *mockmodel.MockDatabaser, *mockplatformorchestratorcp.MockClientWithResponsesInterface, *mockrunners.MockRunnerFactory, *hrabbitmq.NoOpPublisher) {
	ctrl := gomock.NewController(t)
	t.Cleanup(func() {
		ctrl.Finish()
	})
	db := mockmodel.NewMockDatabaser(ctrl)
	tx := mockmodel.NewMockTxWithCommit(ctrl)
	db.EXPECT().BeginTx(gomock.Any(), gomock.Any()).Return(tx, nil).AnyTimes()
	tx.EXPECT().Commit().Return(nil).AnyTimes()
	tx.EXPECT().Rollback().Return(nil).AnyTimes()
	pub := new(hrabbitmq.NoOpPublisher)
	store := new(reliableoutbox.InMemoryStorage[*hstandardreliableoutbox.PendingEventMessage])
	db.EXPECT().AsReliableOutboxStore().Return(store).AnyTimes()
	db.EXPECT().InsertPendingEventMessages(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ model.Tx, m []*hstandardreliableoutbox.PendingEventMessage) ([]*hstandardreliableoutbox.PendingEventMessage, error) {
		store.Put(m)
		return m, nil
	}).AnyTimes()

	cp := mockplatformorchestratorcp.NewMockClientWithResponsesInterface(ctrl)
	mockRunnerFactory := mockrunners.NewMockRunnerFactory(ctrl)

	// Create handler with mock runner factory
	h := &CreateDepHandler{
		db:                 db,
		publisher:          pub,
		controlPlaneClient: cp,
		runnerFactory:      mockRunnerFactory,
		runnerTokenSalt:    tokenSalt,
	}

	return h, db, cp, mockRunnerFactory, pub
}

func buildDepEvent(t *testing.T, orgId string, depId uuid.UUID) []byte {
	out, err := json.Marshal(events.CloudEvent[genevents.DeploymentChangedData]{
		Data: genevents.DeploymentChangedData{
			OrgId:        orgId,
			ProjectId:    projectId,
			EnvId:        envId,
			DeploymentId: depId,
		},
	})
	require.NoError(t, err)
	return out
}

func TestHandle_dep_not_found(t *testing.T) {
	h, db, _, _, _ := prepUnitTest(t)
	db.EXPECT().GetDeployment(gomock.Any(), nil, orgId, depId, model.GetModeDefault).Return(nil, nil, nil, nil, model.NewErrNotFound("deployment not found"))
	require.NoError(t, h.Handle(t.Context(), zaptest.NewLogger(t), &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{
			Body: buildDepEvent(t, orgId, depId),
		},
	}))
}

func TestHandle_runner_not_found(t *testing.T) {
	h, db, cp, _, _ := prepUnitTest(t)
	db.EXPECT().GetDeployment(gomock.Any(), nil, orgId, depId, model.GetModeDefault).Return(&model.DeploymentSummary{
		OrgId:     orgId,
		ProjectId: projectId,
		EnvId:     envId,
		Id:        depId,
		RunnerId:  runnerId,
		Status:    model.DeploymentStatusExecuting,
	}, nil, nil, nil, nil)
	cp.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), orgId, runnerId).
		Return(&platformorchestratorcp.GetInternalRunnerResponse{HTTPResponse: &http.Response{StatusCode: http.StatusNotFound}, JSON404: &platformorchestratorcp.Error{Error: "HTTP-404", Message: "runner not found"}}, nil).Times(1)
	db.EXPECT().UpdateDeploymentStatusAndOutputs(gomock.Any(), gomock.Not(nil), depId, model.UpdateDeploymentStatusAndOutputsParams{
		Status:           model.DeploymentStatusFailed,
		StatusMessage:    "no runner has been configured with id 'default'",
		ExpectedRevision: opt.Of(int64(0)),
	}).Return(&model.DeploymentSummary{}, nil).Times(1)

	db.EXPECT().CreateDeploymentHistoryRecord(gomock.Any(), gomock.Not(nil), gomock.Not(nil)).Return(nil)

	require.NoError(t, h.Handle(t.Context(), zaptest.NewLogger(t), &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{
			Body: buildDepEvent(t, orgId, depId),
		},
	}))
}

func TestHandle_runner_already_running(t *testing.T) {
	h, db, cp, mockRunnerFactory, _ := prepUnitTest(t)

	deploymentSummary := &model.DeploymentSummary{
		OrgId:     orgId,
		ProjectId: projectId,
		EnvId:     envId,
		Id:        depId,
		RunnerId:  runnerId,
		Status:    model.DeploymentStatusExecuting,
	}

	db.EXPECT().GetDeployment(gomock.Any(), nil, orgId, depId, model.GetModeDefault).Return(deploymentSummary, nil, nil, nil, nil)

	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Cluster: platformorchestratorcp.K8sRunnerK8sCluster{
			ClusterData: platformorchestratorcp.K8sRunnerK8sClusterClusterData{
				Server: "my-server",
			},
		},
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
	}))

	internalRunner := platformorchestratorcp.InternalRunner{
		RunnerConfigurationSecret: platformorchestratorcp.ConfigurationSecret{Path: runnerCfgVaultPath, Version: runnerCfgVaultVersion},
		RunnerConfiguration:       *cfg,
	}

	cp.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), orgId, runnerId).Return(
		&platformorchestratorcp.GetInternalRunnerResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &internalRunner,
		}, nil).Times(1)

	// Mock runner that returns true for IsRunning
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRunner := mockrunners.NewMockRunnerInterface(ctrl)
	mockRunner.EXPECT().IsRunning(gomock.Any()).Return(true, nil)

	mockRunnerFactory.EXPECT().CreateRunner(gomock.Any(), internalRunner, deploymentSummary).Return(mockRunner, nil)

	require.NoError(t, h.Handle(t.Context(), zaptest.NewLogger(t), &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{
			Body: buildDepEvent(t, orgId, depId),
		},
	}))
}

func TestHandle_start_runner_error(t *testing.T) {
	h, db, cp, mockRunnerFactory, _ := prepUnitTest(t)

	deploymentSummary := &model.DeploymentSummary{
		OrgId:     orgId,
		ProjectId: projectId,
		EnvId:     envId,
		Id:        depId,
		RunnerId:  runnerId,
		Status:    model.DeploymentStatusExecuting,
	}

	db.EXPECT().GetDeployment(gomock.Any(), nil, orgId, depId, model.GetModeDefault).Return(deploymentSummary, nil, nil, nil, nil)

	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Cluster: platformorchestratorcp.K8sRunnerK8sCluster{
			ClusterData: platformorchestratorcp.K8sRunnerK8sClusterClusterData{
				Server: "my-server",
			},
		},
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
	}))

	internalRunner := platformorchestratorcp.InternalRunner{
		RunnerConfigurationSecret: platformorchestratorcp.ConfigurationSecret{Path: runnerCfgVaultPath, Version: runnerCfgVaultVersion},
		RunnerConfiguration:       *cfg,
	}

	cp.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), orgId, runnerId).Return(
		&platformorchestratorcp.GetInternalRunnerResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &internalRunner,
		}, nil).Times(1)

	// Mock runner that fails on Start
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRunner := mockrunners.NewMockRunnerInterface(ctrl)
	mockRunnerFactory.EXPECT().CreateRunner(gomock.Any(), internalRunner, deploymentSummary).Return(mockRunner, nil)
	mockRunner.EXPECT().IsRunning(gomock.Any()).Return(false, nil)
	mockRunner.EXPECT().Start(gomock.Any()).Return(errors.New("failed to create job"))

	db.EXPECT().UpdateDeploymentStatusAndOutputs(gomock.Any(), gomock.Not(nil), depId, model.UpdateDeploymentStatusAndOutputsParams{
		Status:           model.DeploymentStatusFailed,
		StatusMessage:    "internal failure - please contact support",
		ExpectedRevision: opt.Of(int64(0)),
	}).Return(&model.DeploymentSummary{}, nil).Times(1)

	db.EXPECT().CreateDeploymentHistoryRecord(gomock.Any(), gomock.Not(nil), gomock.Not(nil)).Return(nil)

	require.NoError(t, h.Handle(t.Context(), zaptest.NewLogger(t), &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{
			Body: buildDepEvent(t, orgId, depId),
		},
	}))
}

func TestHandle_start_runner_user_error(t *testing.T) {
	h, db, cp, mockRunnerFactory, _ := prepUnitTest(t)

	deploymentSummary := &model.DeploymentSummary{
		OrgId:     orgId,
		ProjectId: projectId,
		EnvId:     envId,
		Id:        depId,
		RunnerId:  runnerId,
		Status:    model.DeploymentStatusExecuting,
	}

	db.EXPECT().GetDeployment(gomock.Any(), nil, orgId, depId, model.GetModeDefault).Return(deploymentSummary, nil, nil, nil, nil)

	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Cluster: platformorchestratorcp.K8sRunnerK8sCluster{
			ClusterData: platformorchestratorcp.K8sRunnerK8sClusterClusterData{
				Server: "my-server",
			},
		},
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
	}))

	internalRunner := platformorchestratorcp.InternalRunner{
		RunnerConfigurationSecret: platformorchestratorcp.ConfigurationSecret{Path: runnerCfgVaultPath, Version: runnerCfgVaultVersion},
		RunnerConfiguration:       *cfg,
	}

	cp.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), orgId, runnerId).Return(
		&platformorchestratorcp.GetInternalRunnerResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &internalRunner,
		}, nil).Times(1)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRunner := mockrunners.NewMockRunnerInterface(ctrl)
	mockRunnerFactory.EXPECT().CreateRunner(gomock.Any(), internalRunner, deploymentSummary).Return(mockRunner, nil)
	mockRunner.EXPECT().IsRunning(gomock.Any()).Return(false, nil)
	mockRunner.EXPECT().Start(gomock.Any()).Return(usererrors.NewUserError("a user error occurred"))

	db.EXPECT().UpdateDeploymentStatusAndOutputs(gomock.Any(), gomock.Not(nil), depId, model.UpdateDeploymentStatusAndOutputsParams{
		Status:           model.DeploymentStatusFailed,
		StatusMessage:    "a user error occurred",
		ExpectedRevision: opt.Of(int64(0)),
	}).Return(&model.DeploymentSummary{}, nil).Times(1)

	db.EXPECT().CreateDeploymentHistoryRecord(gomock.Any(), gomock.Not(nil), gomock.Not(nil)).Return(nil)

	require.NoError(t, h.Handle(t.Context(), zaptest.NewLogger(t), &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{
			Body: buildDepEvent(t, orgId, depId),
		},
	}))
}

func TestHandle_success(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(func() {
		ctrl.Finish()
	})

	h, db, cp, mockRunnerFactory, pub := prepUnitTest(t)
	deploymentSummary := &model.DeploymentSummary{
		OrgId:                     orgId,
		ProjectId:                 projectId,
		EnvId:                     envId,
		Id:                        depId,
		RunnerId:                  runnerId,
		Status:                    model.DeploymentStatusExecuting,
		EncryptedOutputsRecipient: opt.Of("outputs-key"),
		EncryptedLogsRecipient:    opt.Of("logs-key"),
	}

	db.EXPECT().GetDeployment(gomock.Any(), nil, orgId, depId, model.GetModeDefault).Return(deploymentSummary, nil, nil, nil, nil)

	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Cluster: platformorchestratorcp.K8sRunnerK8sCluster{
			ClusterData: platformorchestratorcp.K8sRunnerK8sClusterClusterData{
				Server: "my-server",
			},
		},
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
	}))

	internalRunner := platformorchestratorcp.InternalRunner{
		RunnerConfigurationSecret: platformorchestratorcp.ConfigurationSecret{Path: runnerCfgVaultPath, Version: runnerCfgVaultVersion},
		RunnerConfiguration:       *cfg,
	}

	cp.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), orgId, runnerId).Return(
		&platformorchestratorcp.GetInternalRunnerResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &internalRunner,
		}, nil).Times(1)

	// Mock runner that succeeds
	mockRunner := mockrunners.NewMockRunnerInterface(ctrl)
	mockRunner.EXPECT().IsRunning(gomock.Any()).Return(false, nil)
	mockRunner.EXPECT().Start(gomock.Any()).Return(nil)

	mockRunnerFactory.EXPECT().CreateRunner(gomock.Any(), internalRunner, deploymentSummary).Return(mockRunner, nil)

	require.NoError(t, h.Handle(t.Context(), zaptest.NewLogger(t), &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{
			Body: buildDepEvent(t, orgId, depId),
		},
	}))

	if assert.Len(t, pub.Recorded, 1) {
		m := pub.Recorded[0]
		assert.Equal(t, []string{string(genevents.IoPlatformOrchestratorRunnerCheckStatus)}, m.Keys)
		assert.Equal(t, events.DefaultExchange, m.Options.Exchange)
		var statusCheckEvent events.CloudEvent[genevents.RunnerStatusCheckData]

		assert.NoError(t, json.Unmarshal(m.Data, &statusCheckEvent))
		require.Equal(t, depId, statusCheckEvent.Data.DeploymentId)
		require.Equal(t, orgId, statusCheckEvent.Data.OrgId)
		require.Equal(t, runnerId, statusCheckEvent.Data.RunnerId)
	}
}

func TestHandle_start_runner_kubernetes_agent_not_reachable_retry(t *testing.T) {
	h, db, cp, mockRunnerFactory, _ := prepUnitTest(t)

	deploymentSummary := &model.DeploymentSummary{
		OrgId:     orgId,
		ProjectId: projectId,
		EnvId:     envId,
		Id:        depId,
		RunnerId:  runnerId,
		Status:    model.DeploymentStatusExecuting,
		CreatedAt: time.Now(), // Recent deployment to be within tolerance
	}

	db.EXPECT().GetDeployment(gomock.Any(), nil, orgId, depId, model.GetModeDefault).Return(deploymentSummary, nil, nil, nil, nil)

	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Cluster: platformorchestratorcp.K8sRunnerK8sCluster{
			ClusterData: platformorchestratorcp.K8sRunnerK8sClusterClusterData{
				Server: "my-server",
			},
		},
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
	}))

	internalRunner := platformorchestratorcp.InternalRunner{
		RunnerConfigurationSecret: platformorchestratorcp.ConfigurationSecret{Path: runnerCfgVaultPath, Version: runnerCfgVaultVersion},
		RunnerConfiguration:       *cfg,
	}

	cp.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), orgId, runnerId).Return(
		&platformorchestratorcp.GetInternalRunnerResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &internalRunner,
		}, nil).Times(1)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRunner := mockrunners.NewMockRunnerInterface(ctrl)
	mockRunnerFactory.EXPECT().CreateRunner(gomock.Any(), internalRunner, deploymentSummary).Return(mockRunner, nil)
	mockRunner.EXPECT().IsRunning(gomock.Any()).Return(false, nil)
	mockRunner.EXPECT().Start(gomock.Any()).Return(runners.ErrKubernetesAgentNotReachableRetry)

	err := h.Handle(t.Context(), zaptest.NewLogger(t), &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{
			Body: buildDepEvent(t, orgId, depId),
		},
	})

	require.Error(t, err)

	assert.Contains(t, err.Error(), "kubernetes agent temporary not reachable, will retry")
}

func TestHandle_start_runner_kubernetes_agent_not_reachable_tolerance_exceeded(t *testing.T) {
	h, db, cp, mockRunnerFactory, _ := prepUnitTest(t)

	// Deployment created 3 minutes ago (exceeds 120 second tolerance)
	deploymentSummary := &model.DeploymentSummary{
		OrgId:     orgId,
		ProjectId: projectId,
		EnvId:     envId,
		Id:        depId,
		RunnerId:  runnerId,
		Status:    model.DeploymentStatusExecuting,
		CreatedAt: time.Now().Add(-3 * time.Minute),
	}

	db.EXPECT().GetDeployment(gomock.Any(), nil, orgId, depId, model.GetModeDefault).Return(deploymentSummary, nil, nil, nil, nil)

	cfg := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, cfg.FromK8sRunnerConfiguration(platformorchestratorcp.K8sRunnerConfiguration{
		Cluster: platformorchestratorcp.K8sRunnerK8sCluster{
			ClusterData: platformorchestratorcp.K8sRunnerK8sClusterClusterData{
				Server: "my-server",
			},
		},
		Job: platformorchestratorcp.K8sRunnerJobConfig{
			Namespace:      "platform-orchestrator-runner",
			ServiceAccount: "platform-orchestrator-runner",
		},
	}))

	internalRunner := platformorchestratorcp.InternalRunner{
		Id:                        runnerId,
		RunnerConfigurationSecret: platformorchestratorcp.ConfigurationSecret{Path: runnerCfgVaultPath, Version: runnerCfgVaultVersion},
		RunnerConfiguration:       *cfg,
	}

	cp.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), orgId, runnerId).Return(
		&platformorchestratorcp.GetInternalRunnerResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &internalRunner,
		}, nil).Times(1)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRunner := mockrunners.NewMockRunnerInterface(ctrl)
	mockRunnerFactory.EXPECT().CreateRunner(gomock.Any(), internalRunner, deploymentSummary).Return(mockRunner, nil)
	mockRunner.EXPECT().IsRunning(gomock.Any()).Return(false, nil)
	mockRunner.EXPECT().Start(gomock.Any()).Return(runners.ErrKubernetesAgentNotReachableRetry)

	// Should fail deployment since tolerance is exceeded
	db.EXPECT().UpdateDeploymentStatusAndOutputs(gomock.Any(), gomock.Not(nil), depId, model.UpdateDeploymentStatusAndOutputsParams{
		Status:           model.DeploymentStatusFailed,
		StatusMessage:    `kubernetes-agent runner "default" not reachable, please check your network connectivity and configuration: kubernetes agent not reachable`,
		ExpectedRevision: opt.Of(int64(0)),
	}).Return(&model.DeploymentSummary{}, nil).Times(1)

	db.EXPECT().CreateDeploymentHistoryRecord(gomock.Any(), gomock.Not(nil), gomock.Not(nil)).Return(nil)

	require.NoError(t, h.Handle(t.Context(), zaptest.NewLogger(t), &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{
			Body: buildDepEvent(t, orgId, depId),
		},
	}))
}
