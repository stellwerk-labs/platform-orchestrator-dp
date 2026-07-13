package runnerstatushandler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rabbitmq/amqp091-go"
	"github.com/stellwerk-labs/golib/hrabbitmq"
	v2 "github.com/stellwerk-labs/golib/hrabbitmq/delayqueues/v2"
	"github.com/stellwerk-labs/golib/hrabbitmq/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardreliableoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wagslane/go-rabbitmq"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	mockplatformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratorcp/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	mockmodel "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"
	mockrunners "github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/mocks"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/genevents"
)

const (
	testOrgId    = "test-org"
	testRunnerId = "default"
)

var (
	testDeploymentId = uuid.New()
)

func mockHandler(t *testing.T) (*RunnerStatusHandler, func(), runners.RunnerInterface) {
	ctrl := gomock.NewController(t)

	db := mockmodel.NewMockDatabaser(ctrl)
	tx := mockmodel.NewMockTxWithCommit(ctrl)
	db.EXPECT().BeginTx(gomock.Any(), gomock.Any()).Return(tx, nil).AnyTimes()
	tx.EXPECT().Commit().Return(nil).AnyTimes()
	tx.EXPECT().Rollback().Return(nil).AnyTimes()
	pub := new(hrabbitmq.NoOpPublisher)

	store := new(reliableoutbox.InMemoryStorage[*hstandardreliableoutbox.PendingEventMessage])
	db.EXPECT().InsertPendingEventMessages(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, m []*hstandardreliableoutbox.PendingEventMessage) ([]*hstandardreliableoutbox.PendingEventMessage, error) {
			store.Put(m)
			return m, nil
		}).AnyTimes()
	db.EXPECT().AsReliableOutboxStore().Return(store).AnyTimes()

	mockRunnerFactory := mockrunners.NewMockRunnerFactory(ctrl)
	mockRunner := mockrunners.NewMockRunnerInterface(ctrl)
	mockRunnerFactory.EXPECT().CreateRunner(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRunner, nil).AnyTimes()
	return New(db, pub, mockplatformorchestratorcp.NewMockClientWithResponsesInterface(ctrl), mockRunnerFactory), func() {
		ctrl.Finish()
	}, mockRunner
}

func TestRunnerStatusHandler_Handle_StillRunning(t *testing.T) {
	handler, fin, runner := mockHandler(t)
	defer fin()
	mockCP := handler.controlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	mockRunner := runner.(*mockrunners.MockRunnerInterface)
	mockDB := handler.db.(*mockmodel.MockDatabaser)

	deployment := &model.DeploymentSummary{
		Id:        testDeploymentId,
		OrgId:     testOrgId,
		RunnerId:  testRunnerId,
		Status:    model.DeploymentStatusExecuting,
		CreatedAt: time.Now().Add(-30 * time.Minute),
	}

	eventData := genevents.RunnerStatusCheckData{
		DeploymentId: testDeploymentId,
		OrgId:        testOrgId,
		RunnerId:     testRunnerId,
	}

	cloudEvent := events.CloudEvent[genevents.RunnerStatusCheckData]{
		SpecVersion: events.CloudEventSpecVersion1{},
		Type:        genevents.EventType(genevents.IoPlatformOrchestratorRunnerCheckStatus),
		Time:        time.Now(),
		Data:        eventData,
	}

	payload, err := json.Marshal(cloudEvent)
	require.NoError(t, err)

	delivery := &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{Body: payload},
	}

	mockDB.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), testOrgId, testDeploymentId, model.GetModeDefault).
		Return(deployment, nil, nil, nil, nil)

	mockCP.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), testOrgId, testRunnerId).
		Return(&platformorchestratorcp.GetInternalRunnerResponse{JSON200: &platformorchestratorcp.InternalRunner{}, HTTPResponse: &http.Response{StatusCode: 200}}, nil)

	runnerStatus := &runners.RunnerStatus{
		IsCompleted: false,
		IsStuck:     false,
		Message:     "Still processing",
	}
	mockRunner.EXPECT().CheckStatus(gomock.Any()).Return(runnerStatus, nil)

	var gracefulRetryErr v2.GracefulRetryError
	assert.ErrorAs(t, handler.Handle(context.Background(), zaptest.NewLogger(t), delivery), &gracefulRetryErr)
}

func TestRunnerStatusHandler_Handle_RunnerStuck(t *testing.T) {
	handler, fin, runner := mockHandler(t)
	defer fin()
	mockCP := handler.controlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	mockRunner := runner.(*mockrunners.MockRunnerInterface)
	mockDB := handler.db.(*mockmodel.MockDatabaser)

	deployment := &model.DeploymentSummary{
		Id:        testDeploymentId,
		OrgId:     testOrgId,
		RunnerId:  testRunnerId,
		Status:    model.DeploymentStatusExecuting,
		CreatedAt: time.Now().Add(-30 * time.Minute),
		Revision:  1,
	}

	eventData := genevents.RunnerStatusCheckData{
		DeploymentId: testDeploymentId,
		OrgId:        testOrgId,
		RunnerId:     testRunnerId,
	}

	cloudEvent := events.CloudEvent[genevents.RunnerStatusCheckData]{
		SpecVersion: events.CloudEventSpecVersion1{},
		Type:        genevents.EventType(genevents.IoPlatformOrchestratorRunnerCheckStatus),
		Time:        time.Now(),
		Data:        eventData,
	}

	payload, err := json.Marshal(cloudEvent)
	require.NoError(t, err)

	delivery := &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{Body: payload},
	}

	mockDB.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), testOrgId, testDeploymentId, model.GetModeDefault).
		Return(deployment, nil, nil, nil, nil)

	mockCP.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), testOrgId, testRunnerId).
		Return(&platformorchestratorcp.GetInternalRunnerResponse{JSON200: &platformorchestratorcp.InternalRunner{}, HTTPResponse: &http.Response{StatusCode: 200}}, nil)

	runnerStatus := &runners.RunnerStatus{
		IsCompleted: true,
		IsStuck:     true,
		Message:     "Runner failed due to error",
	}
	mockRunner.EXPECT().CheckStatus(gomock.Any()).Return(runnerStatus, nil)

	mockDB.EXPECT().UpdateDeploymentStatusAndOutputs(gomock.Any(), gomock.Any(), testDeploymentId, model.UpdateDeploymentStatusAndOutputsParams{
		Status:           model.DeploymentStatusFailed,
		StatusMessage:    runnerStatus.Message,
		ExpectedRevision: opt.Of(int64(deployment.Revision)),
	}).Return(deployment, nil)

	mockDB.EXPECT().CreateDeploymentHistoryRecord(gomock.Any(), gomock.Any(), deployment).Return(nil)

	assert.NoError(t, handler.Handle(context.Background(), zaptest.NewLogger(t), delivery))
}

func TestRunnerStatusHandler_Handle_CheckStatus_Error(t *testing.T) {
	handler, fin, runner := mockHandler(t)
	defer fin()
	mockCP := handler.controlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	mockRunner := runner.(*mockrunners.MockRunnerInterface)
	mockDB := handler.db.(*mockmodel.MockDatabaser)

	deployment := &model.DeploymentSummary{
		Id:        testDeploymentId,
		OrgId:     testOrgId,
		RunnerId:  testRunnerId,
		Status:    model.DeploymentStatusExecuting,
		CreatedAt: time.Now().Add(-30 * time.Minute),
		Revision:  1,
	}

	eventData := genevents.RunnerStatusCheckData{
		DeploymentId: testDeploymentId,
		OrgId:        testOrgId,
		RunnerId:     testRunnerId,
	}

	cloudEvent := events.CloudEvent[genevents.RunnerStatusCheckData]{
		SpecVersion: events.CloudEventSpecVersion1{},
		Type:        genevents.EventType(genevents.IoPlatformOrchestratorRunnerCheckStatus),
		Time:        time.Now(),
		Data:        eventData,
	}

	payload, err := json.Marshal(cloudEvent)
	require.NoError(t, err)

	delivery := &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{Body: payload},
	}

	mockDB.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), testOrgId, testDeploymentId, model.GetModeDefault).
		Return(deployment, nil, nil, nil, nil)

	mockCP.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), testOrgId, testRunnerId).
		Return(&platformorchestratorcp.GetInternalRunnerResponse{JSON200: &platformorchestratorcp.InternalRunner{}, HTTPResponse: &http.Response{StatusCode: 200}}, nil)

	mockRunner.EXPECT().CheckStatus(gomock.Any()).Return(nil, errors.New("job can't be retrieved"))

	mockDB.EXPECT().UpdateDeploymentStatusAndOutputs(gomock.Any(), gomock.Any(), testDeploymentId, model.UpdateDeploymentStatusAndOutputsParams{
		Status:           model.DeploymentStatusFailed,
		StatusMessage:    "failed to check runner status: job can't be retrieved",
		ExpectedRevision: opt.Of(int64(deployment.Revision)),
	}).Return(deployment, nil)

	mockDB.EXPECT().CreateDeploymentHistoryRecord(gomock.Any(), gomock.Any(), deployment).Return(nil)

	assert.NoError(t, handler.Handle(context.Background(), zaptest.NewLogger(t), delivery))
}

func TestRunnerStatusHandler_Handle_DeploymentNotFound(t *testing.T) {
	handler, fin, _ := mockHandler(t)
	defer fin()
	mockDB := handler.db.(*mockmodel.MockDatabaser)

	eventData := genevents.RunnerStatusCheckData{
		DeploymentId: testDeploymentId,
		OrgId:        testOrgId,
		RunnerId:     testRunnerId,
	}

	cloudEvent := events.CloudEvent[genevents.RunnerStatusCheckData]{
		SpecVersion: events.CloudEventSpecVersion1{},
		Type:        genevents.EventType(genevents.IoPlatformOrchestratorRunnerCheckStatus),
		Time:        time.Now(),
		Data:        eventData,
	}

	payload, err := json.Marshal(cloudEvent)
	require.NoError(t, err)

	delivery := &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{Body: payload},
	}

	mockDB.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), testOrgId, testDeploymentId, model.GetModeDefault).
		Return(nil, nil, nil, nil, model.NewErrNotFound("deployment"))

	assert.NoError(t, handler.Handle(context.Background(), zaptest.NewLogger(t), delivery))
}

func TestRunnerStatusHandler_Handle_DeploymentNotExecuting(t *testing.T) {
	handler, fin, _ := mockHandler(t)
	defer fin()

	mockDB := handler.db.(*mockmodel.MockDatabaser)

	// Deployment already completed
	deployment := &model.DeploymentSummary{
		Id:        testDeploymentId,
		OrgId:     testOrgId,
		RunnerId:  testRunnerId,
		Status:    model.DeploymentStatusSucceeded, // Not executing
		CreatedAt: time.Now().Add(-30 * time.Minute),
	}

	eventData := genevents.RunnerStatusCheckData{
		DeploymentId: testDeploymentId,
		OrgId:        testOrgId,
		RunnerId:     testRunnerId,
	}

	cloudEvent := events.CloudEvent[genevents.RunnerStatusCheckData]{
		SpecVersion: events.CloudEventSpecVersion1{},
		Type:        genevents.EventType(genevents.IoPlatformOrchestratorRunnerCheckStatus),
		Time:        time.Now(),
		Data:        eventData,
	}

	payload, err := json.Marshal(cloudEvent)
	require.NoError(t, err)

	delivery := &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{Body: payload},
	}

	mockDB.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), testOrgId, testDeploymentId, model.GetModeDefault).
		Return(deployment, nil, nil, nil, nil)

	assert.NoError(t, handler.Handle(context.Background(), zaptest.NewLogger(t), delivery))
}

func TestRunnerStatusHandler_Handle_CheckStatus_KubernetesAgentNotReachableRetry(t *testing.T) {
	handler, fin, runner := mockHandler(t)
	defer fin()
	mockCP := handler.controlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	mockRunner := runner.(*mockrunners.MockRunnerInterface)
	mockDB := handler.db.(*mockmodel.MockDatabaser)

	deployment := &model.DeploymentSummary{
		Id:        testDeploymentId,
		OrgId:     testOrgId,
		RunnerId:  testRunnerId,
		Status:    model.DeploymentStatusExecuting,
		CreatedAt: time.Now().Add(-30 * time.Minute),
		Revision:  1,
	}

	eventData := genevents.RunnerStatusCheckData{
		DeploymentId: testDeploymentId,
		OrgId:        testOrgId,
		RunnerId:     testRunnerId,
	}

	cloudEvent := events.CloudEvent[genevents.RunnerStatusCheckData]{
		SpecVersion: events.CloudEventSpecVersion1{},
		Type:        genevents.EventType(genevents.IoPlatformOrchestratorRunnerCheckStatus),
		Time:        time.Now(),
		Data:        eventData,
	}

	payload, err := json.Marshal(cloudEvent)
	require.NoError(t, err)

	delivery := &rabbitmq.Delivery{
		Delivery: amqp091.Delivery{Body: payload},
	}

	mockDB.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), testOrgId, testDeploymentId, model.GetModeDefault).
		Return(deployment, nil, nil, nil, nil)

	mockCP.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), testOrgId, testRunnerId).
		Return(&platformorchestratorcp.GetInternalRunnerResponse{JSON200: &platformorchestratorcp.InternalRunner{}, HTTPResponse: &http.Response{StatusCode: 200}}, nil)

	mockRunner.EXPECT().CheckStatus(gomock.Any()).Return(nil, runners.ErrKubernetesAgentNotReachableRetry)

	err = handler.Handle(context.Background(), zaptest.NewLogger(t), delivery)
	require.Error(t, err)

	// Check that it's a GracefulRetryError
	var gracefulRetryErr v2.GracefulRetryError
	require.ErrorAs(t, err, &gracefulRetryErr)
	assert.Contains(t, err.Error(), "kubernetes agent temporary not reachable")
}
