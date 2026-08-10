package runnerstatushandler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	mockplatformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratorcp/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	mockmodel "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"
	mockrunners "github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genevents"
)

const (
	testOrgID    = "test-org"
	testRunnerID = "default"
)

func statusDelivery(t *testing.T, deploymentID uuid.UUID) hmessaging.Delivery {
	t.Helper()
	payload, err := json.Marshal(events.CloudEvent[genevents.RunnerStatusCheckData]{
		SpecVersion: events.CloudEventSpecVersion1{},
		Type:        genevents.EventType(genevents.IoPlatformOrchestratorRunnerCheckStatus),
		Time:        time.Now().UTC(),
		Data: genevents.RunnerStatusCheckData{
			DeploymentId: deploymentID,
			OrgId:        testOrgID,
			RunnerId:     testRunnerID,
		},
	})
	require.NoError(t, err)
	return hmessaging.Delivery{Message: hmessaging.Message{
		ID:      deploymentID.String() + ":runner-status-check",
		Subject: string(genevents.IoPlatformOrchestratorRunnerCheckStatus),
		Data:    payload,
	}}
}

func runningHandler(t *testing.T, deployment *model.DeploymentSummary) (*RunnerStatusHandler, *mockrunners.MockRunnerInterface) {
	t.Helper()
	controller := gomock.NewController(t)
	database := mockmodel.NewMockDatabaser(controller)
	database.EXPECT().GetDeployment(gomock.Any(), nil, testOrgID, deployment.Id, model.GetModeDefault).
		Return(deployment, nil, nil, nil, nil)
	controlPlane := mockplatformorchestratorcp.NewMockClientWithResponsesInterface(controller)
	internalRunner := platformorchestratorcp.InternalRunner{Id: testRunnerID}
	controlPlane.EXPECT().GetInternalRunnerWithResponse(gomock.Any(), testOrgID, testRunnerID).
		Return(&platformorchestratorcp.GetInternalRunnerResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &internalRunner,
		}, nil)
	runnerFactory := mockrunners.NewMockRunnerFactory(controller)
	runner := mockrunners.NewMockRunnerInterface(controller)
	runnerFactory.EXPECT().CreateRunner(gomock.Any(), internalRunner, deployment).Return(runner, nil)
	return New(database, new(hmessaging.RecordingPublisher), controlPlane, runnerFactory), runner
}

func TestHandleRetriesWhileRunnerIsActive(t *testing.T) {
	deployment := &model.DeploymentSummary{
		Id: uuid.New(), OrgId: testOrgID, RunnerId: testRunnerID,
		Status: model.DeploymentStatusExecuting, CreatedAt: time.Now(),
	}
	handler, runner := runningHandler(t, deployment)
	runner.EXPECT().CheckStatus(gomock.Any()).Return(&runners.RunnerStatus{}, nil)

	err := handler.Handle(t.Context(), zaptest.NewLogger(t), statusDelivery(t, deployment.Id))
	retry, ok := hmessaging.AsRetryError(err)
	require.True(t, ok)
	assert.Equal(t, RetryInterval, retry.RetryDelay())
}

func TestHandleFailsStuckRunner(t *testing.T) {
	deployment := &model.DeploymentSummary{
		Id: uuid.New(), OrgId: testOrgID, RunnerId: testRunnerID,
		Status: model.DeploymentStatusExecuting, CreatedAt: time.Now(), Revision: 3,
	}
	handler, runner := runningHandler(t, deployment)
	database := handler.db.(*mockmodel.MockDatabaser)
	transaction := mockmodel.NewMockTxWithCommit(gomock.NewController(t))
	database.EXPECT().BeginTx(gomock.Any(), nil).Return(transaction, nil)
	transaction.EXPECT().Rollback().Return(nil)
	transaction.EXPECT().Commit().Return(nil)
	runner.EXPECT().CheckStatus(gomock.Any()).Return(&runners.RunnerStatus{IsStuck: true, Message: "service account not found"}, nil)
	failed := *deployment
	failed.Status = model.DeploymentStatusFailed
	failed.StatusMessage = "service account not found"
	database.EXPECT().UpdateDeploymentStatusAndOutputs(gomock.Any(), transaction, deployment.Id, model.UpdateDeploymentStatusAndOutputsParams{
		Status: model.DeploymentStatusFailed, StatusMessage: "service account not found",
		ExpectedRevision: opt.Of(int64(3)),
	}).Return(&failed, nil)
	database.EXPECT().CreateDeploymentHistoryRecord(gomock.Any(), transaction, &failed).Return(nil)
	store := new(reliableoutbox.InMemoryStorage[*hstandardoutbox.PendingEventMessage])
	database.EXPECT().InsertPendingEventMessages(gomock.Any(), transaction, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ model.Tx, messages []*hstandardoutbox.PendingEventMessage) ([]*hstandardoutbox.PendingEventMessage, error) {
			store.Put(messages)
			return messages, nil
		})
	database.EXPECT().AsReliableOutboxStore().Return(store)

	require.NoError(t, handler.Handle(t.Context(), zaptest.NewLogger(t), statusDelivery(t, deployment.Id)))
}

func TestHandleStopsAfterDeploymentCompletes(t *testing.T) {
	controller := gomock.NewController(t)
	database := mockmodel.NewMockDatabaser(controller)
	deploymentID := uuid.New()
	database.EXPECT().GetDeployment(gomock.Any(), nil, testOrgID, deploymentID, model.GetModeDefault).
		Return(&model.DeploymentSummary{Id: deploymentID, Status: model.DeploymentStatusSucceeded}, nil, nil, nil, nil)
	handler := New(database, new(hmessaging.RecordingPublisher), nil, nil)
	require.NoError(t, handler.Handle(t.Context(), zaptest.NewLogger(t), statusDelivery(t, deploymentID)))
}

func TestHandleRejectsMalformedEvent(t *testing.T) {
	handler := New(nil, nil, nil, nil)
	err := handler.Handle(t.Context(), zaptest.NewLogger(t), hmessaging.Delivery{Message: hmessaging.Message{Data: []byte("not-json")}})
	require.Error(t, err)
	assert.True(t, hmessaging.IsTerminalError(err))
}
