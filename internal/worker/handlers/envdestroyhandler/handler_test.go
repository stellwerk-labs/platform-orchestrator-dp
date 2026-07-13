package envdestroyhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hrabbitmq"
	"github.com/stellwerk-labs/golib/hrabbitmq/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardreliableoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platformorchestratorgraph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	mockplatformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratorcp/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	mockmodel "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"
)

func mockHandler(t *testing.T, logDel runners.RunnerLogsDeleter) (*EnvDestroyHandler, func()) {
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

	if logDel == nil {
		logDel = func(ctx context.Context, envUuid string) error { return nil }
	}

	return New(mockplatformorchestratorcp.NewMockClientWithResponsesInterface(ctrl), db, pub, logDel), func() {
		ctrl.Finish()
	}
}

func TestHandleInner_active_env(t *testing.T) {
	h, fin := mockHandler(t, nil)
	defer fin()
	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env").Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
	}, nil)
	require.NoError(t, h.handleInner(t.Context(), zaptest.NewLogger(t), "my-org", "my-proj", "my-env", uuid.Nil, false, false))
}

func TestHandleInner_launch_destroy(t *testing.T) {
	h, fin := mockHandler(t, nil)
	defer fin()

	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env").Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusDeleting},
	}, nil).Times(1)
	lastDep := model.DeploymentSummary{OrgId: "my-org", ProjectId: "my-proj", EnvId: "my-env", Id: uuid.New(), Mode: model.DeploymentModeDeploy, Status: model.DeploymentStatusSucceeded}
	h.db.(*mockmodel.MockDatabaser).EXPECT().ListLastDeployments(gomock.Any(), gomock.Not(nil), "my-org", "", 1, model.ListLastDeploymentsParams{
		ProjectId: opt.Of("my-proj"), EnvId: opt.Of("my-env"), StateChangeOnly: true,
	}).Return([]model.DeploymentSummary{lastDep}, "", nil)
	graph, _ := json.Marshal(&platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig]{})
	h.db.(*mockmodel.MockDatabaser).EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-proj", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
		Return(&lastDep, model.EncodedDeploymentManifest(`{}`), nil, graph, nil)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "some-ns"})
	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200: &platformorchestratorcp.Runner{
			Id:                        "my-runner",
			StateStorageConfiguration: ssc,
		},
	}, nil)
	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-proj", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{AreRulesIgnored: true}).
		Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &platformorchestratorcp.InternalModuleCatalogue{Modules: []platformorchestratorcp.InternalModuleCatalogueModule{}},
		}, nil)
	h.db.(*mockmodel.MockDatabaser).EXPECT().CreateDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-proj", "my-env", gomock.Cond(func(x model.CreateDeploymentParams) bool {
		return assert.Equal(t, model.DeploymentModeDestroy, x.Mode) && assert.Equal(t, "my-runner", x.RunnerId)
	})).Return(&model.DeploymentSummary{
		Id:   uuid.Nil,
		Mode: model.DeploymentModeDestroy,
	}, nil)
	h.db.(*mockmodel.MockDatabaser).EXPECT().InitActiveResourcesFromGraph(gomock.Any(), gomock.Not(nil), uuid.Nil, uuid.Nil, gomock.Not(nil)).Return(nil)
	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().InternalUpdateEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env", platformorchestratorcp.EnvironmentInternalUpdateBody{
		Status:        ref.Ref(platformorchestratorcp.EnvironmentStatusDeleting),
		StatusMessage: ref.Ref("Waiting for destroy deployment '00000000-0000-0000-0000-000000000000' to complete"),
	}).Return(&platformorchestratorcp.InternalUpdateEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
	}, nil)

	require.NoError(t, h.handleInner(t.Context(), zaptest.NewLogger(t), "my-org", "my-proj", "my-env", uuid.Nil, false, false))
}

func TestHandleInner_destroy_in_flight(t *testing.T) {
	h, fin := mockHandler(t, nil)
	defer fin()

	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env").Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusDeleting},
	}, nil)
	lastDep := model.DeploymentSummary{OrgId: "my-org", ProjectId: "my-proj", EnvId: "my-env", Id: uuid.New(), Mode: model.DeploymentModeDestroy, Status: model.DeploymentStatusExecuting}
	h.db.(*mockmodel.MockDatabaser).EXPECT().ListLastDeployments(gomock.Any(), gomock.Not(nil), "my-org", "", 1, model.ListLastDeploymentsParams{
		ProjectId: opt.Of("my-proj"), EnvId: opt.Of("my-env"), StateChangeOnly: true,
	}).Return([]model.DeploymentSummary{lastDep}, "", nil)
	require.NoError(t, h.handleInner(t.Context(), zaptest.NewLogger(t), "my-org", "my-proj", "my-env", uuid.Nil, false, false))
}

func TestHandleInner_destroy_failed(t *testing.T) {
	h, fin := mockHandler(t, nil)
	defer fin()

	depId := uuid.New()

	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env").Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusDeleting, StatusMessage: ref.Ref(fmt.Sprintf("Waiting for destroy deployment '%s' to complete", depId))},
	}, nil)
	lastDep := model.DeploymentSummary{OrgId: "my-org", ProjectId: "my-proj", EnvId: "my-env", Id: depId, Mode: model.DeploymentModeDestroy, Status: model.DeploymentStatusFailed, StatusMessage: "some-error"}
	h.db.(*mockmodel.MockDatabaser).EXPECT().ListLastDeployments(gomock.Any(), gomock.Not(nil), "my-org", "", 1, model.ListLastDeploymentsParams{
		ProjectId: opt.Of("my-proj"), EnvId: opt.Of("my-env"), StateChangeOnly: true,
	}).Return([]model.DeploymentSummary{lastDep}, "", nil)

	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().InternalUpdateEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env", platformorchestratorcp.EnvironmentInternalUpdateBody{
		Status:        ref.Ref(platformorchestratorcp.EnvironmentStatusDeleteFailed),
		StatusMessage: ref.Ref(fmt.Sprintf("destroy deployment '%s' failed. Inspect the deployment status for a cause and retry the DeleteEnvironment operation", depId)),
	}).Return(&platformorchestratorcp.InternalUpdateEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
	}, nil)

	require.NoError(t, h.handleInner(t.Context(), zaptest.NewLogger(t), "my-org", "my-proj", "my-env", uuid.Nil, false, false))
}

func TestHandleInner_destroy_succeeded_delete_rules(t *testing.T) {
	var deleteLogsCalledEnvUuid string
	h, fin := mockHandler(t, func(ctx context.Context, envUuid string) error {
		deleteLogsCalledEnvUuid = envUuid
		return nil
	})
	defer fin()

	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env").Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusDeleting},
	}, nil)
	lastDep := model.DeploymentSummary{OrgId: "my-org", ProjectId: "my-proj", EnvId: "my-env", Id: uuid.New(), Mode: model.DeploymentModeDestroy, Status: model.DeploymentStatusSucceeded}
	h.db.(*mockmodel.MockDatabaser).EXPECT().ListLastDeployments(gomock.Any(), gomock.Not(nil), "my-org", "", 1, model.ListLastDeploymentsParams{
		ProjectId: opt.Of("my-proj"), EnvId: opt.Of("my-env"), StateChangeOnly: true,
	}).Return([]model.DeploymentSummary{lastDep}, "", nil)

	h.db.(*mockmodel.MockDatabaser).EXPECT().DeleteDeploymentsForEnv(gomock.Any(), gomock.Not(nil), "my-org", "my-proj", "my-env", false).Return(nil)
	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().InternalForceDeleteEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env", &platformorchestratorcp.InternalForceDeleteEnvironmentParams{DeleteRules: ref.Ref(true)}).
		Return(&platformorchestratorcp.InternalForceDeleteEnvironmentResponse{HTTPResponse: &http.Response{StatusCode: http.StatusNoContent}}, nil)

	require.NoError(t, h.handleInner(t.Context(), zaptest.NewLogger(t), "my-org", "my-proj", "my-env", uuid.Nil, false, true))
	require.Equal(t, uuid.Nil.String(), deleteLogsCalledEnvUuid)
}

func TestHandleInner_force_destroy_succeeded(t *testing.T) {
	var deleteLogsCalledEnvUuid string
	h, fin := mockHandler(t, func(ctx context.Context, envUuid string) error {
		deleteLogsCalledEnvUuid = envUuid
		return nil
	})
	defer fin()

	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env").Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusDeleting},
	}, nil)
	lastDep := model.DeploymentSummary{OrgId: "my-org", ProjectId: "my-proj", EnvId: "my-env", Id: uuid.New(), Mode: model.DeploymentModeDeploy, Status: model.DeploymentStatusSucceeded}
	h.db.(*mockmodel.MockDatabaser).EXPECT().ListLastDeployments(gomock.Any(), gomock.Not(nil), "my-org", "", 1, model.ListLastDeploymentsParams{
		ProjectId: opt.Of("my-proj"), EnvId: opt.Of("my-env"), StateChangeOnly: true,
	}).Return([]model.DeploymentSummary{lastDep}, "", nil)

	h.db.(*mockmodel.MockDatabaser).EXPECT().DeleteDeploymentsForEnv(gomock.Any(), gomock.Not(nil), "my-org", "my-proj", "my-env", true).Return(nil)
	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().InternalForceDeleteEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env", &platformorchestratorcp.InternalForceDeleteEnvironmentParams{DeleteRules: ref.Ref(false)}).
		Return(&platformorchestratorcp.InternalForceDeleteEnvironmentResponse{HTTPResponse: &http.Response{StatusCode: http.StatusNoContent}}, nil)

	require.NoError(t, h.handleInner(t.Context(), zaptest.NewLogger(t), "my-org", "my-proj", "my-env", uuid.Nil, true, false))
	require.Equal(t, uuid.Nil.String(), deleteLogsCalledEnvUuid)
}
func TestHandleInner_destroy_succeeded_delete_rules_log_del_fail(t *testing.T) {
	var deleteLogsCalledEnvUuid string
	h, fin := mockHandler(t, func(ctx context.Context, envUuid string) error {
		deleteLogsCalledEnvUuid = envUuid
		return fmt.Errorf("log deletion failed")
	})
	defer fin()

	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env").Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusDeleting},
	}, nil)
	lastDep := model.DeploymentSummary{OrgId: "my-org", ProjectId: "my-proj", EnvId: "my-env", Id: uuid.New(), Mode: model.DeploymentModeDestroy, Status: model.DeploymentStatusSucceeded}
	h.db.(*mockmodel.MockDatabaser).EXPECT().ListLastDeployments(gomock.Any(), gomock.Not(nil), "my-org", "", 1, model.ListLastDeploymentsParams{
		ProjectId: opt.Of("my-proj"), EnvId: opt.Of("my-env"), StateChangeOnly: true,
	}).Return([]model.DeploymentSummary{lastDep}, "", nil)

	h.db.(*mockmodel.MockDatabaser).EXPECT().DeleteDeploymentsForEnv(gomock.Any(), gomock.Not(nil), "my-org", "my-proj", "my-env", false).Return(nil)
	h.cpClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().InternalForceDeleteEnvironmentWithResponse(gomock.Any(), "my-org", "my-proj", "my-env", &platformorchestratorcp.InternalForceDeleteEnvironmentParams{DeleteRules: ref.Ref(true)}).
		Return(&platformorchestratorcp.InternalForceDeleteEnvironmentResponse{HTTPResponse: &http.Response{StatusCode: http.StatusNoContent}}, nil)

	require.NoError(t, h.handleInner(t.Context(), zaptest.NewLogger(t), "my-org", "my-proj", "my-env", uuid.Nil, false, true))
	require.Equal(t, uuid.Nil.String(), deleteLogsCalledEnvUuid)
}
