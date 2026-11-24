package api

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/herrors"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	platformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/mock/gomock"

	mockplatformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratorcp/mocks"
	mockplatformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratoriam/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	mockmodel "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/completionhooks"
)

const sampleLogs = "test log content"
const orgId = "test-org"

func TestApplyRunnerErrorRecordsAndFailsDeploymentAtomically(t *testing.T) {
	_, server, finish := MockServer(t)
	defer finish()
	database := server.Database.(*mockmodel.MockDatabaser)
	deploymentID := uuid.New()
	deployment := &model.DeploymentSummary{Id: deploymentID, OrgId: orgId, RunnerId: "runner-1", Status: model.DeploymentStatusExecuting}
	database.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), orgId, deploymentID, model.GetModeForUpdate).
		Return(deployment, nil, nil, nil, nil)
	database.EXPECT().TryRecordRunnerEvent(gomock.Any(), gomock.Any(), "terminal-event", orgId, "runner-1", deploymentID, "runner-error").Return(true, nil)
	database.EXPECT().UpdateDeploymentStatusAndOutputs(gomock.Any(), gomock.Any(), deploymentID, gomock.Cond(func(parameters model.UpdateDeploymentStatusAndOutputsParams) bool {
		return parameters.Status == model.DeploymentStatusFailed && parameters.StatusMessage == "command expired before execution"
	})).Return(deployment, nil)
	database.EXPECT().CreateDeploymentHistoryRecord(gomock.Any(), gomock.Any(), deployment).Return(nil)
	store := new(reliableoutbox.InMemoryStorage[*hstandardoutbox.PendingEventMessage])
	database.EXPECT().InsertPendingEventMessages(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ model.Tx, messages []*hstandardoutbox.PendingEventMessage) ([]*hstandardoutbox.PendingEventMessage, error) {
		store.Put(messages)
		return messages, nil
	})
	database.EXPECT().AsReliableOutboxStore().Return(store)

	require.NoError(t, server.ApplyRunnerError(t.Context(), orgId, "runner-1", deploymentID, "terminal-event", "COMMAND_EXPIRED", "command expired before execution"))
}

func TestApplyRunnerErrorDuplicateIsNoOp(t *testing.T) {
	_, server, finish := MockServer(t)
	defer finish()
	database := server.Database.(*mockmodel.MockDatabaser)
	deploymentID := uuid.New()
	deployment := &model.DeploymentSummary{Id: deploymentID, OrgId: orgId, RunnerId: "runner-1", Status: model.DeploymentStatusExecuting}
	database.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), orgId, deploymentID, model.GetModeForUpdate).
		Return(deployment, nil, nil, nil, nil)
	database.EXPECT().TryRecordRunnerEvent(gomock.Any(), gomock.Any(), "terminal-event", orgId, "runner-1", deploymentID, "runner-error").Return(false, nil)

	require.NoError(t, server.ApplyRunnerError(t.Context(), orgId, "runner-1", deploymentID, "terminal-event", "COMMAND_EXPIRED", "command expired before execution"))
}

func TestCreateDeployment_fail_env_deleting(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", gomock.Any()).Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusDeleting},
	}, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {
						Resources: map[string]DeploymentManifestResource{
							"first": {Type: "postgres"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment409JSONResponse{}, r, "unexpected %v", r)
	r2 := r.(CreateDeployment409JSONResponse)
	if assert.NotNil(t, r2) {
		assert.Equal(t, fmt.Sprintf("environment is in status '%s'", platformorchestratorcp.EnvironmentStatusDeleting), r2.Message)
	}
}

func TestCreateDeployment_age_keys(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", "my-env").Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, Status: platformorchestratorcp.EnvironmentStatusActive},
	}, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {},
				},
			},
			EncryptedOutputsRecipient: ref.Ref("bananas"),
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment400JSONResponse{}, r, "unexpected %v", r)
	r400 := r.(CreateDeployment400JSONResponse)
	if assert.NotNil(t, r400) {
		assert.Equal(t, "encrypted_outputs_recipient: failed to parse 'bananas' as age public key", r400.Message)
	}
}

func TestCreateDeployment_authorization_failure(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)

	// First call: deployment_write environment check fails with 403
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{environmentCheck(envUuid, PermissionDeploymentWrite)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
		JSON403: &platformorchestratoriam.Error{
			Error:   "HTTP-403",
			Message: "user cannot write to environment",
			Details: &map[string]interface{}{
				"message": "user cannot write to environment",
			},
		},
	}, nil).Times(1)

	// Second call (fallback): deployment_write organization check also fails with 403
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{orgCheck("my-org", PermissionDeploymentWrite)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
		JSON403: &platformorchestratoriam.Error{
			Error:   "HTTP-403",
			Message: "user cannot write to organization",
			Details: &map[string]interface{}{
				"message": "user cannot write to organization",
			},
		},
	}, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", "my-env").
		Return(&platformorchestratorcp.GetEnvironmentResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: envUuid, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
		}, nil).Times(1)

	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {
						Resources: map[string]DeploymentManifestResource{
							"first": {Type: "postgres"},
						},
					},
				},
			},
		},
	})
	// Authorization failure should return an error, not a response
	require.Error(t, err)
	require.Nil(t, r)
	// Check it's a PlatformOrchestratorError with status 403
	var herr *herrors.PlatformOrchestratorError
	if assert.ErrorAs(t, err, &herr) {
		assert.Equal(t, http.StatusForbidden, herr.StatusCode)
		assert.NotNil(t, herr.Details)
		assert.Contains(t, herr.Details, "message")
	}
}

func TestWaitForDeploymentComplete_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(nil, nil, nil, nil, model.NewErrNotFound("deployment not found"))

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete404JSONResponse{}, r) {
		r404 := r.(WaitForDeploymentComplete404JSONResponse)
		assert.Equal(t, "deployment not found", r404.Message)
	}
}

func TestWaitForDeploymentComplete_already_complete(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(&model.DeploymentSummary{
		Status:            model.DeploymentStatusSucceeded,
		DeploymentEnvUuid: envUuid,
	}, nil, nil, nil, nil).Times(2)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete200JSONResponse{}, r) {
		r200 := r.(WaitForDeploymentComplete200JSONResponse)
		assert.Equal(t, "succeeded", r200.Status)
	}
}

func TestWaitForDeploymentComplete_req_timeout(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).DoAndReturn(func(_ context.Context, _ model.Tx, _ string, _ uuid.UUID, _ model.GetMode) (*model.DeploymentSummary, model.EncodedDeploymentManifest, []byte, model.EncodedDeploymentGraph, error) {
		go cancel()
		return &model.DeploymentSummary{
			Status:            model.DeploymentStatusExecuting,
			DeploymentEnvUuid: envUuid,
		}, nil, nil, nil, nil
	}).Times(2)

	ctx = context.WithValue(ctx, hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete408JSONResponse{}, r) {
		r408 := r.(WaitForDeploymentComplete408JSONResponse)
		assert.Equal(t, "deployment has not completed yet", r408.Message)
	}
}

func TestWaitForDeploymentComplete_event_trigger(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	counter := atomic.NewInt32(0)

	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).DoAndReturn(func(_ context.Context, _ model.Tx, _ string, _ uuid.UUID, _ model.GetMode) (*model.DeploymentSummary, model.EncodedDeploymentManifest, []byte, model.EncodedDeploymentGraph, error) {
		if counter.Inc() == 1 {
			go s.DeploymentCompletedHooks.Notify(completionhooks.DeploymentOrgAndId{OrgId: "my-org", DeploymentId: depId.String()}, struct{}{})
			return &model.DeploymentSummary{
				Status:            model.DeploymentStatusExecuting,
				DeploymentEnvUuid: envUuid,
			}, nil, nil, nil, nil
		}
		return &model.DeploymentSummary{
			Status:            model.DeploymentStatusSucceeded,
			DeploymentEnvUuid: envUuid,
		}, nil, nil, nil, nil
	}).Times(2)

	ctx = context.WithValue(ctx, hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete200JSONResponse{}, r) {
		r200 := r.(WaitForDeploymentComplete200JSONResponse)
		assert.Equal(t, "succeeded", r200.Status)
	}
}

func TestWaitForDeploymentComplete_max_waiters(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)

	s.DeploymentCompletedHooks = &completionhooks.CompletionHooks[completionhooks.DeploymentOrgAndId, struct{}]{MaximumWaitersPerHandle: 1}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	counter := atomic.NewInt32(0)

	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).DoAndReturn(func(_ context.Context, _ model.Tx, _ string, _ uuid.UUID, _ model.GetMode) (*model.DeploymentSummary, model.EncodedDeploymentManifest, []byte, model.EncodedDeploymentGraph, error) {
		if counter.Inc() == 2 {
			go func() {
				_, fin = s.DeploymentCompletedHooks.AddWaiter(completionhooks.DeploymentOrgAndId{OrgId: "my-org", DeploymentId: depId.String()})
				defer fin()
			}()
		}
		return &model.DeploymentSummary{
			Status:            model.DeploymentStatusExecuting,
			DeploymentEnvUuid: envUuid,
		}, nil, nil, nil, nil
	}).Times(3)

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{environmentCheck(envUuid, PermissionDeploymentWrite)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx = context.WithValue(ctx, hecho.ContextKeyUserID, userId.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete408JSONResponse{}, r) {
		r408 := r.(WaitForDeploymentComplete408JSONResponse)
		assert.Equal(t, "deployment has not completed yet", r408.Message)
	}
}

func TestWaitForDeploymentComplete_authorization_fallback_to_org(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)

	// First call: deployment_write environment check fails with 403
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{environmentCheck(envUuid, PermissionDeploymentWrite)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
		JSON403: &platformorchestratoriam.Error{
			Error:   "HTTP-403",
			Message: "user cannot write to environment",
			Details: &map[string]interface{}{
				"message": "user cannot write to environment",
			},
		},
	}, nil).Times(1)

	// Second call (fallback): deployment_write organization check succeeds with 204
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{orgCheck("my-org", PermissionDeploymentWrite)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)

	// First GetDeployment call for authorization check
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(&model.DeploymentSummary{
		Status:            model.DeploymentStatusSucceeded,
		DeploymentEnvUuid: envUuid,
		ProjectId:         "my-project",
		EnvId:             "my-env",
	}, nil, nil, nil, nil).Times(1)

	// Second GetDeployment call in the wait loop
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(&model.DeploymentSummary{
		Status:            model.DeploymentStatusSucceeded,
		DeploymentEnvUuid: envUuid,
		ProjectId:         "my-project",
		EnvId:             "my-env",
	}, nil, nil, nil, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete200JSONResponse{}, r) {
		r200 := r.(WaitForDeploymentComplete200JSONResponse)
		assert.Equal(t, "succeeded", r200.Status)
	}
}

func TestWaitForDeploymentComplete_authorization_failure(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)

	// First call: deployment_write environment check fails with 403
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{environmentCheck(envUuid, PermissionDeploymentWrite)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
		JSON403: &platformorchestratoriam.Error{
			Error:   "HTTP-403",
			Message: "user cannot write to environment",
			Details: &map[string]interface{}{
				"message": "user cannot write to environment",
			},
		},
	}, nil).Times(1)

	// Second call (fallback): deployment_write organization check also fails with 403
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{orgCheck("my-org", PermissionDeploymentWrite)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
		JSON403: &platformorchestratoriam.Error{
			Error:   "HTTP-403",
			Message: "user cannot write to organization",
			Details: &map[string]interface{}{
				"message": "user cannot write to organization",
			},
		},
	}, nil).Times(1)

	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(&model.DeploymentSummary{
		Status:            model.DeploymentStatusSucceeded,
		DeploymentEnvUuid: envUuid,
		ProjectId:         "my-project",
		EnvId:             "my-env",
	}, nil, nil, nil, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	// Authorization failure should return an error, not a response
	require.Error(t, err)
	require.Nil(t, r)
	// Check it's a PlatformOrchestratorError with status 403
	var herr *herrors.PlatformOrchestratorError
	if assert.ErrorAs(t, err, &herr) {
		assert.Equal(t, http.StatusForbidden, herr.StatusCode)
		assert.NotNil(t, herr.Details)
		assert.Contains(t, herr.Details, "message")
	}
}

func TestGetDeployment_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	userId := userid.NewHumanUserId()
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	mdb := s.Database.(*mockmodel.MockDatabaser)
	deploymentId := uuid.New()
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", deploymentId, model.GetModeDefault).
		Return(nil, nil, nil, nil, model.NewErrNotFound("deployment not found"))

	r, err := s.GetDeployment(ctx, GetDeploymentRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeployment404JSONResponse{}, r, "unexpected %v", r)
	r404 := r.(GetDeployment404JSONResponse)
	if assert.NotNil(t, r404) {
		assert.Equal(t, "deployment not found", r404.Message)
	}
}

func TestGetDeploymentBundle(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	depId := uuid.New()

	rawGraph, _ := graphs.ToJson(&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			{Type: "workload", Class: "default", Id: "workloads.one"}: {
				ModuleConfiguration: nil,
			},
			{Type: "thing", Class: "default", Id: "workloads.one.a"}: {
				ModuleConfiguration: &graphs.GraphNodeModuleConfig{
					DefinitionId: "thing1", VersionId: "v1",
				},
			},
			{Type: "thing", Class: "default", Id: "workloads.one.b"}: {
				ModuleConfiguration: &graphs.GraphNodeModuleConfig{
					DefinitionId: "thing1", VersionId: "v2",
					HasInlineSource: true,
				},
			},
		},
	})

	s.Database.(*mockmodel.MockDatabaser).EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(&model.DeploymentSummary{
		ProjectId: "my-project", EnvId: "my-env",
	}, nil, model.RawTofu(`some-tofu`), rawGraph, nil)

	s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().GenerateInternalModuleCatalogueWithResponse(
		gomock.Any(), "my-org", "my-project", "my-env", nil, platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{AreRulesIgnored: true, PinnedModuleVersions: []string{"thing1@v2"}},
		gomock.Any(),
	).Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200: &platformorchestratorcp.InternalModuleCatalogue{
			Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
				{
					ResourceType: "thing",
					Id:           "thing1", VersionId: "v2",
					ModuleSource:     "inline",
					ModuleSourceCode: ref.Ref("some-module-source"),
				},
			},
		},
	}, nil)

	r, err := s.GetDeploymentBundle(t.Context(), GetDeploymentBundleRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
		Params: GetDeploymentBundleParams{
			XDeploymentToken: util.GenerateHashedRunnerToken(s.RunnerTokenSalt, "my-org", depId.String()),
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentBundle200ApplicationxGzipResponse{}, r, "%+v", r)

	gr, err := gzip.NewReader(r.(GetDeploymentBundle200ApplicationxGzipResponse).Body)
	require.NoError(t, err)
	tr := tar.NewReader(gr)

	var fileTree []string
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of tar archive
		}
		require.NoError(t, err)
		fileTree = append(fileTree, header.Name)
	}
	assert.Equal(t, []string{
		"main.tf",
		"modules/thing1/v2/",
		"modules/thing1/v2/main.tf",
	}, fileTree)
}

func TestGetDeploymentLogs_deployment_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{orgCheck("my-org", PermissionDeploymentRead)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(0)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: uuid.New(),
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs404JSONResponse{}, r, "%+v", r)
}

func TestGetDeploymentLogs_logs_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	deploymentEnvUuid := uuid.New()

	deploymentId := uuid.New()
	s.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		assert.Equal(t, deploymentEnvUuid.String()+"/"+deploymentId.String(), filename)
		return nil, ErrRunnerLogsNotFound
	}

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).
		Return(&model.DeploymentSummary{Id: deploymentId, DeploymentEnvUuid: deploymentEnvUuid}, nil, nil, nil, nil)

	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs404JSONResponse{}, r, "%+v", r)
}

func TestGetDeploymentLogs_no_encryption_key(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	deploymentEnvUuid := uuid.New()

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	deploymentId := uuid.New()
	s.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		assert.Equal(t, deploymentEnvUuid.String()+"/"+deploymentId.String(), filename)
		var encryptedData = &bytes.Buffer{}
		w, err := age.Encrypt(encryptedData, id.Recipient())
		require.NoError(t, err)
		_, err = w.Write([]byte(sampleLogs))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		return io.NopCloser(strings.NewReader(base64.StdEncoding.EncodeToString(encryptedData.Bytes()))), nil
	}

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(&model.DeploymentSummary{Id: deploymentId, DeploymentEnvUuid: deploymentEnvUuid}, nil, nil, nil, nil)

	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs200TextResponse{}, r, "%+v", r)
	require.NotEqual(t, sampleLogs, r.(GetDeploymentLogs200TextResponse).Body)
}

func TestGetDeploymentLogs_improper_key(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	deploymentEnvUuid := uuid.New()

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	deploymentId := uuid.New()
	s.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		assert.Equal(t, deploymentEnvUuid.String()+"/"+deploymentId.String(), filename)
		sampleLogs := sampleLogs
		var encryptedData = &bytes.Buffer{}
		w, err := age.Encrypt(encryptedData, id.Recipient())
		require.NoError(t, err)
		_, err = w.Write([]byte(sampleLogs))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		return io.NopCloser(strings.NewReader(base64.StdEncoding.EncodeToString(encryptedData.Bytes()))), nil
	}

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(&model.DeploymentSummary{Id: deploymentId, DeploymentEnvUuid: deploymentEnvUuid}, nil, nil, nil, nil)

	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
		Params: GetDeploymentLogsParams{
			DecryptKey: ref.Ref("ageDumbKey"),
		},
	})

	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs400JSONResponse{}, r, "%+v", r)
	require.Equal(t, "the supplied key is not a valid 'age' private key", r.(GetDeploymentLogs400JSONResponse).Message)
}

func TestGetDeploymentLogs_private_key_not_matching(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	deploymentEnvUuid := uuid.New()

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	deploymentId := uuid.New()
	s.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		assert.Equal(t, deploymentEnvUuid.String()+"/"+deploymentId.String(), filename)
		sampleLogs := sampleLogs
		var encryptedData = &bytes.Buffer{}
		w, err := age.Encrypt(encryptedData, id.Recipient())
		require.NoError(t, err)
		_, err = w.Write([]byte(sampleLogs))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		return io.NopCloser(strings.NewReader(base64.StdEncoding.EncodeToString(encryptedData.Bytes()))), nil
	}

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(&model.DeploymentSummary{Id: deploymentId, DeploymentEnvUuid: deploymentEnvUuid}, nil, nil, nil, nil)
	anotherId, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
		Params: GetDeploymentLogsParams{
			DecryptKey: ref.Ref(anotherId.String()),
		},
	})

	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs400JSONResponse{}, r, "%+v", r)
	require.Equal(t, "logs cannot be decrypted with the selected identity", r.(GetDeploymentLogs400JSONResponse).Message)
}

func TestGetDeploymentLogs_valid_key(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	deploymentEnvUuid := uuid.New()

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	deploymentId := uuid.New()
	s.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		assert.Equal(t, deploymentEnvUuid.String()+"/"+deploymentId.String(), filename)
		sampleLogs := sampleLogs
		var encryptedData = &bytes.Buffer{}
		w, err := age.Encrypt(encryptedData, id.Recipient())
		require.NoError(t, err)
		_, err = w.Write([]byte(sampleLogs))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		return io.NopCloser(strings.NewReader(base64.StdEncoding.EncodeToString(encryptedData.Bytes()))), nil
	}

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(&model.DeploymentSummary{Id: deploymentId, DeploymentEnvUuid: deploymentEnvUuid}, nil, nil, nil, nil)

	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
		Params: GetDeploymentLogsParams{
			DecryptKey: ref.Ref(id.String()),
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs200TextResponse{}, r, "%+v", r)
	require.Equal(t, sampleLogs, r.(GetDeploymentLogs200TextResponse).Body)
}

func TestGetDeploymentOutputs(t *testing.T) {
	deploymentId := uuid.New()

	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		model    model.DeploymentSummary
		expected GetDeploymentEncryptedOutputsResponseObject
	}{
		{name: "success plan", model: model.DeploymentSummary{Mode: "plan_only", CompletedAt: opt.Of(time.Now()), Status: "succeeded", EncryptedOutputsRecipient: opt.Of(id.Recipient().String())}, expected: GetDeploymentEncryptedOutputs200JSONResponse{Raw: "encrypted-outputs"}},
		{name: "success deploy", model: model.DeploymentSummary{Mode: "deploy", CompletedAt: opt.Of(time.Now()), Status: "succeeded", EncryptedOutputsRecipient: opt.Of(id.Recipient().String())}, expected: GetDeploymentEncryptedOutputs200JSONResponse{Raw: "encrypted-outputs"}},
		{name: "fail destroy", model: model.DeploymentSummary{Mode: "destroy", CompletedAt: opt.Of(time.Now()), Status: "succeeded", EncryptedOutputsRecipient: opt.Of(id.Recipient().String())}, expected: GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404Response("outputs are not captured for deployments with mode 'destroy'")}},
		{name: "fail not completed", model: model.DeploymentSummary{Mode: "deploy", Status: "executing", EncryptedOutputsRecipient: opt.Of(id.Recipient().String())}, expected: GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404Response("deployment '00000000-0000-0000-0000-000000000000' has not completed yet")}},
		{name: "fail not succeeded", model: model.DeploymentSummary{Mode: "deploy", CompletedAt: opt.Of(time.Now()), Status: "failed", EncryptedOutputsRecipient: opt.Of(id.Recipient().String())}, expected: GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404Response("outputs are not captured for deployments with status 'failed'")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, s, fin := MockServer(t)
			defer fin()

			ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

			mdb := s.Database.(*mockmodel.MockDatabaser)
			mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(&tc.model, nil, nil, nil, nil)
			if _, success := tc.expected.(GetDeploymentEncryptedOutputs200JSONResponse); success {
				mdb.EXPECT().GetDeploymentEncryptedOutputs(gomock.Any(), gomock.Any(), "my-org", deploymentId).Return("encrypted-outputs", nil).Times(1)
			}

			r, err := s.GetDeploymentEncryptedOutputs(ctx, GetDeploymentEncryptedOutputsRequestObject{
				OrgId:        "my-org",
				DeploymentId: deploymentId,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.expected, r)
		})
	}
}

func TestCalculateDeploymentDiff_specific(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	fromDepId := uuid.New()
	toDepEnvUuid := uuid.New()
	toDepId := uuid.New()

	mdb := s.Database.(*mockmodel.MockDatabaser)
	encodedFromManifest, _ := json.Marshal(&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
		{Type: "a", Class: "default", Id: "x"}: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"}},
		{Type: "a", Class: "default", Id: "y"}: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"}},
	}})
	encodedToManifest, _ := json.Marshal(&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
		{Type: "a", Class: "default", Id: "y"}: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "2"}},
		{Type: "a", Class: "default", Id: "z"}: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"}},
	}})

	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Nil(), "my-org", toDepId, model.GetModeDefault).Return(&model.DeploymentSummary{DeploymentEnvUuid: toDepEnvUuid, Id: toDepId}, nil, nil, encodedToManifest, nil)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Nil(), "my-org", fromDepId, model.GetModeDefault).Return(&model.DeploymentSummary{DeploymentEnvUuid: toDepEnvUuid, Id: fromDepId}, nil, nil, encodedFromManifest, nil)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.CalculateDeploymentDiff(ctx, CalculateDeploymentDiffRequestObject{OrgId: "my-org", DeploymentId: toDepId, Params: CalculateDeploymentDiffParams{FromDeploymentId: &fromDepId}})
	require.NoError(t, err)
	require.Equal(t, CalculateDeploymentDiff200JSONResponse{
		FromDeploymentId: &fromDepId,
		ToDeploymentId:   &toDepId,
		NumRemoved:       1,
		NumChanged:       1,
		NumAdded:         1,
		Changes: []DeploymentDiffChange{
			{Id: util.GenerateNodeHash(toDepEnvUuid, "a", "default", "y"), Resource: "a.default@y", Type: DeploymentDiffChangeTypeModuleChanged, Summary: "module changed from d@1 to d@2"},
			{Id: util.GenerateNodeHash(toDepEnvUuid, "a", "default", "z"), Resource: "a.default@z", Type: DeploymentDiffChangeTypeAdded, Summary: "add resource using module d@1"},
			{Id: util.GenerateNodeHash(toDepEnvUuid, "a", "default", "x"), Resource: "a.default@x", Type: DeploymentDiffChangeTypeRemoved, Summary: "remove resource using module d@1"},
		},
	}, r)
}

func TestEnrichDeploymentError_ModuleEnrichment(t *testing.T) {
	tests := []struct {
		name              string
		entityId          string
		entityVersion     string
		graphNodes        map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]
		expectedModuleId  string
		expectedModuleVer string
		shouldEnrich      bool
	}{
		{
			name:              "enriches error when entity_id and entity_version are both provided",
			entityId:          "postgres_default_mydb_direct",
			entityVersion:     "v2.5.0",
			graphNodes:        map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{},
			expectedModuleId:  "postgres_default_mydb_direct",
			expectedModuleVer: "v2.5.0",
			shouldEnrich:      true,
		},
		{
			name:              "enriches error with entity_version directly without graph lookup",
			entityId:          "module-abc-123",
			entityVersion:     "v1.0.0",
			graphNodes:        map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{},
			expectedModuleId:  "module-abc-123",
			expectedModuleVer: "v1.0.0",
			shouldEnrich:      true,
		},
		{
			name:     "enriches error with matching module node",
			entityId: "postgres_default_mydb_123",
			graphNodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				{Type: "postgres", Class: "default", Id: "mydb"}: {
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{
						DefinitionId: "postgres-module-def",
						VersionId:    "v1.2.3",
					},
				},
			},
			expectedModuleId:  "postgres-module-def",
			expectedModuleVer: "v1.2.3",
			shouldEnrich:      true,
		},
		{
			name:     "enriches error with special characters stripped",
			entityId: "postgres_default_my-db_456",
			graphNodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				{Type: "postgres", Class: "default", Id: "my-db"}: {
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{
						DefinitionId: "postgres-v2",
						VersionId:    "v2.0.0",
					},
				},
			},
			expectedModuleId:  "postgres-v2",
			expectedModuleVer: "v2.0.0",
			shouldEnrich:      true,
		},
		{
			name:          "falls back to graph lookup when entity_version is empty",
			entityId:      "postgres_default_mydb_123",
			entityVersion: "", // Empty version should trigger graph lookup
			graphNodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				{Type: "postgres", Class: "default", Id: "mydb"}: {
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{
						DefinitionId: "postgres-module-def",
						VersionId:    "v1.2.3",
					},
				},
			},
			expectedModuleId:  "postgres-module-def",
			expectedModuleVer: "v1.2.3",
			shouldEnrich:      true,
		},
		{
			name:     "does not enrich when node not found",
			entityId: "postgres_default_nonexistent_789",
			graphNodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				{Type: "postgres", Class: "default", Id: "mydb"}: {
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{
						DefinitionId: "postgres-module-def",
						VersionId:    "v1.2.3",
					},
				},
			},
			shouldEnrich: false,
		},
		{
			name:     "does not enrich when node has no module configuration",
			entityId: "workload_default_app_123",
			graphNodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				{Type: "workload", Class: "default", Id: "app"}: {
					ModuleConfiguration: nil,
				},
			},
			shouldEnrich: false,
		},
		{
			name:         "does not enrich when entity_id has no underscore",
			entityId:     "nounderscore",
			graphNodes:   map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{},
			shouldEnrich: false,
		},
		{
			name:         "returns original JSON when entity_id is empty",
			entityId:     "",
			graphNodes:   map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{},
			shouldEnrich: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mdb := mockmodel.NewMockDatabaser(ctrl)
			ctx := context.Background()
			deploymentId := uuid.New()

			// Create graph and encode it
			graph := &platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
				Nodes: tc.graphNodes,
			}
			encodedGraph, err := graphs.ToJson(graph)
			require.NoError(t, err)

			// Mock GetDeployment - only called if entityId is not empty
			if tc.entityId != "" {
				mdb.EXPECT().GetDeployment(gomock.Any(), nil, orgId, deploymentId, model.GetModeDefault).
					Return(nil, nil, nil, encodedGraph, nil).Times(1)
			}

			// Create error message
			errorMsg := map[string]interface{}{
				"entity_id":   tc.entityId,
				"entity_type": "module",
				"summary":     "Module error occurred",
			}
			if tc.entityVersion != "" {
				errorMsg["entity_version"] = tc.entityVersion
			}
			errorMsgJson, err := json.Marshal(errorMsg)
			require.NoError(t, err)

			// Call enrichDeploymentError
			result, err := enrichDeploymentError(ctx, mdb, orgId, deploymentId, "TF_DIAGNOSTIC_ERROR", string(errorMsgJson))
			require.NoError(t, err)

			// Verify the result
			if tc.shouldEnrich {
				var enrichedError map[string]interface{}
				err = json.Unmarshal([]byte(result), &enrichedError)
				require.NoError(t, err)

				assert.Equal(t, tc.expectedModuleId, enrichedError["module_id"])
				assert.Equal(t, tc.expectedModuleVer, enrichedError["module_version"])
				assert.Equal(t, tc.entityId, enrichedError["entity_id"])
				assert.Equal(t, "module", enrichedError["entity_type"])
			} else {
				// Should return JSON without enrichment fields
				var enrichedError map[string]interface{}
				err = json.Unmarshal([]byte(result), &enrichedError)
				require.NoError(t, err)
				assert.Equal(t, tc.entityId, enrichedError["entity_id"])
				assert.Equal(t, "module", enrichedError["entity_type"])
				assert.Nil(t, enrichedError["module_id"])
				assert.Nil(t, enrichedError["module_version"])
			}
		})
	}
}

func TestEnrichDeploymentError_ProviderEnrichment(t *testing.T) {
	tests := []struct {
		name                 string
		providerType         string
		providerId           string
		variantHash          string
		expectedProviderType string
		expectedProviderId   string
		shouldEnrich         bool
	}{
		{
			name:                 "enriches provider error with valid entity_id",
			providerType:         "aws",
			providerId:           "default",
			variantHash:          "abc12345",
			expectedProviderType: "aws",
			expectedProviderId:   "default",
			shouldEnrich:         true,
		},
		{
			name:                 "enriches provider error with underscores in type",
			providerType:         "azure_rm",
			providerId:           "prod",
			variantHash:          "def67890",
			expectedProviderType: "azure_rm",
			expectedProviderId:   "prod",
			shouldEnrich:         true,
		},
		{
			name:                 "enriches provider error with complex provider id",
			providerType:         "google",
			providerId:           "prod-us-east",
			variantHash:          "xyz98765",
			expectedProviderType: "google",
			expectedProviderId:   "prod-us-east",
			shouldEnrich:         true,
		},
		{
			name:         "does not enrich when provider not found in graph",
			providerType: "aws",
			providerId:   "missing",
			variantHash:  "notfound",
			shouldEnrich: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mdb := mockmodel.NewMockDatabaser(ctrl)
			ctx := context.Background()
			deploymentId := uuid.New()

			// Create graph with provider mapping
			localProviderName := graphs.LocalProviderName(tc.providerType, tc.providerId, tc.variantHash)

			// The full provider identifier (e.g., "aws.default")
			fullProviderIdentifier := fmt.Sprintf("%s.%s", tc.providerType, tc.providerId)

			var providerMapping map[string]string

			if tc.shouldEnrich {
				// Map from full provider identifier to hash variant
				providerMapping = map[string]string{
					fullProviderIdentifier: tc.variantHash,
				}
			}

			graph := &platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
				Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
					{Type: "test", Class: "default", Id: "test"}: {
						ModuleConfiguration: &graphs.GraphNodeModuleConfig{
							DefinitionId:                       "test-def",
							VersionId:                          "test-ver",
							ProviderFullIdToHashVariantMapping: providerMapping,
						},
					},
				},
			}
			encodedGraph, err := graphs.ToJson(graph)
			require.NoError(t, err)

			// Mock GetDeployment to return the graph
			mdb.EXPECT().GetDeployment(gomock.Any(), nil, orgId, deploymentId, model.GetModeDefault).
				Return(nil, nil, nil, model.EncodedDeploymentGraph(encodedGraph), nil)

			// Create error message with the hashed local provider name
			errorMsg := map[string]interface{}{
				"entity_id":   localProviderName,
				"entity_type": "provider",
				"summary":     "Provider error occurred",
			}
			errorMsgJson, err := json.Marshal(errorMsg)
			require.NoError(t, err)

			// Call enrichDeploymentError
			result, err := enrichDeploymentError(ctx, mdb, orgId, deploymentId, "TF_DIAGNOSTIC_ERROR", string(errorMsgJson))
			require.NoError(t, err)

			// Verify the result
			if tc.shouldEnrich {
				var enrichedError map[string]interface{}
				err = json.Unmarshal([]byte(result), &enrichedError)
				require.NoError(t, err)

				assert.Equal(t, tc.expectedProviderType, enrichedError["provider_type"])
				assert.Equal(t, tc.expectedProviderId, enrichedError["provider_id"])
				assert.Equal(t, localProviderName, enrichedError["entity_id"])
				assert.Equal(t, "provider", enrichedError["entity_type"])
			} else {
				// Should return JSON without enrichment fields
				var enrichedError map[string]interface{}
				err = json.Unmarshal([]byte(result), &enrichedError)
				require.NoError(t, err)
				assert.Equal(t, localProviderName, enrichedError["entity_id"])
				assert.Equal(t, "provider", enrichedError["entity_type"])
				assert.Nil(t, enrichedError["provider_type"])
				assert.Nil(t, enrichedError["provider_id"])
			}
		})
	}
}

func TestEnrichDeploymentError_UnknownEntityType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mdb := mockmodel.NewMockDatabaser(ctrl)
	deploymentId := uuid.New()

	errorMsg := map[string]interface{}{
		"entity_id":   "some-entity",
		"entity_type": "unknown_type",
		"summary":     "Unknown error",
	}
	errorMsgJson, err := json.Marshal(errorMsg)
	require.NoError(t, err)

	result, err := enrichDeploymentError(t.Context(), mdb, orgId, deploymentId, "TF_DIAGNOSTIC_ERROR", string(errorMsgJson))
	require.NoError(t, err)

	// Should return JSON without enrichment fields
	var enrichedError map[string]interface{}
	err = json.Unmarshal([]byte(result), &enrichedError)
	require.NoError(t, err)
	assert.Equal(t, "some-entity", enrichedError["entity_id"])
	assert.Equal(t, "unknown_type", enrichedError["entity_type"])
	assert.Nil(t, enrichedError["module_id"])
	assert.Nil(t, enrichedError["module_version"])
	assert.Nil(t, enrichedError["provider_type"])
	assert.Nil(t, enrichedError["provider_id"])
}

func TestListDeployments_org_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{orgCheck("my-org", PermissionDeploymentRead)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), "my-org").Return(&platformorchestratorcp.GetInternalOrganizationResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNotFound},
	}, nil)

	r, err := s.ListDeployments(ctx, ListDeploymentsRequestObject{
		OrgId: "my-org",
	})
	require.NoError(t, err)
	require.IsTypef(t, ListDeployments404JSONResponse{}, r, "unexpected %v", r)
	r404 := r.(ListDeployments404JSONResponse)
	if assert.NotNil(t, r404) {
		assert.Equal(t, "organization my-org not found", r404.Message)
	}
}

func TestListLastDeployments_org_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{orgCheck("my-org", PermissionDeploymentRead)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), "my-org").Return(&platformorchestratorcp.GetInternalOrganizationResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNotFound},
	}, nil)

	r, err := s.ListLastDeployments(ctx, ListLastDeploymentsRequestObject{
		OrgId: "my-org",
	})
	require.NoError(t, err)
	require.IsTypef(t, ListLastDeployments404JSONResponse{}, r, "unexpected %v", r)
	r404 := r.(ListLastDeployments404JSONResponse)
	if assert.NotNil(t, r404) {
		assert.Equal(t, "organization my-org not found", r404.Message)
	}
}
