package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/herrors"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestQueuePromotionFailuresAreRetriedOnlyWhenTheEnvironmentOrCoreCanResolveThem(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		verdict queuePromotionVerdict
		reason  string
	}{
		{"lease or queue", errors.Wrap(model.NewErrCodedConflict(model.CodeEnvironmentBusy, "busy"), "failed to create deployment"), queueRetryLater, ""},
		{"Core settling", model.NewErrCodedConflict(model.CodeEnvironmentBusy, "Core is still settling"), queueRetryLater, ""},
		{"Core unavailable", fmt.Errorf("%w: timeout", ErrDeploymentAdmissionUnavailable), queueRetryLater, ""},
		{"transport", errors.New("failed to get environment: connection refused"), queueRetryLater, ""},
		{"fenced", errors.Wrap(model.NewErrCodedConflict(model.CodeOutcomeUnreconciled, "unreconciled"), "failed to create deployment"), queueRejectAll, model.CodeOutcomeUnreconciled},
		{"Pin protection", &deploymentAdmissionDeniedError{statusCode: http.StatusConflict, code: "pin_protection"}, queueRejectItem, "pin_protection"},
		{"uncoded Core denial", &deploymentAdmissionDeniedError{statusCode: http.StatusForbidden}, queueRejectItem, deploymentQueueReasonAdmission},
		{"lost permission", &herrors.PlatformOrchestratorError{StatusCode: http.StatusForbidden}, queueRejectItem, deploymentQueueReasonForbidden},
		{"base changed", model.NewErrCodedConflict(model.CodeDeploymentBaseChanged, "moved"), queueRejectItem, model.CodeDeploymentBaseChanged},
		{"deleting", model.NewErrCodedConflict(model.CodeEnvironmentDeleting, "deleting"), queueRejectItem, model.CodeEnvironmentDeleting},
		{"uncoded conflict", model.NewErrConflict("cannot reference a deployment that is not part of this environment"), queueRejectItem, deploymentQueueReasonConflict},
		{"invalid", model.NewErrBadRequest("Defective Module Version requires explicit confirmation"), queueRejectItem, deploymentQueueReasonInvalid},
		{"pruned rollback target", errors.Wrap(model.NewErrNotFound("deployment not found"), "failed to find reference deployment"), queueRejectItem, deploymentQueueReasonNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			verdict, reason := classifyQueuePromotionError(test.err)
			require.Equal(t, test.verdict, verdict)
			require.Equal(t, test.reason, reason)
		})
	}
}

func TestQueueRetryBackoffDoublesUpToItsCap(t *testing.T) {
	require.Equal(t, 5*time.Second, deploymentQueueBackoff(0))
	require.Equal(t, 5*time.Second, deploymentQueueBackoff(1))
	require.Equal(t, 10*time.Second, deploymentQueueBackoff(2))
	require.Equal(t, 80*time.Second, deploymentQueueBackoff(5))
	require.Equal(t, 2*time.Minute, deploymentQueueBackoff(6))
	require.Equal(t, 2*time.Minute, deploymentQueueBackoff(1000))
}

func TestDeploymentQueueConfigFallsBackToTheRecommendedDefaults(t *testing.T) {
	require.Equal(t, DeploymentQueueConfig{MaxDepth: 10, DefaultTTL: 2 * time.Hour, MaxTTL: 24 * time.Hour}, (&Server{}).deploymentQueueConfig())
	configured := DeploymentQueueConfig{MaxDepth: 3, DefaultTTL: time.Minute, MaxTTL: time.Hour}
	require.Equal(t, configured, (&Server{DeploymentQueue: configured}).deploymentQueueConfig())
}

func TestQueuedEnvironmentsAreBusyForPlugins(t *testing.T) {
	succeeded := model.EnvironmentDeploymentReadiness{LatestStatus: opt.Of(model.DeploymentStatusSucceeded)}
	require.Equal(t, "ready", projectEnvironmentReadiness(succeeded).State)
	succeeded.Queued = true
	require.Equal(t, "busy", projectEnvironmentReadiness(succeeded).State)
	failed := model.EnvironmentDeploymentReadiness{LatestStatus: opt.Of(model.DeploymentStatusFailed), Queued: true}
	require.Equal(t, "busy", projectEnvironmentReadiness(failed).State, "the queue deploys next")
}

func TestConflictResponsesCarryTheirStableCode(t *testing.T) {
	require.Equal(t, "HTTP-409", Generate409FromModelErr(model.NewErrConflict("plain")).Error)
	coded := Generate409FromModelErr(model.NewErrCodedConflict(model.CodeDeploymentQueueFull, "full"))
	require.Equal(t, model.CodeDeploymentQueueFull, coded.Error)
	require.Equal(t, "full", coded.Message)

	response, err := createDeploymentErrorResponse(&deploymentAdmissionDeniedError{statusCode: http.StatusConflict, code: "pin_protection"})
	require.NoError(t, err)
	require.Equal(t, "pin_protection", response.(CreateDeployment409JSONResponse).Error)
	response, err = createDeploymentErrorResponse(&deploymentAdmissionDeniedError{statusCode: http.StatusConflict})
	require.NoError(t, err)
	require.Equal(t, "HTTP-409", response.(CreateDeployment409JSONResponse).Error)
}

func TestPluginDispatchLearnsWhyTheEnvironmentIsBusy(t *testing.T) {
	for _, test := range []struct {
		err  error
		code string
	}{
		{errors.Wrap(model.NewErrCodedConflict(model.CodeEnvironmentBusy, "Deployments are queued for the Environment"), "create"), model.CodeEnvironmentBusy},
		{model.NewErrCodedConflict(model.CodeOutcomeUnreconciled, "unreconciled"), model.CodeOutcomeUnreconciled},
		{model.NewErrConflict("external deployment request identity was reused with different inputs"), "deployment_conflict"},
	} {
		recorder := httptest.NewRecorder()
		c := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/", nil), recorder)
		require.NoError(t, (&ExternalDeploymentHTTPAdapter{}).writeAdmissionError(c, test.err))
		require.Equal(t, http.StatusConflict, recorder.Code)
		var body externalDeploymentErrorResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
		require.Equal(t, test.code, body.Error)
	}
}
