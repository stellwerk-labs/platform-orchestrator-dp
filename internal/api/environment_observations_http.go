package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/plugincontext"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
)

const (
	observationClockSkew        = 30 * time.Second
	observationJSONContentType  = "application/json"
	maxObservationBodyBytes     = 16 << 10
	ObservationStartPath        = "/orgs/:orgId/plugins/:pluginId/observations/v1/runs"
	ObservationRunPath          = "/orgs/:orgId/plugins/:pluginId/observations/v1/runs/:runId"
	ObservationRequestPath      = "/orgs/:orgId/plugins/:pluginId/observations/v1/requests/:requestId"
	ObservationStartAction      = "observation.try-start"
	ObservationGetAction        = "observation.get"
	ObservationRequestGetAction = "observation.request.get"
)

// Observation transport is optional and uses exactly the same authenticated
// request-bound CP-to-DP boundary as external deployments. CP retains its grant
// fencing transaction and revalidates the authorizing Admin before signing.
func (a *ExternalDeploymentHTTPAdapter) authenticateObservation(c echo.Context, action string, body []byte, idempotency string) (plugincontext.Claims, error) {
	if a == nil || a.Issuer == "" || len(a.VerificationKey) != ed25519.PublicKeySize || a.Admission.Database == nil || a.ControlPlaneClient == nil || a.EventPublisher == nil {
		return plugincontext.Claims{}, echo.NewHTTPError(http.StatusServiceUnavailable, "observation transport unavailable")
	}
	request := c.Request()
	orgID, pluginID := c.Param("orgId"), c.Param("pluginId")
	if !validExternalPathIdentity(orgID, pluginID) || request.URL.RawQuery != "" || request.URL.Fragment != "" {
		return plugincontext.Claims{}, echo.NewHTTPError(http.StatusBadRequest, "invalid observation path")
	}
	assertion, ok := singleHeader(request.Header, plugincontext.Header)
	if !ok || assertion == "" {
		return plugincontext.Claims{}, echo.NewHTTPError(http.StatusUnauthorized, "signed plugin context required")
	}
	claims, err := plugincontext.Verify(
		a.VerificationKey,
		assertion,
		plugincontext.Request{Issuer: a.Issuer, Audience: ExternalDeploymentAudience, PluginID: pluginID, OrgID: orgID, Method: request.Method, Path: request.URL.EscapedPath(), Body: body, OperationID: action, IdempotencyKey: idempotency},
		a.now(),
	)
	if err != nil {
		return plugincontext.Claims{}, echo.NewHTTPError(http.StatusUnauthorized, "invalid signed plugin context")
	}
	if actor, err := uuid.Parse(claims.ActorID); err != nil || actor == uuid.Nil {
		return plugincontext.Claims{}, echo.NewHTTPError(http.StatusUnauthorized, "invalid signed actor")
	}
	return claims, nil
}

func (a *ExternalDeploymentHTTPAdapter) HandleObservationStart(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), RequestHandlerTimeout)
	defer cancel()
	c.SetRequest(c.Request().WithContext(ctx))
	contentType, _, err := mime.ParseMediaType(c.Request().Header.Get("Content-Type"))
	if err != nil || contentType != observationJSONContentType {
		return echo.NewHTTPError(http.StatusUnsupportedMediaType, "Content-Type must be application/json")
	}
	raw, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxObservationBodyBytes))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "observation request too large")
	}
	key, ok := singleHeader(c.Request().Header, "Idempotency-Key")
	if !ok {
		return echo.NewHTTPError(http.StatusBadRequest, "Idempotency-Key required")
	}
	claims, err := a.authenticateObservation(c, ObservationStartAction, raw, key)
	if err != nil {
		return err
	}
	var body observation.ObservationRequest
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || requireJSONEOF(decoder) != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "strict observation request required")
	}
	actor, _ := uuid.Parse(claims.ActorID)
	if body.GrantId == uuid.Nil || body.GrantRevision <= 0 || body.ProjectUuid == uuid.Nil || body.EnvironmentUuid == uuid.Nil || body.RequestId == uuid.Nil || key != body.RequestId.String() || body.ProjectId == "" || body.EnvId == "" || len(body.ProjectId) > 63 || len(body.EnvId) > 63 ||
		body.Occurrence.IsZero() ||
		body.AdmissionDeadline.IsZero() ||
		body.AdmissionDeadline.Sub(body.Occurrence) <= 0 ||
		body.AdmissionDeadline.Sub(body.Occurrence) > 10*time.Minute ||
		body.Occurrence.After(a.now().Add(observationClockSkew)) {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid bounded observation identity or admission window")
	}
	readTx, err := a.Admission.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	replay, replayErr := model.ReplayObservationRequest(ctx, readTx, claims.OrgID, claims.PluginID, actor, body)
	_ = readTx.Rollback()
	if replayErr == nil {
		return c.JSON(http.StatusOK, replay)
	}
	if _, missing := model.IsErrNotFound(replayErr); !missing {
		return writeObservationError(c, replayErr)
	}
	if !a.now().Before(body.AdmissionDeadline) {
		receipt, _, err := model.TryStartObservation(ctx, a.Admission.Database, claims.OrgID, claims.PluginID, actor, body, "", 0, false)
		if err != nil {
			return writeObservationError(c, err)
		}
		return c.JSON(http.StatusOK, receipt)
	}
	// Resolve canonical target and current Runner, never accept plugin-provided
	// execution material, Runner identity, command, outputs, or source overrides.
	env, err := a.ControlPlaneClient.GetEnvironmentWithResponse(ctx, claims.OrgID, body.ProjectId, body.EnvId)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "Core target lookup unavailable")
	}
	if env.StatusCode() != http.StatusOK || env.JSON200 == nil || env.JSON200.ProjectUuid == nil {
		return echo.NewHTTPError(http.StatusConflict, "Environment unavailable")
	}
	target := env.JSON200
	if target.Uuid != body.EnvironmentUuid || *target.ProjectUuid != body.ProjectUuid || target.ProjectId != body.ProjectId || target.Id != body.EnvId {
		return echo.NewHTTPError(http.StatusConflict, "Environment identity changed")
	}
	limit, supported, runnerID := 0, false, ""
	if target.RunnerId != nil {
		runnerID = *target.RunnerId
		runner, err := a.ControlPlaneClient.GetInternalRunnerWithResponse(ctx, claims.OrgID, runnerID)
		if err != nil {
			return echo.NewHTTPError(http.StatusServiceUnavailable, "Core Runner lookup unavailable")
		}
		if runner.StatusCode() == http.StatusOK && runner.JSON200 != nil {
			limit, supported, err = model.RunnerAdmissionConfiguration(runner.JSON200.RunnerConfiguration)
			if err != nil {
				return echo.NewHTTPError(http.StatusServiceUnavailable, "invalid Runner scheduling configuration")
			}
		}
	}
	receipt, pending, err := model.TryStartObservation(ctx, a.Admission.Database, claims.OrgID, claims.PluginID, actor, body, runnerID, limit, supported)
	if err != nil {
		return writeObservationError(c, err)
	}
	if len(pending) > 0 {
		reliableoutbox.OptimisticPublish(ctx, a.logger(), a.Admission.Database.AsReliableOutboxStore(), a.EventPublisher, pending)
	}
	return c.JSON(http.StatusOK, receipt)
}
func (a *ExternalDeploymentHTTPAdapter) HandleObservationRequest(c echo.Context) error {
	return a.handleObservationRead(c, false)
}
func (a *ExternalDeploymentHTTPAdapter) HandleObservationRun(c echo.Context) error {
	return a.handleObservationRead(c, true)
}
func (a *ExternalDeploymentHTTPAdapter) handleObservationRead(c echo.Context, run bool) error {
	action, param := ObservationRequestGetAction, "requestId"
	if run {
		action, param = ObservationGetAction, "runId"
	}
	claims, err := a.authenticateObservation(c, action, nil, "")
	if err != nil {
		return err
	}
	id, err := uuid.Parse(c.Param(param))
	if err != nil || id == uuid.Nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid observation identity")
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), RequestHandlerTimeout)
	defer cancel()
	tx, err := a.Admission.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var value any
	if run {
		value, err = model.ReadObservationRun(ctx, tx, claims.OrgID, claims.PluginID, id)
	} else {
		value, err = model.ReadObservationReceipt(ctx, tx, claims.OrgID, claims.PluginID, id)
	}
	if err != nil {
		return writeObservationError(c, err)
	}
	return c.JSON(http.StatusOK, value)
}
func writeObservationError(c echo.Context, err error) error {
	if errors.Is(err, model.ErrObservationRequestSealed) {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "observation_request_not_started", Message: "This exact request was sealed without starting"})
	}
	var denied *deploymentAdmissionDeniedError
	if errors.As(err, &denied) {
		return c.JSON(denied.statusCode, externalDeploymentErrorResponse{Error: "deployment_admission_denied", Message: "Core denied ordinary deployment admission"})
	}
	if errors.Is(err, ErrDeploymentAdmissionUnavailable) {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "ordinary deployment admission unavailable")
	}

	if _, ok := model.IsErrNotFound(err); ok {
		return echo.NewHTTPError(http.StatusNotFound, "observation not found")
	}
	if conflict, ok := model.IsErrConflict(err); ok {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "observation_conflict", Message: conflict.Message})
	}
	if bad, ok := model.IsErrBadRequest(err); ok {
		return echo.NewHTTPError(http.StatusBadRequest, bad.Message)
	}
	return echo.NewHTTPError(http.StatusInternalServerError, "observation storage unavailable").SetInternal(err)
}

const ObservationCapabilitiesPath = "/orgs/:orgId/plugins/:pluginId/observations/v1/capabilities"
const ObservationCapabilitiesAction = "observation.capabilities"

func (a *ExternalDeploymentHTTPAdapter) HandleObservationCapabilities(c echo.Context) error {
	if _, err := a.authenticateObservation(c, ObservationCapabilitiesAction, nil, ""); err != nil {
		return err
	}
	protocols := []string{}
	if a.OrdinaryAdmissions != nil && a.OrdinaryAdmissions.Core != nil && a.OrdinaryAdmissions.Database != nil && a.OrdinaryAdmissions.BearerToken != "" {
		protocols = append(protocols, "environment-observation/v1", "environment-observation-recovery/v1")
	}
	return c.JSON(http.StatusOK, observation.ObservationCapabilities{Protocols: protocols})
}
