package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/plugincontext"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
)

const (
	ObservationRequestResolvePath    = "/orgs/:orgId/plugins/:pluginId/observations/v1/requests/:requestId/resolve"
	ObservationRedeployResolvePath   = "/orgs/:orgId/plugins/:pluginId/observations/v1/redeployments/:requestId/resolve"
	ObservationRequestResolveAction  = "observation.request.resolve"
	ObservationRedeployResolveAction = "observation.redeploy.resolve"
)

// These separate service-signed safety actions do not authorize execution. CP
// closes its original routing intent before asking DP to arbitrate admission.
func decodeObservationResolution[T any](a *ExternalDeploymentHTTPAdapter, c echo.Context, action string) (plugincontext.Claims, T, error) {
	var body T
	kind, _, err := mime.ParseMediaType(c.Request().Header.Get("Content-Type"))
	if err != nil || kind != observationJSONContentType {
		return plugincontext.Claims{}, body, echo.NewHTTPError(http.StatusUnsupportedMediaType, "Content-Type must be application/json")
	}
	raw, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxObservationBodyBytes))
	if err != nil {
		return plugincontext.Claims{}, body, echo.NewHTTPError(http.StatusBadRequest, "resolution request exceeds bound")
	}
	key, ok := singleHeader(c.Request().Header, "Idempotency-Key")
	requestID, parseErr := uuid.Parse(c.Param("requestId"))
	if !ok || parseErr != nil || requestID == uuid.Nil || key != requestID.String() {
		return plugincontext.Claims{}, body, echo.NewHTTPError(http.StatusBadRequest, "exact resolution request identity required")
	}
	claims, err := a.authenticateObservation(c, action, raw, key)
	if err != nil {
		return claims, body, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&body); err != nil || requireJSONEOF(decoder) != nil {
		return claims, body, echo.NewHTTPError(http.StatusBadRequest, "strict original resolution request required")
	}
	return claims, body, nil
}

func (a *ExternalDeploymentHTTPAdapter) HandleObservationRequestResolve(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), RequestHandlerTimeout)
	defer cancel()
	c.SetRequest(c.Request().WithContext(ctx))
	claims, body, err := decodeObservationResolution[observation.ObservationRequest](a, c, ObservationRequestResolveAction)
	if err != nil {
		return err
	}
	if body.GrantId == uuid.Nil || body.GrantRevision <= 0 || body.ProjectUuid == uuid.Nil || body.EnvironmentUuid == uuid.Nil || body.RequestId == uuid.Nil || body.RequestId.String() != c.Param("requestId") || body.ProjectId == "" || body.EnvId == "" || len(body.ProjectId) > 63 ||
		len(body.EnvId) > 63 ||
		body.Occurrence.IsZero() ||
		body.AdmissionDeadline.IsZero() ||
		body.AdmissionDeadline.Sub(body.Occurrence) <= 0 ||
		body.AdmissionDeadline.Sub(body.Occurrence) > 10*time.Minute {
		return echo.NewHTTPError(http.StatusBadRequest, "exact original bounded observation identity required")
	}
	actor, _ := uuid.Parse(claims.ActorID)
	result, err := model.ResolveObservationRequest(ctx, a.Admission.Database, claims.OrgID, claims.PluginID, actor, body)
	if err != nil {
		return writeObservationError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}

func (a *ExternalDeploymentHTTPAdapter) HandleObservationRedeployResolve(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), RequestHandlerTimeout)
	defer cancel()
	c.SetRequest(c.Request().WithContext(ctx))
	claims, envelope, err := decodeObservationResolution[observation.ObservationRedeployResolveRequest](a, c, ObservationRedeployResolveAction)
	if err != nil {
		return err
	}
	body := envelope.Request
	if envelope.RunId == uuid.Nil || body.RequestId == uuid.Nil || body.RequestId.String() != c.Param("requestId") || body.ProjectUuid == uuid.Nil || body.EnvironmentUuid == uuid.Nil || body.BaselineId == uuid.Nil || !externalPlanFingerprintPattern.MatchString(body.BaselineFingerprint) ||
		body.ProjectId == "" ||
		body.EnvId == "" ||
		len(body.ProjectId) > 63 ||
		len(body.EnvId) > 63 {
		return echo.NewHTTPError(http.StatusBadRequest, "exact original baseline identity required")
	}
	actor, _ := uuid.Parse(claims.ActorID)
	result, err := model.ResolveObservationRedeploy(ctx, a.Admission.Database, claims.OrgID, claims.PluginID, actor, envelope.RunId, body)
	if err != nil {
		return writeObservationError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}
