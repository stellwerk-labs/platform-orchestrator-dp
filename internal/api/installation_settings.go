package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	iamapi "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
)

const systemMaintenanceCode = "system_maintenance"

const maximumSettingsBodyBytes = 32 * 1024

const settingsReadPermission = "platform_settings_read"
const settingsWritePermission = "platform_settings_write"

func (s *Server) installationSettingsCaller(c echo.Context, permission string) (uuid.UUID, error) {
	actor, err := uuid.Parse(c.Request().Header.Get("From"))
	if err != nil || actor == uuid.Nil {
		return uuid.Nil, echo.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}
	if !userid.IsHumanUser(actor) || actor == userid.InternalSystemUuid {
		return uuid.Nil, echo.NewHTTPError(http.StatusForbidden, "A platform settings operator is required")
	}
	if s.IamClient == nil {
		return uuid.Nil, echo.NewHTTPError(http.StatusServiceUnavailable, "Platform authorization unavailable")
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), RequestHandlerTimeout)
	defer cancel()
	response, err := s.IamClient.InternalAuthorizeWithResponse(ctx, iamapi.InternalAuthorizeBody{UserId: actor, Checks: []iamapi.ResourcePermissionCheck{{Resource: "platform", Permission: permission}}})
	if err != nil {
		return uuid.Nil, echo.NewHTTPError(http.StatusServiceUnavailable, "Platform authorization unavailable").SetInternal(err)
	}
	if response.StatusCode() == http.StatusForbidden {
		return uuid.Nil, echo.NewHTTPError(http.StatusForbidden, "The platform permission "+permission+" is required")
	}
	if response.StatusCode() != http.StatusNoContent {
		return uuid.Nil, echo.NewHTTPError(http.StatusServiceUnavailable, "Platform authorization unavailable")
	}
	return actor, nil
}

func (s *Server) getInstallationMaintenance(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "no-store")
	if _, err := s.installationSettingsCaller(c, settingsReadPermission); err != nil {
		return err
	}
	value, err := model.ReadInstallationMaintenance(c.Request().Context(), s.Database)
	if err != nil {
		return err
	}
	c.Response().Header().Set("ETag", `"`+strconv.FormatInt(value.Revision, 10)+`"`)
	return c.JSON(http.StatusOK, value)
}

func (s *Server) putInstallationMaintenance(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "no-store")
	actor, err := s.installationSettingsCaller(c, settingsWritePermission)
	if err != nil {
		return err
	}
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	revision, err := decodeSettingsMutation(c, s.UiHostUrl, &body)
	if err != nil {
		return err
	}

	if body.Enabled == nil {
		return echo.NewHTTPError(http.StatusBadRequest, "enabled must be a boolean")
	}
	value, err := model.UpdateInstallationMaintenance(c.Request().Context(), s.Database, actor, revision, *body.Enabled)
	if errors.Is(err, model.ErrSettingsRevision) {
		return echo.NewHTTPError(http.StatusPreconditionFailed, err.Error())
	}
	if err != nil {
		return err
	}
	c.Response().Header().Set("ETag", `"`+strconv.FormatInt(value.Revision, 10)+`"`)
	return c.JSON(http.StatusOK, value)
}

func parseSettingsRevision(value string) (int64, error) {
	if value == "" {
		return 0, echo.NewHTTPError(http.StatusPreconditionRequired, "If-Match is required; read settings before changing them")
	}
	if len(value) < 3 || value[0] != '"' || value[len(value)-1] != '"' {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "If-Match must contain the quoted settings revision")
	}
	revision, err := strconv.ParseInt(value[1:len(value)-1], 10, 64)
	if err != nil || revision < 1 {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "Invalid settings revision")
	}
	return revision, nil
}

func decodeSettingsMutation(c echo.Context, uiHost string, value any) (int64, error) {
	if _, cookie := c.Cookie("session-token"); cookie == nil {
		console, err := url.Parse(uiHost)
		if err != nil || console.Host == "" || c.Request().Header.Get("Origin") != console.Scheme+"://"+console.Host {
			return 0, echo.NewHTTPError(http.StatusForbidden, "The exact Console origin is required")
		}
	}
	revision, err := parseSettingsRevision(c.Request().Header.Get("If-Match"))
	if err != nil {
		return 0, err
	}
	media, _, err := mime.ParseMediaType(c.Request().Header.Get("Content-Type"))
	if err != nil || media != observationJSONContentType {
		return 0, echo.NewHTTPError(http.StatusUnsupportedMediaType, "application/json is required")
	}
	decoder := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, maximumSettingsBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "A valid settings object is required")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "One JSON object is required")
	}
	return revision, nil
}

// Internal reads use the same trusted service-network identity boundary as other
// internal DP APIs. The edge Gateway does not publish /internal routes.
func (s *Server) getInternalInstallationMaintenance(c echo.Context) error {
	if c.Request().Header.Get("From") != userid.InternalSystemUuid.String() {
		return echo.NewHTTPError(http.StatusForbidden, "Core identity required")
	}
	value, err := model.ReadInstallationMaintenance(c.Request().Context(), s.Database)
	if err != nil {
		return err
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, value)
}
