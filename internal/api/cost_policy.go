package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/costestimation"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func (s *Server) respondCostPolicy(c echo.Context, value model.CostPolicy) error {
	c.Response().Header().Set("ETag", `"`+strconv.FormatInt(value.Revision, 10)+`"`)
	return c.JSON(http.StatusOK, value)
}

func (s *Server) getCostPolicy(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "no-store")
	if _, err := s.installationSettingsCaller(c, settingsReadPermission); err != nil {
		return err
	}
	value, err := model.ReadCostPolicy(c.Request().Context(), s.Database)
	if err != nil {
		return err
	}
	return s.respondCostPolicy(c, value)
}

func (s *Server) putCostPolicy(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "no-store")
	actor, err := s.installationSettingsCaller(c, settingsWritePermission)
	if err != nil {
		return err
	}
	var body struct {
		Enabled *bool           `json:"enabled"`
		APIKey  json.RawMessage `json:"api_key"`
	}
	revision, err := decodeSettingsMutation(c, s.UiHostUrl, &body)
	if err != nil {
		return err
	}
	if body.Enabled == nil {
		return echo.NewHTTPError(http.StatusBadRequest, "enabled must be a boolean")
	}
	var ciphertext *string
	if body.APIKey != nil {
		var key *string
		if json.Unmarshal(body.APIKey, &key) != nil || key == nil || !costestimation.ValidAPIKey(*key) {
			return echo.NewHTTPError(http.StatusBadRequest, "api_key must be a string of at most 4096 bytes without whitespace or controls")
		}
		encrypted, err := costestimation.Encrypt(c.Request().Context(), s.Vault, *key)
		if err != nil {
			return echo.NewHTTPError(http.StatusServiceUnavailable, costestimation.ErrCredentialUnavailable.Error())
		}
		ciphertext = &encrypted
	}
	value, err := model.UpdateCostPolicy(c.Request().Context(), s.Database, actor, revision, *body.Enabled, ciphertext)
	if errors.Is(err, model.ErrSettingsRevision) {
		return echo.NewHTTPError(http.StatusPreconditionFailed, err.Error())
	}
	if errors.Is(err, model.ErrCostAPIKeyRequired) {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err != nil {
		return err
	}
	return s.respondCostPolicy(c, value)
}
