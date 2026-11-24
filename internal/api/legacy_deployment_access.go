package api

import (
	"crypto/subtle"
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

const LegacyDeploymentAccessPath = "/v1/orgs/:orgId/runners/:runnerId/deployments/:deploymentId/legacy-access"

type legacyDeploymentAccess struct {
	Legacy          bool      `json:"legacy"`
	OrganizationID  string    `json:"organization_id"`
	DeploymentID    uuid.UUID `json:"deployment_id"`
	RunnerID        string    `json:"runner_id"`
	EnvironmentUUID uuid.UUID `json:"environment_uuid"`
}

func (s *Server) getLegacyDeploymentAccess(c echo.Context) error {
	org, runner, rawID := c.Param("orgId"), c.Param("runnerId"), c.Param("deploymentId")
	id, err := uuid.Parse(rawID)
	if err != nil || id == uuid.Nil || id.String() != rawID {
		return echo.NewHTTPError(http.StatusBadRequest)
	}
	// Reserve space for fixed receipt fields within the Gateway's 4096-byte response bound.
	if len(org)+len(runner) > 3072 || hmessaging.ValidateSubjectToken("organization id", org) != nil || hmessaging.ValidateSubjectToken("runner id", runner) != nil || strings.HasPrefix(runner, hmessaging.NativeRunnerTokenPrefix) {
		return echo.NewHTTPError(http.StatusBadRequest)
	}
	tokens := c.Request().Header.Values("X-Deployment-Token")
	expected := util.GenerateHashedRunnerToken(s.RunnerTokenSalt, org, id.String())
	if len(tokens) != 1 || tokens[0] == "" || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(expected)) != 1 {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}
	middleware.SetAuthChecked(c)
	ctx := c.Request().Context()
	tx, err := s.Database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable)
	}
	defer func() { _ = tx.Rollback() }()
	access := legacyDeploymentAccess{Legacy: true, OrganizationID: org, DeploymentID: id}
	err = tx.QueryRowContext(ctx, `SELECT d.runner_id,e.id FROM deployments d
 JOIN deployment_environments e ON e.id=d.de_id
 WHERE e.org_id=$1 AND d.id=$2 AND d.runner_id=$3 AND d.native_target IS NULL`, org, id, runner).Scan(&access.RunnerID, &access.EnvironmentUUID)
	if errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusNotFound)
	}
	if err != nil || access.EnvironmentUUID == uuid.Nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable)
	}
	if err := tx.Rollback(); err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable)
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, access)
}
