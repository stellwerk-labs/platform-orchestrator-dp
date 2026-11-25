package api

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/labstack/echo/v4"
	iam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
)

func (s *Server) authorizeDeploymentList(ctx context.Context, org string, project, environment *string) error {
	actor, authErr := GetAuthenticatedUserIdOr401(ctx)
	if authErr != nil {
		return authErr
	}
	if project == nil {
		if environment != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "project_id param required if env_id is specified")
		}
		return s.checkOrgAuthorization(ctx, actor, org, PermissionDeploymentRead)
	}
	if environment != nil {
		response, err := s.ControlPlaneClient.GetEnvironmentWithResponse(ctx, org, *project, *environment)
		if err != nil {
			return err
		}
		if response.StatusCode() == http.StatusNotFound {
			// A missing Environment has nothing to disclose; authorize the list like an unfiltered project list.
			return s.authorizeProjectDeploymentList(ctx, actor, org, *project)
		}
		if response.JSON200 == nil || response.JSON200.Id != *environment || response.JSON200.ProjectId != *project || response.JSON200.Uuid == uuid.Nil {
			return echo.NewHTTPError(http.StatusBadGateway, "environment unavailable")
		}
		return s.checkEnvAuthorization(ctx, actor, org, response.JSON200.Uuid, PermissionDeploymentRead)
	}
	return s.authorizeProjectDeploymentList(ctx, actor, org, *project)
}

func (s *Server) authorizeProjectDeploymentList(ctx context.Context, actor uuid.UUID, org, project string) error {
	response, err := s.ControlPlaneClient.GetProjectWithResponse(ctx, org, project)
	if err != nil {
		return err
	}
	if response.StatusCode() == http.StatusNotFound {
		// A missing project has nothing to disclose; authorize the list like an unfiltered organization list.
		return s.checkOrgAuthorization(ctx, actor, org, PermissionDeploymentRead)
	}
	if response.JSON200 == nil || response.JSON200.Id != project || response.JSON200.Uuid == uuid.Nil {
		return echo.NewHTTPError(http.StatusBadGateway, "project unavailable")
	}
	check := iam.ResourcePermissionCheck{Resource: "project:" + response.JSON200.Uuid.String(), Permission: PermissionDeploymentRead}
	if err := s.innerCheck(ctx, actor, org, []iam.ResourcePermissionCheck{check}); err != nil {
		if orgErr := s.checkOrgAuthorization(ctx, actor, org, PermissionDeploymentRead); orgErr != nil {
			return err
		}
	}
	return nil
}
