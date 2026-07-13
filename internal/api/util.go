package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/herrors"
	"github.com/stellwerk-labs/golib/hlogger"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/authz"
	platformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const notFoundErrorCode = "HTTP-404"

func Generate400Response(message string) N400BadRequestJSONResponse {
	return N400BadRequestJSONResponse{Error: "HTTP-400", Message: message}
}

func Generate404Response(message string) N404NotFoundJSONResponse {
	return N404NotFoundJSONResponse{Error: notFoundErrorCode, Message: message}
}

func Generate409Response(message string) N409ConflictJSONResponse {
	return N409ConflictJSONResponse{Error: "HTTP-409", Message: message}
}

func (resp N400BadRequestJSONResponse) WithDetails(details *map[string]interface{}) N400BadRequestJSONResponse {
	resp.Details = details
	return resp
}

func Generate400FromModelErr(e model.ErrBadRequest) N400BadRequestJSONResponse {
	return Generate400Response(e.Message)
}

func Generate404FromModelErr(e model.ErrNotFound) N404NotFoundJSONResponse {
	return N404NotFoundJSONResponse{Error: notFoundErrorCode, Message: e.Message}
}

func Generate409FromModelErr(e model.ErrConflict) N409ConflictJSONResponse {
	return Generate409Response(e.Message)
}

func checkIfOrganizationExists(ctx context.Context, client platformorchestratorcp.ClientWithResponsesInterface, orgId string) (bool, error) {
	resp, err := client.GetInternalOrganizationWithResponse(ctx, orgId)
	if err != nil {
		return false, errors.Wrap(err, "failed to get organization")
	}
	switch resp.StatusCode() {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, errors.Wrap(fmt.Errorf("returned unexpected status code: %d, with body: %s", resp.StatusCode(), string(resp.Body)), "getting organization")
	}
}

// GetAuthenticatedUserId retrieves the human or service users id from the authenticated From HTTP header.
func GetAuthenticatedUserId(ctx context.Context) (uuid.UUID, error) {
	return uuid.Parse(hecho.GetUserID(ctx))
}

// GetAuthenticatedUserIdOr401 is the same as GetAuthenticatedUserId but returns a useful http 401 error
func GetAuthenticatedUserIdOr401(ctx context.Context) (uuid.UUID, *echo.HTTPError) {
	if u, err := GetAuthenticatedUserId(ctx); err == nil {
		return u, nil
	}
	return uuid.Nil, echo.NewHTTPError(http.StatusUnauthorized)
}

func (s *Server) checkOrgReadAuthorization(ctx context.Context, userId uuid.UUID, orgId string) error {
	return s.innerCheck(ctx, userId, orgId, []platformorchestratoriam.ResourcePermissionCheck{authz.CanReadOrgCheck(orgId)})
}

func (s *Server) checkOrgManageAuthorization(ctx context.Context, userId uuid.UUID, orgId string) error {
	return s.innerCheck(ctx, userId, orgId, []platformorchestratoriam.ResourcePermissionCheck{authz.CanManageOrgCheck(orgId)})
}

func (s *Server) checkEnvWriteAuthorization(ctx context.Context, userId uuid.UUID, orgId string, envUuid uuid.UUID) error {
	if scopedErr := s.innerCheck(ctx, userId, orgId, []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)}); scopedErr != nil {
		// If the scoped check fails, we fall back to the org manage check for compatibility with older envs
		if orgErr := s.innerCheck(ctx, userId, orgId, []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteOrgCheck(orgId)}); orgErr != nil {
			return scopedErr
		}
	}
	return nil
}

func (s *Server) innerCheck(ctx context.Context, userId uuid.UUID, orgId string, checks []platformorchestratoriam.ResourcePermissionCheck) error {
	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.OrgId = orgId
	ids.UserId = userId.String()
	logger := hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).WithLazy(ids.AsLogField())

	if c := middleware.GetEchoCtx(ctx); c != nil && c.Request().Header.Get("Po-Org-Id") == orgId {
		logger.Warn("DEPRECATED ORG-TOKEN authorization used, please switch to real users")
		middleware.SetAuthCheckedCtx(ctx)
		return nil
	}

	if userId == userid.InternalSystemUuid {
		// system user id can do these things for now
	} else if r, err := s.IamClient.InternalAuthorizeWithResponse(ctx, platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: checks,
	}); err != nil {
		return errors.Wrap(err, "failed to check authorization")
	} else if r.StatusCode() == http.StatusForbidden {
		return &herrors.PlatformOrchestratorError{
			StatusCode: http.StatusForbidden,
			Details:    *r.JSON403.Details,
		}
	} else if r.StatusCode() != http.StatusNoContent {
		return errors.Errorf("unexpected status code when checking authorization: %s: %s", r.Status(), string(r.Body))
	}
	middleware.SetAuthCheckedCtx(ctx)
	return nil
}
