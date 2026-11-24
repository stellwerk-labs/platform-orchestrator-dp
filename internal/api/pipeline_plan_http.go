package api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const PipelineApplyPlanPath = CanonicalPipelinePath + "/actions/apply-plan"
const CanonicalPipelineReplanPath = CanonicalPipelinePath + "/actions/replan"

func (s *Server) ApplyDeploymentPipelinePlan(ctx context.Context, request ApplyDeploymentPipelinePlanRequestObject) (ApplyDeploymentPipelinePlanResponseObject, error) {
	actor, err := pipelineCaller(ctx)
	if err != nil {
		return nil, err
	}
	if err = s.validatePipelinePlanAction(ctx, request.Params.IdempotencyKey); err != nil {
		return nil, err
	}
	child, err := s.applyPipelinePlan(ctx, actor, request.OrgId, request.DeploymentId, request.Body.Subject, request.Params.IdempotencyKey)
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			middleware.SetAuthCheckedCtx(ctx)
		}
		response, responseErr := createDeploymentErrorResponse(err)
		switch failure := response.(type) {
		case CreateDeployment400JSONResponse:
			return ApplyDeploymentPipelinePlan400JSONResponse(failure), nil
		case CreateDeployment409JSONResponse:
			return ApplyDeploymentPipelinePlan409JSONResponse(failure), nil
		}
		return nil, responseErr
	}
	return ApplyDeploymentPipelinePlan201JSONResponse{Id: child.Id}, nil
}

func (s *Server) ReplanDeploymentPipelinePlan(ctx context.Context, request ReplanDeploymentPipelinePlanRequestObject) (ReplanDeploymentPipelinePlanResponseObject, error) {
	actor, err := pipelineCaller(ctx)
	if err != nil {
		return nil, err
	}
	if err = s.validatePipelinePlanAction(ctx, request.Params.IdempotencyKey); err != nil {
		return nil, err
	}
	child, err := s.replanPipelinePlan(ctx, actor, request.OrgId, request.DeploymentId, request.Params.IdempotencyKey)
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			middleware.SetAuthCheckedCtx(ctx)
		}
		response, responseErr := createDeploymentErrorResponse(err)
		switch failure := response.(type) {
		case CreateDeployment400JSONResponse:
			return ReplanDeploymentPipelinePlan400JSONResponse(failure), nil
		case CreateDeployment409JSONResponse:
			return ReplanDeploymentPipelinePlan409JSONResponse(failure), nil
		}
		return nil, responseErr
	}
	return ReplanDeploymentPipelinePlan201JSONResponse{Id: child.Id}, nil
}

func (s *Server) validatePipelinePlanAction(ctx context.Context, key string) error {
	if c := middleware.GetEchoCtx(ctx); c != nil {
		if origin := c.Request().Header.Get("Origin"); origin != "" {
			u, err := url.Parse(s.UiHostUrl)
			if err != nil || u.Host == "" || origin != u.Scheme+"://"+u.Host {
				return echo.NewHTTPError(http.StatusForbidden, "origin refused")
			}
		}
	}
	if key == "" || len(key) > 128 || strings.ContainsAny(key, "\r\n") {
		return echo.NewHTTPError(http.StatusBadRequest, "Idempotency-Key required (max 128 bytes)")
	}
	return nil
}

var pipelinePlanActionURL = regexp.MustCompile(`^/orgs/[^/]+/deployments/[^/]+/pipeline/(actions/(apply-plan|replan)|controls)$`)

func pipelinePlanBodyLimitSkipper(c echo.Context) bool {
	return c.Request().Method != http.MethodPost || !pipelinePlanActionURL.MatchString(c.Request().URL.EscapedPath())
}

func pipelinePlanBodyLimit(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if pipelinePlanBodyLimitSkipper(c) {
			return next(c)
		}
		request := c.Request()
		body := request.Body
		defer func() { _ = body.Close() }()
		if request.ContentLength > consoleJSONLimit {
			return echo.ErrStatusRequestEntityTooLarge
		}
		raw, err := io.ReadAll(io.LimitReader(request.Body, consoleJSONLimit+1))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid request body").SetInternal(err)
		}
		if len(raw) > consoleJSONLimit {
			return echo.ErrStatusRequestEntityTooLarge
		}
		if strings.HasSuffix(request.URL.EscapedPath(), "/pipeline/controls") && !pipelineControlObject(raw, "action subject failed_step revision", "action") {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid control body")
		}
		request.Body = io.NopCloser(bytes.NewReader(raw))
		return next(c)
	}
}
