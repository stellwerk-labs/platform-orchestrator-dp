package api

import (
	"context"
	"io"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

const (
	RunnerRolloutPlanLogsPath      = RunnerRolloutReadPath + "/plan-logs"
	RunnerRolloutExecutionLogsPath = RunnerRolloutReadPath + "/execution-logs"
	runnerPlanLogLimit             = 4 << 20
	runnerPlanSubjectHeader        = "X-Stellwerk-Plan-Subject"
)

func (p *RunnerRolloutService) planLogs(c echo.Context) error {
	env, actor, err := p.scope(c, PermissionDeploymentRead)
	if err != nil {
		return err
	}
	if err = p.assigned(env); err != nil {
		return err
	}
	run, err := p.read(c, env, actor)
	if err != nil {
		return err
	}
	if err = p.Server.checkEnvAuthorization(c.Request().Context(), actor, p.OrgID, env.Uuid, PermissionDeploymentLogsRead); err != nil {
		return err
	}
	if c.QueryParam("subject") != run.Subject {
		return echo.NewHTTPError(http.StatusConflict, "the plan changed; reload its status")
	}
	content, err := p.loadPlanLogs(c.Request().Context(), actor, run)
	if err != nil {
		return err
	}
	c.Response().Header().Set(runnerPlanSubjectHeader, run.Subject)
	c.Response().Header().Set("X-Content-Type-Options", "nosniff")
	return c.Blob(http.StatusOK, "text/plain; charset=utf-8", content)
}

func (p *RunnerRolloutService) loadPlanLogs(ctx context.Context, actor uuid.UUID, run runnerRolloutView) ([]byte, error) {
	return p.loadPipelineLog(ctx, actor, run, "plan")
}

func (p *RunnerRolloutService) loadPipelineLog(ctx context.Context, actor uuid.UUID, run runnerRolloutView, kind string) ([]byte, error) {
	subject := run.Subject
	if kind == "execution" {
		subject = ""
	}
	address := p.BaseURL + "/api/runner-rollouts/" + run.ID + "/" + kind + "-logs?subject=" + url.QueryEscape(subject)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "plan logs unavailable")
	}
	request.Header.Set("Authorization", "Bearer "+p.Token)
	request.Header.Set("X-Rollout-Actor", actor.String())
	request.Header.Set("Accept", "text/plain")
	response, err := p.Client.Do(request)
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "the Runner site is unavailable; try again")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, runnerPlanLogUpstreamError(response.StatusCode)
	}
	if response.Header.Get(runnerPlanSubjectHeader) != subject {
		return nil, echo.NewHTTPError(http.StatusConflict, "the plan changed; reload its status")
	}
	content, err := io.ReadAll(io.LimitReader(response.Body, runnerPlanLogLimit+1))
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "plan logs unavailable; try again")
	}
	if len(content) > runnerPlanLogLimit {
		return nil, runnerPlanLogUpstreamError(http.StatusRequestEntityTooLarge)
	}
	return content, nil
}

func runnerPlanLogUpstreamError(status int) error {
	switch status {
	case http.StatusNotFound:
		return echo.NewHTTPError(status, "plan logs were not captured for this run or are not available yet")
	case http.StatusConflict:
		return echo.NewHTTPError(status, "the plan changed; reload its status")
	case http.StatusRequestEntityTooLarge:
		return echo.NewHTTPError(status, "plan logs exceed the 4 MiB viewer limit")
	default:
		return echo.NewHTTPError(http.StatusServiceUnavailable, "the Runner site is unavailable; try again")
	}
}

func (p *RunnerRolloutService) executionLogs(c echo.Context) error {
	env, actor, err := p.scope(c, PermissionDeploymentRead)
	if err != nil {
		return err
	}
	if err = p.assigned(env); err != nil {
		return err
	}
	run, err := p.read(c, env, actor)
	if err != nil {
		return err
	}
	if err = p.Server.checkEnvAuthorization(c.Request().Context(), actor, p.OrgID, env.Uuid, PermissionDeploymentLogsRead); err != nil {
		return err
	}
	if c.QueryParam("subject") != "" {
		return echo.NewHTTPError(http.StatusBadRequest, "execution logs are scoped to the Deployment")
	}
	content, err := p.loadPipelineLog(c.Request().Context(), actor, run, "execution")
	if err != nil {
		return err
	}
	c.Response().Header().Set(runnerPlanSubjectHeader, "")
	c.Response().Header().Set("X-Content-Type-Options", "nosniff")
	return c.Blob(http.StatusOK, "text/plain; charset=utf-8", content)
}

func (s *Server) getPipelineDeploymentLogs(ctx context.Context, request GetDeploymentLogsRequestObject, uid uuid.UUID, ds *model.DeploymentSummary, managed bool) (GetDeploymentLogsResponseObject, error) {
	if !managed || request.Params.DecryptKey != nil {
		return GetDeploymentLogs409JSONResponse{N409ConflictJSONResponse: Generate409Response("pipeline logs support automatic log access only")}, nil
	}
	if err := s.checkEnvAuthorization(ctx, uid, request.OrgId, ds.DeploymentEnvUuid, PermissionDeploymentRead); err != nil {
		return nil, err
	}
	if s.RunnerRollouts == nil {
		return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "pipeline log adapter unavailable")
	}
	content, loadErr := s.RunnerRollouts.loadPipelineLog(ctx, uid, runnerRolloutView{ID: ds.Id.String()}, "execution")
	if loadErr != nil {
		return nil, loadErr
	}
	return GetDeploymentLogs200TextResponse{Body: string(content), Headers: GetDeploymentLogs200ResponseHeaders{CacheControl: ref.Ref("no-store"), XContentTypeOptions: ref.Ref("nosniff")}}, nil
}
