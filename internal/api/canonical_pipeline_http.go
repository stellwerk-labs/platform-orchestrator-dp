package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
)

const CanonicalPipelinePath = "/orgs/:orgId/deployments/:deploymentId/pipeline"
const PipelineCataloguePath = "/orgs/:orgId/projects/:projectId/envs/:envId/deployment-pipelines"

func pipelineCaller(ctx context.Context) (uuid.UUID, error) {
	if c := middleware.GetEchoCtx(ctx); c != nil {
		c.Response().Header().Set("Cache-Control", "no-store")
		if c.Request().Header.Get("Po-Org-Id") != "" {
			return uuid.Nil, echo.NewHTTPError(http.StatusForbidden, "IAM user authorization required")
		}
	}
	if _, ok := ctx.Value(hecho.ContextKeyUserID).(string); !ok {
		return uuid.Nil, echo.NewHTTPError(http.StatusUnauthorized, "authenticated user required")
	}
	actor, err := GetAuthenticatedUserIdOr401(ctx)
	if err != nil || actor == uuid.Nil || actor == userid.InternalSystemUuid {
		return uuid.Nil, echo.NewHTTPError(http.StatusUnauthorized, "authenticated user required")
	}
	return actor, nil
}

func (s *Server) readDeploymentPipeline(ctx context.Context, id uuid.UUID) (*model.DeploymentPipeline, error) {
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	return model.ReadDeploymentPipeline(ctx, tx, id)
}

func (s *Server) GetDeploymentPipeline(ctx context.Context, request GetDeploymentPipelineRequestObject) (GetDeploymentPipelineResponseObject, error) {
	actor, err := pipelineCaller(ctx)
	if err != nil {
		return nil, err
	}
	dep, _, _, _, err := s.Database.GetDeployment(ctx, nil, request.OrgId, request.DeploymentId, model.GetModeDefault)
	if err != nil {
		return nil, pipelineHTTPError(err)
	}
	if err = s.checkEnvAuthorization(ctx, actor, dep.OrgId, dep.DeploymentEnvUuid, PermissionDeploymentRead); err != nil {
		return nil, err
	}
	run, err := s.readDeploymentPipeline(ctx, dep.Id)
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, echo.NewHTTPError(http.StatusNotFound, "Deployment does not use a pipeline")
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	plan, err := s.pipelinePlanStatus(ctx, tx, dep, run)
	if err != nil {
		return nil, err
	}
	origin, err := readDeploymentPluginOrigin(ctx, tx, dep.OrgId, dep.Id)
	if err != nil {
		return nil, err
	}
	view, err := deploymentPipelineView(run, plan, origin, dep.NativeTarget != nil)
	nativeLogs := dep.NativeTarget != nil
	view.NativeLogs = &nativeLogs
	return GetDeploymentPipeline200JSONResponse(view), err
}

func deploymentPipelineView(run *model.DeploymentPipeline, plan *pipelinePlanStatus, origin *deploymentPluginOrigin, native bool) (DeploymentPipelineExecution, error) {
	var view DeploymentPipelineExecution
	var controls *[]string
	if native {
		actions := []string{}
		switch run.Progress.Phase {
		case "awaiting_approval":
			actions = []string{"approve", "deny", runnerRolloutCancel}
		case "post_failed":
			actions = []string{"repair", runnerRolloutCancel}
		case "accepted", "planning", "planned", "applying", "pre_plan", "plan_check", pipelinePostApplyPhase:
			actions = []string{runnerRolloutCancel}
		}
		controls = &actions
	}
	raw, err := json.Marshal(struct {
		*model.DeploymentPipeline
		AvailableControls *[]string               `json:"available_controls,omitempty"`
		Plan              *pipelinePlanStatus     `json:"plan,omitempty"`
		Origin            *deploymentPluginOrigin `json:"origin,omitempty"`
	}{run, controls, plan, origin})
	if err != nil {
		return view, err
	}
	err = json.Unmarshal(raw, &view)
	return view, err
}

func (p *RunnerRolloutService) readCanonicalProgress(c echo.Context, environment uuid.UUID, id uuid.UUID) (*runnerRolloutView, error) {
	run, err := p.Server.readDeploymentPipeline(c.Request().Context(), id)
	if err != nil || run == nil {
		return nil, err
	}
	dep, _, _, _, err := p.Server.Database.GetDeployment(c.Request().Context(), nil, p.OrgID, id, model.GetModeDefault)
	if err != nil || dep.DeploymentEnvUuid != environment || dep.RunnerId != p.RunnerID {
		return nil, echo.NewHTTPError(http.StatusNotFound)
	}
	bytes, err := json.Marshal(run.Progress)
	if err != nil {
		return nil, err
	}
	var result runnerRolloutView
	if err = json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func pipelineHTTPError(err error) error {
	if conflict, ok := model.IsErrConflict(err); ok {
		return echo.NewHTTPError(http.StatusConflict, conflict.Message)
	}
	if bad, ok := model.IsErrBadRequest(err); ok {
		return echo.NewHTTPError(http.StatusBadRequest, bad.Message)
	}
	if missing, ok := model.IsErrNotFound(err); ok {
		return echo.NewHTTPError(http.StatusNotFound, missing.Message)
	}
	return err
}

func (s *Server) catalogueCostEnabled(ctx context.Context, definitions []model.PipelineDefinition) (bool, error) {
	for _, definition := range definitions {
		if definition.Cost != nil && definition.Cost.Enabled {
			policy, err := model.ReadCostPolicy(ctx, s.Database)
			return policy.Enabled, err
		}
	}
	return true, nil
}
