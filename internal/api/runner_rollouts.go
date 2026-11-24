package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
)

const (
	RunnerRolloutsPath       = "/orgs/:orgId/projects/:projectId/envs/:envId/runner-rollouts"
	RunnerRolloutCatalogPath = RunnerRolloutsPath + "/catalog"
	RunnerRolloutReadPath    = RunnerRolloutsPath + "/:rolloutId"
	RunnerRolloutActionPath  = RunnerRolloutReadPath + "/:action"
	runnerRolloutLimit       = 1 << 20
	runnerRolloutTimeout     = 15 * time.Second
	runnerRolloutApprove     = "approve"
	runnerRolloutCancel      = "cancel"
	runnerRolloutRepair      = "repair"
)

// RunnerRolloutService exposes the isolated rollout experiment through normal IAM.
// The adapter retains command authoring and projection; execution stays at the site.
type RunnerRolloutService struct {
	Server                          *Server
	BaseURL, Token, OrgID, RunnerID string
	Client                          *http.Client
}

func NewRunnerRolloutService(s *Server, address, tokenFile, orgID, runnerID string) (*RunnerRolloutService, error) {
	u, err := url.Parse(address)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || orgID == "" || runnerID == "" {
		return nil, fmt.Errorf("invalid runner rollout adapter configuration")
	}
	token, err := LoadServiceBearerToken(tokenFile)
	if err != nil {
		return nil, err
	}
	return &RunnerRolloutService{Server: s, BaseURL: strings.TrimRight(address, "/"), Token: token, OrgID: orgID, RunnerID: runnerID, Client: &http.Client{Timeout: runnerRolloutTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

type runnerRolloutView struct {
	InfrastructureApplied bool       `json:"infrastructure_applied"`
	ID                    string     `json:"id"`
	EnvironmentID         string     `json:"environment_id"`
	Phase                 string     `json:"phase"`
	Subject               string     `json:"subject,omitempty"`
	ErrorCode             string     `json:"error_code,omitempty"`
	FailedStep            string     `json:"failed_step,omitempty"`
	Authority             string     `json:"authority,omitempty"`
	Revision              int64      `json:"revision"`
	CreatedAt             time.Time  `json:"created_at"`
	LastReceivedAt        *time.Time `json:"last_received_at,omitempty"`
	Changes               *struct {
		Add    int `json:"add"`
		Change int `json:"change"`
		Remove int `json:"remove"`
	} `json:"changes,omitempty"`
	PlanChanges      *model.PipelineChanges          `json:"plan_changes,omitempty"`
	ApplyChanges     *model.PipelineChanges          `json:"apply_changes,omitempty"`
	Cost             *model.PipelineCostComparison   `json:"cost,omitempty"`
	ApprovalDecision *model.PipelineApprovalDecision `json:"approval_decision,omitempty"`
	Steps            []model.PipelineStepResult      `json:"steps"`
}

func (p *RunnerRolloutService) MapRoutes(e *echo.Echo) {
	e.GET(RunnerRolloutsPath, p.list)
	e.GET(RunnerRolloutCatalogPath, p.catalog)
	e.GET(RunnerRolloutReadPath, p.get)
	e.GET(RunnerRolloutPlanLogsPath, p.planLogs)
	e.GET(RunnerRolloutExecutionLogsPath, p.executionLogs)
	e.POST(RunnerRolloutsPath, p.create)
	e.POST(RunnerRolloutActionPath, p.action)
}

func (p *RunnerRolloutService) scope(c echo.Context, permission string) (*cp.Environment, uuid.UUID, error) {
	c.Response().Header().Set("Cache-Control", "no-store")
	actor, err := uuid.Parse(c.Request().Header.Get("From"))
	if err != nil || actor == uuid.Nil || actor == userid.InternalSystemUuid {
		return nil, uuid.Nil, echo.NewHTTPError(http.StatusUnauthorized, "authenticated user required")
	}
	if c.Request().Header.Get("Po-Org-Id") != "" {
		return nil, actor, echo.NewHTTPError(http.StatusForbidden, "IAM user authorization required")
	}
	if c.Param("orgId") != p.OrgID {
		return nil, actor, echo.NewHTTPError(http.StatusNotFound)
	}
	ctx := c.Request().Context()
	env, err := p.Server.ControlPlaneClient.GetEnvironmentWithResponse(ctx, p.OrgID, c.Param("projectId"), c.Param("envId"))
	if err != nil || env.JSON200 == nil {
		return nil, actor, echo.NewHTTPError(http.StatusNotFound, "environment unavailable")
	}
	if err = p.Server.checkEnvAuthorization(ctx, actor, p.OrgID, env.JSON200.Uuid, permission); err != nil {
		return nil, actor, err
	}
	return env.JSON200, actor, nil
}

func (p *RunnerRolloutService) assigned(env *cp.Environment) error {
	if env.RunnerId == nil || *env.RunnerId != p.RunnerID {
		return echo.NewHTTPError(http.StatusConflict, "environment is not assigned to the experimental Runner site")
	}
	return nil
}

func (p *RunnerRolloutService) list(c echo.Context) error {
	env, actor, err := p.scope(c, PermissionDeploymentRead)
	if err != nil {
		return err
	}
	if err = p.assigned(env); err != nil {
		return err
	}
	var all []runnerRolloutView
	if _, err = p.call(c.Request().Context(), actor, http.MethodGet, "", nil, "", &all); err != nil {
		return err
	}
	items := make([]runnerRolloutView, 0)
	for _, run := range all {
		if run.EnvironmentID == env.Uuid.String() {
			items = append(items, run)
		}
	}
	return c.JSON(http.StatusOK, items)
}

func (p *RunnerRolloutService) catalog(c echo.Context) error {
	env, actor, err := p.scope(c, PermissionDeploymentRead)
	if err != nil {
		return err
	}
	if err = p.assigned(env); err != nil {
		return err
	}
	var catalog struct {
		Site struct {
			OrgID        string   `json:"org_id"`
			RunnerID     string   `json:"runner_id"`
			Capabilities []string `json:"capabilities"`
		} `json:"site"`
		Tools []struct {
			ID                   string   `json:"id"`
			Name                 string   `json:"name"`
			Phase                string   `json:"phase"`
			RequiredCapabilities []string `json:"required_capabilities"`
			Compatible           bool     `json:"compatible"`
			Reason               string   `json:"reason,omitempty"`
		} `json:"tools"`
	}
	if _, err = p.call(c.Request().Context(), actor, http.MethodGet, "/catalog", nil, "", &catalog); err != nil {
		return err
	}
	if catalog.Site.OrgID != p.OrgID || catalog.Site.RunnerID != p.RunnerID {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "Runner site binding unavailable")
	}
	return c.JSON(http.StatusOK, catalog)
}

func (p *RunnerRolloutService) read(c echo.Context, env *cp.Environment, actor uuid.UUID) (runnerRolloutView, error) {
	var run runnerRolloutView
	id, err := uuid.Parse(c.Param("rolloutId"))
	if err != nil {
		return run, echo.NewHTTPError(http.StatusNotFound)
	}
	if p.Server.Database != nil {
		canonical, e := p.readCanonicalProgress(c, env.Uuid, id)
		if e != nil {
			return run, e
		}
		if canonical != nil {
			return *canonical, nil
		}
	}
	_, err = p.call(c.Request().Context(), actor, http.MethodGet, "/"+id.String(), nil, "", &run)
	if err != nil {
		return run, err
	}
	if run.EnvironmentID != env.Uuid.String() || run.ID != id.String() {
		return runnerRolloutView{}, echo.NewHTTPError(http.StatusNotFound)
	}
	return run, nil
}

func (p *RunnerRolloutService) get(c echo.Context) error {
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
	return c.JSON(http.StatusOK, run)
}

func (p *RunnerRolloutService) create(c echo.Context) error {
	return echo.NewHTTPError(http.StatusGone, "Create a normal Deployment with a pipeline selection")
}

func (p *RunnerRolloutService) action(c echo.Context) error {
	action := c.Param("action")
	if action != runnerRolloutApprove && action != runnerRolloutCancel && action != runnerRolloutRepair {
		return echo.NewHTTPError(http.StatusNotFound)
	}
	env, actor, err := p.scope(c, PermissionDeploymentWrite)
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
	var body struct {
		Subject string `json:"subject,omitempty"`
		StepID  string `json:"step_id,omitempty"`
	}
	if err = p.Server.decodeConsoleJSON(c, &body); err != nil {
		return err
	}
	status, err := p.call(c.Request().Context(), actor, http.MethodPost, "/"+run.ID+"/"+action, body, "", &run)
	if err != nil {
		return err
	}
	if run.EnvironmentID != env.Uuid.String() {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "rollout scope unavailable")
	}
	return c.JSON(status, run)
}

func (p *RunnerRolloutService) call(ctx context.Context, actor uuid.UUID, method, path string, body any, key string, target any) (int, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.BaseURL+"/api/runner-rollouts"+path, reader)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+p.Token)
	req.Header.Set("X-Rollout-Actor", actor.String())
	req.Header.Set("Content-Type", observationJSONContentType)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	response, err := p.Client.Do(req)
	if err != nil {
		return 0, echo.NewHTTPError(http.StatusServiceUnavailable, "rollout service unavailable")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted {
		return 0, runnerRolloutUpstreamError(response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, runnerRolloutLimit+1))
	if err != nil || len(data) > runnerRolloutLimit || json.Unmarshal(data, target) != nil {
		return 0, echo.NewHTTPError(http.StatusServiceUnavailable, "rollout projection unavailable")
	}
	return response.StatusCode, nil
}

func runnerRolloutUpstreamError(status int) error {
	switch status {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusConflict:
		return echo.NewHTTPError(status, "rollout request refused; reload its status")
	default:
		return echo.NewHTTPError(http.StatusServiceUnavailable, "rollout service unavailable")
	}
}
