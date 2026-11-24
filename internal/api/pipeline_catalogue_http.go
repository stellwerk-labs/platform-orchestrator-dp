package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	publication "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func (s *Server) ListDeploymentPipelines(ctx context.Context, request ListDeploymentPipelinesRequestObject) (ListDeploymentPipelinesResponseObject, error) {
	actor, err := pipelineCaller(ctx)
	if err != nil {
		return nil, err
	}
	env, err := s.pipelineCatalogueEnvironment(ctx, request)
	if err != nil {
		return nil, err
	}
	if err = s.checkEnvAuthorization(ctx, actor, request.OrgId, env.Uuid, PermissionDeploymentRead); err != nil {
		return nil, err
	}
	choices, err := s.projectPipelineChoices(ctx, request.OrgId, env)
	if err != nil {
		return nil, pipelineHTTPError(err)
	}
	definitions, defaultID, err := pipelineCatalogueDefinitions(choices)
	if err != nil {
		return nil, pipelineHTTPError(err)
	}
	costEnabled, err := s.catalogueCostEnabled(ctx, definitions)
	if err != nil {
		return nil, err
	}
	runner, err := s.pipelineCatalogueRunner(ctx, request, env)
	if err != nil {
		return nil, err
	}
	view := DeploymentPipelineCatalogue{DefaultPipelineId: defaultID, Items: []DeploymentPipelineCatalogueItem{}}
	for _, definition := range definitions {
		item, err := pipelineCatalogueItem(definition, runner, costEnabled)
		if err != nil {
			return nil, err
		}
		view.Items = append(view.Items, item)
	}
	if err = pipelineCataloguePolicy(&view, choices); err != nil {
		return nil, err
	}
	return ListDeploymentPipelines200JSONResponse(view), nil
}

func (s *Server) pipelineCatalogueEnvironment(ctx context.Context, request ListDeploymentPipelinesRequestObject) (*cp.Environment, error) {
	response, err := s.ControlPlaneClient.GetEnvironmentWithResponse(ctx, request.OrgId, request.ProjectId, request.EnvId)
	if err != nil {
		return nil, err
	}
	if response.StatusCode() == http.StatusNotFound {
		return nil, echo.NewHTTPError(http.StatusNotFound)
	}
	if response.JSON200 == nil || response.JSON200.Id != request.EnvId || response.JSON200.ProjectId != request.ProjectId || response.JSON200.Uuid == uuid.Nil {
		return nil, echo.NewHTTPError(http.StatusBadGateway, "environment unavailable")
	}
	return response.JSON200, nil
}

func (s *Server) pipelineCatalogueRunner(ctx context.Context, request ListDeploymentPipelinesRequestObject, env *cp.Environment) (*cp.Runner, error) {
	if env.RunnerId == nil {
		dry := true
		selected, err := s.ControlPlaneClient.UpdateRunnerInAnEnvironmentWithResponse(ctx, request.OrgId, request.ProjectId, request.EnvId, &cp.UpdateRunnerInAnEnvironmentParams{DryRun: &dry})
		if err != nil || selected.JSON200 == nil {
			return nil, nil
		}
		env.RunnerId = &selected.JSON200.RunnerId
	}
	response, err := s.ControlPlaneClient.GetRunnerWithResponse(ctx, request.OrgId, *env.RunnerId)
	if err != nil {
		return nil, err
	}
	if response.JSON200 != nil && response.JSON200.Id != *env.RunnerId {
		return nil, echo.NewHTTPError(http.StatusBadGateway, "Runner configuration unavailable")
	}
	return response.JSON200, nil
}

func pipelineCatalogueDefinitions(choices *publication.PipelineChoices) ([]model.PipelineDefinition, string, error) {
	definitions := []model.PipelineDefinition{}
	defaultID := model.PipelineDefault
	if choices == nil || !choices.Policy.Enabled {
		definitions = model.PipelineDefinitions()
	} else if choices.EffectiveDefault != nil && choices.EffectiveDefault.VersionId != nil {
		defaultID = choices.EffectiveDefault.VersionId.String()
	} else if choices.Policy.DefaultVersionId != nil {
		defaultID = choices.Policy.DefaultVersionId.String()
	}
	if choices != nil {
		for _, version := range choices.Versions {
			definition, err := publishedPipeline(version, model.DeploymentModeDeploy)
			if err != nil {
				return nil, "", err
			}
			definitions = append(definitions, *definition)
		}
	}
	return definitions, defaultID, nil
}

func pipelineCatalogueItem(definition model.PipelineDefinition, runner *cp.Runner, costEnabled bool) (DeploymentPipelineCatalogueItem, error) {
	var item DeploymentPipelineCatalogueItem
	raw, err := json.Marshal(definition)
	if err != nil {
		return item, err
	}
	if err = json.Unmarshal(raw, &item); err != nil {
		return item, err
	}
	switch {
	case definition.Cost != nil && definition.Cost.Enabled && !costEnabled:
		item.Reason = model.CostEstimationDisabledMessage
	case runner == nil:
		item.Reason = "Runner configuration is unavailable"
	default:
		if _, err := pipelineForRunner(*runner, model.DeploymentModeDeploy, &definition); err != nil {
			item.Reason = err.Error()
		}
	}
	item.Compatible = item.Reason == ""
	return item, nil
}

func pipelineCataloguePolicy(view *DeploymentPipelineCatalogue, choices *publication.PipelineChoices) error {
	if choices == nil {
		return nil
	}
	raw, err := json.Marshal(choices.Policy)
	if err != nil {
		return err
	}
	var policy DeploymentPipelinePolicy
	if err = json.Unmarshal(raw, &policy); err != nil {
		return err
	}
	view.Policy = &policy
	if choices.DefaultSource != nil {
		source := DeploymentPipelineCatalogueDefaultSource(*choices.DefaultSource)
		view.DefaultSource = &source
	}
	return nil
}
