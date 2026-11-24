package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	p "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

type externalPipelineAdmission struct {
	Runner     *cp.Runner
	Definition *model.PipelineDefinition
	Attempt    *OrdinaryDeploymentAdmissionAttempt
}

func (s *Server) resolveExternalPipeline(ctx context.Context, org string, env *cp.Environment, mode model.DeploymentMode, selection *PipelineReference) (*model.PipelineDefinition, *cp.Runner, error) {
	choices, err := s.projectPipelineChoices(ctx, org, env)
	if err != nil {
		return nil, nil, err
	}
	var definition *model.PipelineDefinition
	if selection == nil {
		definition, err = selectProjectPipeline(choices, nil, mode)
	} else {
		raw, _ := json.Marshal(selection)
		var reference p.PipelineReference
		if err = json.Unmarshal(raw, &reference); err != nil {
			return nil, nil, err
		}
		if err = p.ValidateReference(reference); err != nil {
			return nil, nil, model.NewErrBadRequest(err.Error())
		}
		if choices == nil {
			return nil, nil, model.NewErrConflict("pipeline policy service is unavailable")
		}
		definition, err = externalPipelineDefinition(choices, reference, mode)
	}
	if err != nil {
		return nil, nil, err
	}
	if err = s.bindRequestedPipelineCostPolicy(ctx, definition); err != nil {
		return nil, nil, err
	}
	runner, err := s.requestedPipelineRunner(ctx, org, env)
	if err != nil {
		return nil, nil, err
	}
	target, err := nativeExecutionTarget(org, *env.RunnerId, *runner, mode, definition)
	if err != nil {
		return nil, nil, err
	}
	if target != nil {
		if err := requireNativeFirstEnvironment(*env); err != nil {
			return nil, nil, err
		}
	}
	definition, err = pipelineForRunner(*runner, mode, definition)
	return definition, runner, err
}
func pipelineAdmissionClosed(ctx context.Context, tx model.Tx, id uuid.UUID) (bool, error) {
	receipt, err := model.GetDeploymentAdmissionReceiptByDeployment(ctx, tx, id)
	if _, missing := model.IsErrNotFound(err); missing {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return receipt.CoreSettledAt.Valid, nil
}
func (a *ExternalDeploymentHTTPAdapter) resolvePipelineAdmission(ctx context.Context, actor uuid.UUID, org, plugin, requestID string, env *cp.Environment, mode model.DeploymentMode, selection *PipelineReference) (externalPipelineAdmission, error) {
	if a.OrdinaryAdmissions == nil || a.Server == nil {
		return externalPipelineAdmission{}, ErrExternalDeploymentUnavailable
	}
	definition, runner, err := a.Server.resolveExternalPipeline(ctx, org, env, mode, selection)
	if err != nil {
		if _, conflict := model.IsErrConflict(err); conflict {
			return externalPipelineAdmission{}, err
		}
		if _, bad := model.IsErrBadRequest(err); bad {
			return externalPipelineAdmission{}, err
		}
		return externalPipelineAdmission{}, errors.Join(ErrExternalDeploymentUnavailable, err)
	}
	attempt := a.OrdinaryAdmissions.NewAttempt(org, *env.ProjectUuid, env.Uuid, actor, mode, "external-pipeline:"+plugin+":"+requestID)
	return externalPipelineAdmission{Definition: definition, Runner: runner, Attempt: attempt}, nil
}
func externalPipelineDefinition(choices *p.PipelineChoices, reference p.PipelineReference, mode model.DeploymentMode) (*model.PipelineDefinition, error) {
	if reference.Builtin != nil {
		if choices.Policy.Enabled {
			return nil, model.NewErrConflict("project requires an allowed published pipeline")
		}
		return model.ResolvePipeline(nil, mode)
	}
	for _, version := range choices.Versions {
		if version.Id == *reference.VersionId && version.Digest == reference.Digest {
			return publishedPipeline(version, mode)
		}
	}
	return nil, model.NewErrConflict("selected pipeline is unavailable under current project policy")
}
