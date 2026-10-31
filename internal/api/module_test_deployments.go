package api

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

type moduleTestDeploymentResponse struct{ CreateDeploymentResponseObject }

func (r moduleTestDeploymentResponse) VisitCreateModuleTestDeploymentResponse(w http.ResponseWriter) error {
	return r.VisitCreateDeploymentResponse(w)
}

func (s *Server) CreateModuleTestDeployment(ctx context.Context, request CreateModuleTestDeploymentRequestObject) (CreateModuleTestDeploymentResponseObject, error) {
	if request.Body == nil {
		return nil, model.NewErrBadRequest("a captured test selection is required")
	}
	selection := request.Body.DraftTestSnapshots
	if err := validateDraftTestSelection(selection); err != nil {
		response, err := createDeploymentErrorResponse(err)
		return moduleTestDeploymentResponse{response}, err
	}
	body := request.Body.Deployment
	if body.Mode != DeploymentCreateBodyModeDeploy && body.Mode != DeploymentCreateBodyModePlanOnly {
		response, err := createDeploymentErrorResponse(model.NewErrBadRequest("fresh Module tests support deploy and plan-only; rollback uses retained Deployment history"))
		return moduleTestDeploymentResponse{response}, err
	}
	response, err := s.createDeployment(ctx, CreateDeploymentRequestObject{OrgId: request.OrgId, Params: CreateDeploymentParams(request.Params), Body: &body}, selection)
	if err != nil {
		return nil, err
	}
	return moduleTestDeploymentResponse{response}, nil
}

func validateDraftTestSelection(selection map[string]uuid.UUID) error {
	if len(selection) == 0 || len(selection) > 1000 {
		return model.NewErrBadRequest("select between 1 and 1000 immutable Module test snapshots")
	}
	for key, snapshot := range selection {
		module, err := uuid.Parse(key)
		if err != nil || module == uuid.Nil || module.String() != key || snapshot == uuid.Nil {
			return model.NewErrBadRequest("Module test selection requires canonical Module and snapshot UUIDs")
		}
	}
	return nil
}
