package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	cperrors "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/errcodes"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func compilationCatalogue(
	ctx context.Context,
	client cp.ClientWithResponsesInterface,
	org, project, environment string,
	body cp.InternalModuleCatalogueGenerateBody,
	options DeploymentOptions,
	retained []cp.ModuleDraftTestCoordinate,
	editors []cp.RequestEditorFn,
) (*cp.InternalExecutionCatalogue, error) {
	var status int
	var catalogue *cp.InternalExecutionCatalogue
	var conflict *cp.N409Conflict
	var err error
	if len(options.DraftTestSnapshots) > 0 || len(retained) > 0 || (len(options.ExplicitModuleVersions) > 0 && options.ExecutionCatalogue != nil) {
		if options.ExecutionCatalogue == nil {
			return nil, ErrDeploymentAdmissionUnavailable
		}
		request := cp.InternalExecutionCatalogueGenerateBody{
			PinnedModuleVersions:       body.PinnedModuleVersions,
			PinnedProviders:            body.PinnedProviders,
			AreRulesIgnored:            body.AreRulesIgnored,
			RetainedDraftTestSnapshots: retained,
			MaterialOnly:               body.AreRulesIgnored && len(options.DraftTestSnapshots) == 0,
		}
		if len(options.DraftTestSnapshots) > 0 {
			request.DraftTestSnapshots = &options.DraftTestSnapshots
		}
		if !request.MaterialOnly && len(options.ExplicitModuleVersions) > 0 {
			request.SelectedPublishedModuleVersions = &options.ExplicitModuleVersions
		}
		response, callErr := options.ExecutionCatalogue.GenerateInternalExecutionCatalogueWithResponse(ctx, org, project, environment, nil, request, editors...)
		if callErr != nil {
			return nil, callErr
		}
		status, catalogue, conflict = response.StatusCode(), response.JSON200, response.JSON409
		if status == http.StatusNotFound {
			return nil, model.NewErrConflict("the execution catalogue or requested immutable material is unavailable; no published-catalogue fallback was attempted")
		}
	} else {
		response, callErr := client.GenerateInternalModuleCatalogueWithResponse(ctx, org, project, environment, nil, body, editors...)
		if callErr != nil {
			return nil, callErr
		}
		status, conflict = response.StatusCode(), response.JSON409
		if status == http.StatusOK {
			catalogue, err = graphs.DecodeExecutionCatalogue(response.Body, response.JSON200)
		}
		if status == http.StatusNotFound && response.JSON404 != nil {
			return nil, model.NewErrConflict(response.JSON404.Message)
		}
	}
	if status == http.StatusConflict && conflict != nil {
		if conflict.Error == string(cperrors.PinnedModuleMissingProvider) {
			var details string
			if conflict.Details != nil {
				if missing, ok := (*conflict.Details)["missing_providers"].([]interface{}); ok {
					details = fmt.Sprintf(": %s", missing)
				}
			}
			return nil, model.NewErrConflict(fmt.Sprintf("%s: existing module(s) depend on providers that no longer exist%s", conflict.Error, details))
		}
		return nil, model.NewErrConflict(conflict.Message)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("module catalogue request failed with HTTP %d", status)
	}
	if err != nil {
		return nil, err
	}
	if catalogue == nil {
		return nil, model.NewErrConflict("module catalogue response is missing")
	}
	return catalogue, nil
}

func validateCatalogueDraftSelection(catalogue *cp.InternalExecutionCatalogue, selected map[string]uuid.UUID, retained []cp.ModuleDraftTestCoordinate, environment uuid.UUID, published map[string]string) (map[string]string, error) {
	allowed := map[string]bool{}
	found := map[string]bool{}
	seen := map[string]bool{}
	resolved := map[string]string{}
	for module, snapshot := range selected {
		allowed[module+"/"+snapshot.String()] = true
	}
	for _, coordinate := range retained {
		allowed[coordinate.ModuleUuid.String()+"/"+coordinate.SnapshotUuid.String()] = true
	}
	for _, module := range catalogue.Modules {
		reference, err := graphs.DraftTestReferenceFromCatalogue(module)
		if err != nil {
			return nil, err
		}
		if reference == nil {
			continue
		}
		key := reference.ModuleUUID.String() + "/" + reference.SnapshotUUID.String()
		if seen[key] || !allowed[key] || reference.EnvironmentUUID != environment {
			return nil, model.NewErrConflict("unrequested or incorrectly scoped Draft test material in catalogue")
		}
		seen[key] = true
		if selected[reference.ModuleUUID.String()] == reference.SnapshotUUID {
			if _, conflict := published[module.Id]; conflict {
				return nil, model.NewErrBadRequest("one Module cannot select both published and test executables")
			}
			found[reference.ModuleUUID.String()] = true
			resolved[module.Id] = module.VersionId
		} else if len(module.Rules) > 0 {
			return nil, model.NewErrConflict("retained test material cannot receive new Module Rules")
		}
	}
	if len(found) != len(selected) {
		return nil, model.NewErrConflict("the catalogue did not return the exact requested Draft test selection")
	}
	return resolved, nil
}
