package api

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"

	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	graph "github.com/stellwerk-labs/platform-orchestrator-graph"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

// Deleted nodes need their original provider mappings, but must not participate in
// selecting modules for the new graph or override an Environment Pin.
func resolveDeletedModuleMaterial(ctx context.Context, client cp.ClientWithResponsesInterface, org, project, environment string,
	g *graph.Graph[*graphs.GraphNodeModuleConfig], editors []cp.RequestEditorFn,
) ([]cp.ModuleProvider, error) {
	missing := &graph.Graph[*graphs.GraphNodeModuleConfig]{Nodes: make(map[graph.ResourceCoordinate]graph.ResourceNode[*graphs.GraphNodeModuleConfig])}
	versions := make(map[string]bool)
	for coordinate, node := range g.Nodes {
		configuration := node.ModuleConfiguration
		if configuration != nil && configuration.Deleted && configuration.Definition == nil {
			missing.Nodes[coordinate] = node
			versions[configuration.DefinitionId+"@"+configuration.VersionId] = true
		}
	}
	if len(versions) == 0 {
		return nil, nil
	}
	if client == nil {
		return nil, model.NewErrConflict("cannot remove resources: historical module material requires the execution catalogue")
	}
	response, err := client.GenerateInternalExecutionCatalogueWithResponse(ctx, org, project, environment, nil, cp.InternalExecutionCatalogueGenerateBody{
		AreRulesIgnored: true, MaterialOnly: true, PinnedModuleVersions: slices.Sorted(maps.Keys(versions)),
	}, editors...)
	if err != nil {
		return nil, fmt.Errorf("failed to get historical modules for removed resources: %w", err)
	}
	switch response.StatusCode() {
	case http.StatusOK:
		if response.JSON200 == nil {
			return nil, model.NewErrConflict("cannot remove resources: historical module catalogue response is missing")
		}
	case http.StatusNotFound:
		return nil, model.NewErrConflict("cannot remove resources: historical module material is unavailable")
	case http.StatusConflict:
		if response.JSON409 != nil {
			return nil, model.NewErrConflict("cannot remove resources: " + response.JSON409.Message)
		}
		return nil, model.NewErrConflict("cannot remove resources: historical modules or providers are unavailable")
	default:
		return nil, fmt.Errorf("historical module catalogue request failed with HTTP %d", response.StatusCode())
	}
	if err := assignDeletedModuleMaterial(missing, response.JSON200.Modules); err != nil {
		return nil, err
	}
	for coordinate, node := range missing.Nodes {
		g.Nodes[coordinate] = node
	}
	return response.JSON200.Providers, nil
}

func assignDeletedModuleMaterial(deleted *graph.Graph[*graphs.GraphNodeModuleConfig], definitions []cp.InternalExecutionCatalogueModule) error {
	if _, err := graphs.AssignModuleDefinitionsToGraphNodes(deleted, definitions); err != nil {
		return model.NewErrConflict(err.Error())
	}
	for coordinate, node := range deleted.Nodes {
		if node.ModuleConfiguration.Definition == nil {
			return model.NewErrConflict(fmt.Sprintf("cannot remove resource '%s': historical module %s@%s is unavailable; restore that exact module version and its providers before retrying",
				coordinate, node.ModuleConfiguration.DefinitionId, node.ModuleConfiguration.VersionId))
		}
		node.ModuleConfiguration.HasInlineSource = false
	}
	return nil
}

// Keep the initial catalogue's provider configurations when both reads contain
// the same provider. Fetching deletion material must not change active nodes.
func mergeModuleProviders(current, historical []cp.ModuleProvider) []cp.ModuleProvider {
	known := make(map[string]bool, len(current)+len(historical))
	for _, provider := range current {
		known[provider.ProviderType+"."+provider.Id] = true
	}
	for _, provider := range historical {
		key := provider.ProviderType + "." + provider.Id
		if !known[key] {
			current = append(current, provider)
			known[key] = true
		}
	}
	return current
}
