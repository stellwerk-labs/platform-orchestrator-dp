package api

import (
	"bytes"
	"maps"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

func TestDeletedModuleMaterialPreservesHistoricalProviders(t *testing.T) {
	for _, resourceType := range []string{"k8s-namespace", "database", "route"} {
		t.Run(resourceType, func(t *testing.T) {
			historical := cp.InternalExecutionCatalogueModule{
				Id: "retired", VersionId: "opaque-before-upgrade", ResourceType: resourceType,
				ModuleSourceCode: ref.Ref(`resource "terraform_data" "old" { input = "old" }`),
				Rules:            []cp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
				ProviderMapping:  map[string]string{"random": "random.historical"},
			}
			oldGraph, err := graphs.BuildGraph(t.Context(), graph.Manifest{
				SharedResources: map[string]graph.ManifestResource{"old": {Type: resourceType}},
			}, []cp.InternalExecutionCatalogueModule{historical}, nil)
			require.NoError(t, err)
			coordinate := graph.ResourceCoordinate{Type: resourceType, Class: "default", Id: "shared.old"}
			providers := []cp.ModuleProvider{{ProviderType: "random", Id: "historical", Source: "hashicorp/random", VersionConstraint: "3.7.2", Configuration: map[string]any{"seed": "${var.retained_seed}"}}}
			require.NoError(t, graphs.AddProviderMappingToNodes(oldGraph, providers, graphs.BuildGraphDistanceMatrix(oldGraph)))
			originalProviders := maps.Clone(oldGraph.Nodes[coordinate].ModuleConfiguration.ProviderFullIdToHashVariantMapping)
			raw, err := graphs.ToJson(oldGraph)
			require.NoError(t, err)
			oldGraph, err = graphs.FromJson(bytes.NewReader(raw))
			require.NoError(t, err)
			require.Nil(t, oldGraph.Nodes[coordinate].ModuleConfiguration.Definition)

			current := historical
			current.VersionId = "new-current-version"
			current.ProviderMapping = nil
			newGraph, err := graphs.BuildGraph(t.Context(), graph.Manifest{
				SharedResources: map[string]graph.ManifestResource{"new": {Type: resourceType}},
			}, []cp.InternalExecutionCatalogueModule{current}, oldGraph)
			require.NoError(t, err)
			graphs.AddDeletedNodes(newGraph, oldGraph, false)
			deleted := &graph.Graph[*graphs.GraphNodeModuleConfig]{Nodes: map[graph.ResourceCoordinate]graph.ResourceNode[*graphs.GraphNodeModuleConfig]{coordinate: newGraph.Nodes[coordinate]}}
			historical.Rules = nil
			require.NoError(t, assignDeletedModuleMaterial(deleted, []cp.InternalExecutionCatalogueModule{historical}))
			require.False(t, newGraph.Nodes[coordinate].ModuleConfiguration.HasInlineSource)
			require.True(t, newGraph.Nodes[coordinate].ModuleConfiguration.Deleted)
			require.Equal(t, "opaque-before-upgrade", newGraph.Nodes[coordinate].ModuleConfiguration.VersionId)
			require.Equal(t, "new-current-version", newGraph.Nodes[graph.ResourceCoordinate{Type: resourceType, Class: "default", Id: "shared.new"}].ModuleConfiguration.VersionId)

			matrix := graphs.BuildGraphDistanceMatrix(newGraph)
			require.NoError(t, graphs.AddProviderMappingToNodes(newGraph, providers, matrix))
			require.Equal(t, originalProviders, newGraph.Nodes[coordinate].ModuleConfiguration.ProviderFullIdToHashVariantMapping)
			var storage cp.StateStorageConfiguration
			require.NoError(t, storage.FromK8sStorageConfiguration(cp.K8sStorageConfiguration{Namespace: "test-state"}))
			tofu, err := graphs.BuildTofuFromGraph(newGraph, providers, cp.Runner{StateStorageConfiguration: storage}, uuid.Nil, nil, matrix)
			require.NoError(t, err)
			require.Contains(t, string(tofu), `source  = "hashicorp/random"`)
			require.Contains(t, string(tofu), `variable "retained_seed"`)
			require.Contains(t, string(tofu), `seed  = var.retained_seed`)
			require.Contains(t, string(tofu), graphs.LocalProviderName("random", "historical", originalProviders["random.historical"]))
			require.NotContains(t, string(tofu), `modules/retired/opaque-before-upgrade`)
			require.Contains(t, string(tofu), `modules/retired/new-current-version`)
		})
	}
}

func TestDeletedModuleMaterialMissingVersionIsConflict(t *testing.T) {
	coordinate := graph.ResourceCoordinate{Type: "database", Class: "default", Id: "shared.db"}
	g := &graph.Graph[*graphs.GraphNodeModuleConfig]{Nodes: map[graph.ResourceCoordinate]graph.ResourceNode[*graphs.GraphNodeModuleConfig]{coordinate: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "postgres", VersionId: "original", Deleted: true}}}}
	err := assignDeletedModuleMaterial(g, []cp.InternalExecutionCatalogueModule{{Id: "postgres", VersionId: "different"}})
	_, conflict := model.IsErrConflict(err)
	require.True(t, conflict)
	require.ErrorContains(t, err, "postgres@original")
	require.ErrorContains(t, err, coordinate.String())
	require.ErrorContains(t, err, "restore that exact module version")
	response, internalError := createDeploymentErrorResponse(err)
	require.NoError(t, internalError)
	require.IsType(t, CreateDeployment409JSONResponse{}, response)
}

func TestDeletedModuleMaterialDoesNotReplaceCurrentProviders(t *testing.T) {
	current := cp.ModuleProvider{ProviderType: "kubernetes", Id: "cluster", Configuration: map[string]any{"host": "initial"}}
	changed := current
	changed.Configuration = map[string]any{"host": "changed-between-catalogue-reads"}
	historical := cp.ModuleProvider{ProviderType: "aws", Id: "retired"}
	require.Equal(t, []cp.ModuleProvider{current, historical}, mergeModuleProviders([]cp.ModuleProvider{current}, []cp.ModuleProvider{changed, historical, historical}))
}
