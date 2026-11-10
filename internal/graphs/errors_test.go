package graphs

import (
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/require"
)

func TestMissingModuleDefinitionIsBadRequest(t *testing.T) {
	coordinate := graph.ResourceCoordinate{Type: "database", Class: "default", Id: "shared.db"}
	for _, configuration := range []*GraphNodeModuleConfig{nil, {DefinitionId: "postgres", VersionId: "original", Deleted: true}} {
		g := &graph.Graph[*GraphNodeModuleConfig]{Nodes: map[graph.ResourceCoordinate]graph.ResourceNode[*GraphNodeModuleConfig]{coordinate: {ModuleConfiguration: configuration}}}
		matrix := g.BuildAdjacencyMatrix().FillDistanceMatrix()
		mappingError := AddProviderMappingToNodes(g, nil, matrix)
		var storage cp.StateStorageConfiguration
		require.NoError(t, storage.FromK8sStorageConfiguration(cp.K8sStorageConfiguration{Namespace: "test-state"}))
		_, tofuError := BuildTofuFromGraph(g, nil, cp.Runner{StateStorageConfiguration: storage}, uuid.Nil, nil, matrix)
		for _, err := range []error{mappingError, tofuError} {
			var badRequest UserBadRequestError
			require.ErrorAs(t, err, &badRequest)
			require.ErrorContains(t, err, coordinate.String())
			if configuration != nil {
				require.ErrorContains(t, err, "postgres@original")
			}
		}
	}
}
