package graphs

import (
	"github.com/google/uuid"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

// ResolvedNode is a graph node with pre-computed hashes for itself and its edges.
// DefinitionId and VersionId are empty for workload nodes (ModuleConfiguration == nil).
type ResolvedNode struct {
	Hash          string
	ResourceType  string
	ResourceClass string
	ResourceId    string
	DefinitionId  string
	VersionId     string
	Edges         map[string]string // alias → target node hash
}

// ResolveNodes returns all non-deleted nodes (including workload nodes), computes
// deterministic hashes, and resolves edge references to hashes.
func ResolveNodes(graph *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig], envUuid uuid.UUID) []ResolvedNode {
	hashes := make(map[platform_orchestrator_graph.ResourceCoordinate]string, len(graph.Nodes))
	for coord := range graph.Nodes {
		hashes[coord] = util.GenerateNodeHash(envUuid, coord.Type, coord.Class, coord.Id)
	}

	nodes := make([]ResolvedNode, 0, len(graph.Nodes))
	for rc, node := range graph.Nodes {
		if node.ModuleConfiguration != nil && node.ModuleConfiguration.Deleted {
			continue
		}

		edges := map[string]string{}
		for alias, target := range graph.Edges[rc] {
			edges[alias] = hashes[target]
		}

		n := ResolvedNode{
			Hash:          hashes[rc],
			ResourceType:  rc.Type,
			ResourceClass: rc.Class,
			ResourceId:    rc.Id,
			Edges:         edges,
		}
		if node.ModuleConfiguration != nil {
			n.DefinitionId = node.ModuleConfiguration.DefinitionId
			n.VersionId = node.ModuleConfiguration.VersionId
		}
		nodes = append(nodes, n)
	}

	return nodes
}
