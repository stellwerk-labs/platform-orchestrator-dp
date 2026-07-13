package graphs

import (
	"testing"

	"github.com/google/uuid"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/assert"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

var (
	rcPostgres = platform_orchestrator_graph.ResourceCoordinate{Type: "postgres", Class: "default", Id: "shared.pg"}
	rcNs       = platform_orchestrator_graph.ResourceCoordinate{Type: "k8s-namespace", Class: "default", Id: "shared.ns"}
	rcWorkload = platform_orchestrator_graph.ResourceCoordinate{Type: "workload", Class: "default", Id: "workloads.app"}
)

func makeNode(defId, versionId string) platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig] {
	return platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
		ModuleConfiguration: &GraphNodeModuleConfig{DefinitionId: defId, VersionId: versionId},
	}
}

func TestResolveNodes_empty_graph(t *testing.T) {
	result := ResolveNodes(&platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{}, uuid.New())
	assert.Equal(t, []ResolvedNode{}, result)
}

func TestResolveNodes_single_node(t *testing.T) {
	envUuid := uuid.New()

	result := ResolveNodes(&platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			rcPostgres: makeNode("pg-def", "v1"),
		},
	}, envUuid)

	assert.Equal(t, []ResolvedNode{{
		Hash:          util.GenerateNodeHash(envUuid, rcPostgres.Type, rcPostgres.Class, rcPostgres.Id),
		ResourceType:  rcPostgres.Type,
		ResourceClass: rcPostgres.Class,
		ResourceId:    rcPostgres.Id,
		DefinitionId:  "pg-def",
		VersionId:     "v1",
		Edges:         map[string]string{},
	}}, result)
}

func TestResolveNodes_nominal(t *testing.T) {
	envUuid := uuid.New()

	result := ResolveNodes(&platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			rcPostgres: makeNode("pg-def", "v1"),
			rcNs:       makeNode("ns-def", "v2"),
		},
		Edges: map[platform_orchestrator_graph.ResourceCoordinate]map[string]platform_orchestrator_graph.ResourceCoordinate{
			rcPostgres: {"ns": rcNs},
		},
	}, envUuid)

	nsHash := util.GenerateNodeHash(envUuid, rcNs.Type, rcNs.Class, rcNs.Id)
	pgHash := util.GenerateNodeHash(envUuid, rcPostgres.Type, rcPostgres.Class, rcPostgres.Id)

	assert.ElementsMatch(t, []ResolvedNode{
		{
			Hash: pgHash, ResourceType: rcPostgres.Type, ResourceClass: rcPostgres.Class, ResourceId: rcPostgres.Id,
			DefinitionId: "pg-def", VersionId: "v1",
			Edges: map[string]string{"ns": nsHash},
		},
		{
			Hash: nsHash, ResourceType: rcNs.Type, ResourceClass: rcNs.Class, ResourceId: rcNs.Id,
			DefinitionId: "ns-def", VersionId: "v2",
			Edges: map[string]string{},
		},
	}, result)
}

func TestResolveNodes_workload_nodes_included(t *testing.T) {
	envUuid := uuid.New()
	result := ResolveNodes(&platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			rcWorkload: {ModuleConfiguration: nil},
			rcPostgres: makeNode("pg-def", "v1"),
		},
	}, envUuid)

	assert.Len(t, result, 2)
	for _, n := range result {
		if n.ResourceType == rcWorkload.Type {
			assert.Equal(t, util.GenerateNodeHash(envUuid, rcWorkload.Type, rcWorkload.Class, rcWorkload.Id), n.Hash)
			assert.Empty(t, n.DefinitionId)
			assert.Empty(t, n.VersionId)
		}
	}
}

func TestResolveNodes_deleted_nodes_excluded(t *testing.T) {
	result := ResolveNodes(&platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			rcPostgres: {ModuleConfiguration: &GraphNodeModuleConfig{Deleted: true}},
			rcNs:       makeNode("ns-def", "v2"),
		},
	}, uuid.New())

	assert.Len(t, result, 1)
	assert.Equal(t, rcNs.Type, result[0].ResourceType)
}

func TestResolveNodes_mixed_node_types(t *testing.T) {
	result := ResolveNodes(&platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			rcWorkload: {ModuleConfiguration: nil},
			rcPostgres: {ModuleConfiguration: &GraphNodeModuleConfig{Deleted: true}},
			rcNs:       makeNode("ns-def", "v2"),
			{Type: "redis", Class: "default", Id: "shared.cache"}: makeNode("redis-def", "v1"),
		},
	}, uuid.New())

	assert.Len(t, result, 3) // workload + ns + redis; deleted postgres excluded
}

func TestResolveNodes_node_with_no_edges(t *testing.T) {
	result := ResolveNodes(&platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			rcNs: makeNode("ns-def", "v2"),
		},
	}, uuid.New())

	assert.Len(t, result, 1)
	assert.Equal(t, map[string]string{}, result[0].Edges)
}
