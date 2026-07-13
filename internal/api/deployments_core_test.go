package api

import (
	"testing"

	"github.com/google/uuid"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/assert"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

func TestDiffGraphs_empty(t *testing.T) {
	assert.Empty(t, DiffGraphs(
		uuid.New(),
		&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{},
		&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{},
	).Changes)
}

func TestDiffGraphs_paramsChangedWithoutParamsDefinedBy(t *testing.T) {
	envUuid := uuid.New()

	rcA := platform_orchestrator_graph.ResourceCoordinate{Type: "x", Class: "default", Id: "shared.a"}

	diff := DiffGraphs(
		envUuid,
		&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
			Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				rcA: {
					Params:              map[string]interface{}{"x": "a"},
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"},
				},
			},
		},
		&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
			Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				rcA: {
					Params:              map[string]interface{}{"x": "b"},
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"},
				},
			},
		},
	)
	assert.Equal(t, []DeploymentDiffChange{
		{Id: util.GenerateNodeHash(envUuid, "x", "default", "shared.a"), Resource: "x.default@shared.a", Summary: "resource params changed", Type: "params_changed"},
	}, diff.Changes)
}

func TestDiffGraphs_nominal(t *testing.T) {
	envUuid := uuid.New()

	rcWa := platform_orchestrator_graph.ResourceCoordinate{Type: "workload", Class: "default", Id: "workloads.a"}
	rcWb := platform_orchestrator_graph.ResourceCoordinate{Type: "workload", Class: "default", Id: "workloads.b"}
	rcWc := platform_orchestrator_graph.ResourceCoordinate{Type: "workload", Class: "default", Id: "workloads.c"}
	rcA := platform_orchestrator_graph.ResourceCoordinate{Type: "x", Class: "default", Id: "shared.a"}
	rcB := platform_orchestrator_graph.ResourceCoordinate{Type: "x", Class: "default", Id: "shared.b"}
	rcC := platform_orchestrator_graph.ResourceCoordinate{Type: "x", Class: "default", Id: "shared.c"}
	rcD := platform_orchestrator_graph.ResourceCoordinate{Type: "x", Class: "default", Id: "shared.d"}

	diff := DiffGraphs(
		envUuid,
		&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
			Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				rcWa: {},
				rcWb: {},
				rcA: {
					ParamsDefinedBy:     &rcWa,
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"},
				},
				rcB: {
					ParamsDefinedBy:     &rcWb,
					Params:              map[string]interface{}{"x": "a"},
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"},
				},
				rcC: {
					ParamsDefinedBy:     &rcWb,
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"},
				},
			},
		},
		&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
			Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				rcWb: {},
				rcB: {
					ParamsDefinedBy:     &rcWb,
					Params:              map[string]interface{}{"x": "b"},
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"},
				},
				rcC: {
					ParamsDefinedBy:     &rcWb,
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "2"},
				},
				rcWc: {},
				rcD: {
					ParamsDefinedBy:     &rcWc,
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"},
				},
			},
		},
	)
	assert.Equal(t, DeploymentDiff{
		Changes: []DeploymentDiffChange{
			{Id: util.GenerateNodeHash(envUuid, "workload", "default", "workloads.c"), Resource: "workload.default@workloads.c", Summary: "workload added", Type: "added"},
			{Id: util.GenerateNodeHash(envUuid, "x", "default", "shared.b"), Resource: "x.default@shared.b", Summary: "resource params changed by workload.default@workloads.b", Type: "params_changed"},
			{Id: util.GenerateNodeHash(envUuid, "x", "default", "shared.c"), Resource: "x.default@shared.c", Summary: "module changed from d@1 to d@2", Type: "module_changed"},
			{Id: util.GenerateNodeHash(envUuid, "x", "default", "shared.d"), Resource: "x.default@shared.d", Summary: "add resource using module d@1 (dependency of workload.default@workloads.c)", Type: "added"},
			{Id: util.GenerateNodeHash(envUuid, "workload", "default", "workloads.a"), Resource: "workload.default@workloads.a", Summary: "workload removed", Type: "removed"},
			{Id: util.GenerateNodeHash(envUuid, "x", "default", "shared.a"), Resource: "x.default@shared.a", Summary: "remove resource using module d@1 (dependency of workload.default@workloads.a)", Type: "removed"},
		},
		NumAdded: 2, NumRemoved: 2, NumChanged: 2,
	}, diff)
}
