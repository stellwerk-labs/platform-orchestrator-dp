package api

import (
	"testing"

	"github.com/google/uuid"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

func TestModuleArtifactRequirementsFreezeGraphVersions(t *testing.T) {
	coordinate := platform_orchestrator_graph.ResourceCoordinate{Type: "database", Class: "default", Id: "shared.db"}
	graph := &platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			coordinate: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "postgres", VersionId: "2.4.0"}},
		},
	}
	requirements, err := moduleArtifactRequirements(graph, map[string]catalogueArtifact{
		"postgres@2.4.0": {Source: "registry.example/postgres@2.4.0", Digest: "sha256:abc"},
	}, false)
	require.NoError(t, err)
	assert.Equal(t, []model.ModuleArtifactRequirement{{
		ModuleID: "postgres", Version: "2.4.0", Source: "registry.example/postgres@2.4.0", ArtifactDigest: "sha256:abc",
	}}, requirements)
}

func TestModuleVersionRequestDigestCanonicalIntent(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	assert.Empty(t, moduleVersionRequestDigest(nil, nil))
	assert.Empty(t, moduleVersionRequestDigest(map[string]string{}, []uuid.UUID{}))
	left := moduleVersionRequestDigest(map[string]string{"redis": "1.0.0", "namespace": "2.0.0"}, []uuid.UUID{first, second, first})
	right := moduleVersionRequestDigest(map[string]string{"namespace": "2.0.0", "redis": "1.0.0"}, []uuid.UUID{second, first})
	assert.Equal(t, left, right, "map order and duplicate confirmations are not distinct intent")
	assert.NotEqual(t, left, moduleVersionRequestDigest(map[string]string{"redis": "1.0.1", "namespace": "2.0.0"}, []uuid.UUID{first, second}))
	assert.NotEqual(t, left, moduleVersionRequestDigest(map[string]string{"redis": "1.0.0", "namespace": "2.0.0"}, []uuid.UUID{first}))
	assert.NotEqual(t, moduleVersionRequestDigest(nil, nil), moduleVersionRequestDigest(map[string]string{"redis": "1.0.0"}, nil), "explicit selection is not the same request as implicit resolution")
}

func TestModuleArtifactRequirementsRejectMissingMetadata(t *testing.T) {
	coordinate := platform_orchestrator_graph.ResourceCoordinate{Type: "database", Class: "default", Id: "shared.db"}
	graph := &platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			coordinate: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "postgres", VersionId: "2.4.0"}},
		},
	}
	_, err := moduleArtifactRequirements(graph, nil, false)
	require.ErrorContains(t, err, "no immutable version metadata")
}

func TestModuleArtifactRequirementsRetainAuthoritativeLegacyGeneration(t *testing.T) {
	const legacyVersion = "k9s2-opaque-history"
	body := []byte(`{"modules":[{"id":"postgres","version_id":"` + legacyVersion + `","module_source":"git::https://example.com/postgres?ref=retained","migration_generation":"v0","semantic_status":"deprecated"}]}`)
	artifacts, err := decodeCatalogueArtifacts(body, nil)
	require.NoError(t, err)
	coordinate := platform_orchestrator_graph.ResourceCoordinate{Type: "database", Class: "default", Id: "shared.db"}
	graph := &platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			coordinate: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "postgres", VersionId: legacyVersion}},
		},
	}
	for _, rollback := range []bool{false, true} {
		requirements, err := moduleArtifactRequirements(graph, artifacts, rollback)
		require.NoError(t, err)
		require.Len(t, requirements, 1)
		assert.Equal(t, legacyVersion, requirements[0].Version)
		assert.Equal(t, "v0", requirements[0].MigrationGeneration)
		assert.Empty(t, requirements[0].ArtifactDigest)
		assert.Empty(t, requirements[0].SemanticVersion)
		assert.Equal(t, rollback, requirements[0].RetainedArtifactException)
	}
}

func TestDecodeCatalogueArtifactsDoesNotInferLegacyFromMissingMetadata(t *testing.T) {
	artifacts, err := decodeCatalogueArtifacts([]byte(`{"modules":[{"id":"postgres","version_id":"opaque-history","module_source":"git::https://example.com/postgres"}]}`), nil)
	require.NoError(t, err)
	assert.Empty(t, artifacts["postgres@opaque-history"].MigrationGeneration)
}

func TestActiveModuleDefinitionsExcludeRemovedResources(t *testing.T) {
	graph := &platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			{Type: "database", Id: "active"}:  {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "postgres", VersionId: "opaque-history"}},
			{Type: "database", Id: "other"}:   {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "postgres", VersionId: "opaque-history"}},
			{Type: "database", Id: "deleted"}: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "redis", VersionId: "removed", Deleted: true}},
			{Type: "workload", Id: "outputs"}: {},
		},
	}
	assert.Equal(t, []string{"postgres@opaque-history"}, activeModuleDefinitions(graph))
}

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
