package graphs

import (
	"reflect"
	"testing"

	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoesModuleParamTypeMatch(t *testing.T) {
	type expectedMatch struct {
		isNumber bool
		isBool   bool
		isString bool
		isList   bool
		isMap    bool
		isAny    bool
	}
	for _, tt := range []struct {
		raw      interface{}
		expected expectedMatch
	}{
		{1.0, expectedMatch{true, false, false, false, false, true}},
		{int64(1), expectedMatch{true, false, false, false, false, true}},
		{true, expectedMatch{false, true, false, false, false, true}},
		{"1", expectedMatch{false, false, true, false, false, true}},
		{[]interface{}{"1"}, expectedMatch{false, false, false, true, false, true}},
		{map[string]interface{}{"a": "1"}, expectedMatch{false, false, false, false, true, true}},
	} {
		t.Run(reflect.ValueOf(tt.raw).Type().String(), func(t *testing.T) {
			assert.Equal(t, tt.expected, expectedMatch{
				isNumber: doesModuleParamTypeMatch(genclient.Number, tt.raw),
				isBool:   doesModuleParamTypeMatch(genclient.Bool, tt.raw),
				isString: doesModuleParamTypeMatch(genclient.String, tt.raw),
				isList:   doesModuleParamTypeMatch(genclient.List, tt.raw),
				isMap:    doesModuleParamTypeMatch(genclient.Map, tt.raw),
				isAny:    doesModuleParamTypeMatch(genclient.Any, tt.raw),
			})
		})
	}
}

func TestValidateModuleParamInner_param_not_defined(t *testing.T) {
	assert.EqualError(t, validateModuleParamInner(
		t.Context(),
		platform_orchestrator_graph.ResourceCoordinate{},
		&platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			Params: map[string]interface{}{"a": "1"},
			ModuleConfiguration: &GraphNodeModuleConfig{
				DefinitionId: "my-def", VersionId: "v1",
				Definition: &genclient.InternalModuleCatalogueModule{ModuleParams: map[string]genclient.ModuleParamItem{}},
			},
		},
	), "node 'type=,class=,id=': uses module my-def@v1 which does not define module_param 'a'")
}

func TestValidateModuleParamInner_expect_required_param(t *testing.T) {
	assert.EqualError(t, validateModuleParamInner(
		t.Context(),
		platform_orchestrator_graph.ResourceCoordinate{},
		&platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			Params: map[string]interface{}{"a": "1"},
			ModuleConfiguration: &GraphNodeModuleConfig{
				DefinitionId: "my-def", VersionId: "v1",
				Definition: &genclient.InternalModuleCatalogueModule{ModuleParams: map[string]genclient.ModuleParamItem{
					"key": {Type: genclient.String},
				}},
			},
		},
	), "node 'type=,class=,id=': uses module my-def@v1 which requires string param 'key' to be set")
}

func TestValidateModuleParamInner_incorrect_type(t *testing.T) {
	assert.EqualError(t, validateModuleParamInner(
		t.Context(),
		platform_orchestrator_graph.ResourceCoordinate{},
		&platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			Params: map[string]interface{}{"key": 42},
			ModuleConfiguration: &GraphNodeModuleConfig{
				DefinitionId: "my-def", VersionId: "v1",
				Definition: &genclient.InternalModuleCatalogueModule{ModuleParams: map[string]genclient.ModuleParamItem{
					"key": {Type: genclient.String},
				}},
			},
		},
	), "node 'type=,class=,id=': uses module my-def@v1 which expects param 'key' to be of type 'string' but got 'int'")
}

func TestValidateModuleParamInner_undefined_param(t *testing.T) {
	assert.EqualError(t, validateModuleParamInner(
		t.Context(),
		platform_orchestrator_graph.ResourceCoordinate{},
		&platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			Params: map[string]interface{}{"a": "1"},
			ModuleConfiguration: &GraphNodeModuleConfig{
				DefinitionId: "my-def", VersionId: "v1",
				Definition: &genclient.InternalModuleCatalogueModule{ModuleParams: map[string]genclient.ModuleParamItem{
					"key": {Type: genclient.String, IsOptional: true},
				}},
			},
		},
	), "node 'type=,class=,id=': uses module my-def@v1 which does not define module_param 'a'")
}

func TestAddProviderMappingToNodes_success(t *testing.T) {
	// Create a simple graph with one node that uses a provider
	g := &platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			{Type: "database", Class: "default", Id: "mydb"}: {
				ModuleConfiguration: &GraphNodeModuleConfig{
					DefinitionId: "postgres-module",
					VersionId:    "v1",
					Definition: &genclient.InternalModuleCatalogueModule{
						ProviderMapping: map[string]string{
							"postgres": "postgresql.default",
						},
					},
				},
			},
		},
		Edges: map[platform_orchestrator_graph.ResourceCoordinate]map[string]platform_orchestrator_graph.ResourceCoordinate{},
	}

	// Create a provider
	providers := []genclient.ModuleProvider{
		{
			ProviderType:      "postgresql",
			Id:                "default",
			Source:            "cyrilgdn/postgresql",
			VersionConstraint: "~> 1.0",
			Configuration: map[string]interface{}{
				"host": "localhost",
			},
		},
	}

	matrix := BuildGraphDistanceMatrix(g)
	err := AddProviderMappingToNodes(g, providers, matrix)
	require.NoError(t, err)

	// Verify the provider mapping was added
	node := g.Nodes[platform_orchestrator_graph.ResourceCoordinate{Type: "database", Class: "default", Id: "mydb"}]
	assert.Contains(t, node.ModuleConfiguration.ProviderFullIdToHashVariantMapping, "postgresql.default")

	hash := node.ModuleConfiguration.ProviderFullIdToHashVariantMapping["postgresql.default"]
	assert.Len(t, hash, 64) // SHA256 hash is 64 hex characters

	// Verify ProviderSubsMap was populated (keyed by localRef, not full identifier)
	assert.Contains(t, node.ModuleConfiguration.ProviderSubsMap, "postgres", "ProviderSubsMap should use localRef as key")
	subs := node.ModuleConfiguration.ProviderSubsMap["postgres"]
	assert.NotNil(t, subs, "Substitutions should be set even if empty")
}

func TestAddProviderMappingToNodes_same_config_same_hash(t *testing.T) {
	// Create a graph with two nodes using the same provider with the same configuration
	g := &platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			{Type: "database", Class: "default", Id: "db1"}: {
				ModuleConfiguration: &GraphNodeModuleConfig{
					DefinitionId: "postgres-module",
					VersionId:    "v1",
					Definition: &genclient.InternalModuleCatalogueModule{
						ProviderMapping: map[string]string{
							"postgres": "postgresql.default",
						},
					},
				},
			},
			{Type: "database", Class: "default", Id: "db2"}: {
				ModuleConfiguration: &GraphNodeModuleConfig{
					DefinitionId: "postgres-module",
					VersionId:    "v1",
					Definition: &genclient.InternalModuleCatalogueModule{
						ProviderMapping: map[string]string{
							"postgres": "postgresql.default",
						},
					},
				},
			},
		},
		Edges: map[platform_orchestrator_graph.ResourceCoordinate]map[string]platform_orchestrator_graph.ResourceCoordinate{},
	}

	providers := []genclient.ModuleProvider{
		{
			ProviderType:      "postgresql",
			Id:                "default",
			Source:            "cyrilgdn/postgresql",
			VersionConstraint: "~> 1.0",
			Configuration: map[string]interface{}{
				"host": "localhost",
			},
		},
	}

	matrix := BuildGraphDistanceMatrix(g)
	err := AddProviderMappingToNodes(g, providers, matrix)
	require.NoError(t, err)

	// Both nodes should have the same hash since they use the same provider configuration
	node1 := g.Nodes[platform_orchestrator_graph.ResourceCoordinate{Type: "database", Class: "default", Id: "db1"}]
	node2 := g.Nodes[platform_orchestrator_graph.ResourceCoordinate{Type: "database", Class: "default", Id: "db2"}]

	hash1 := node1.ModuleConfiguration.ProviderFullIdToHashVariantMapping["postgresql.default"]
	hash2 := node2.ModuleConfiguration.ProviderFullIdToHashVariantMapping["postgresql.default"]

	assert.Equal(t, hash1, hash2, "Same provider configuration should result in same hash")

	// Verify ProviderSubsMap is set for both nodes
	assert.Contains(t, node1.ModuleConfiguration.ProviderSubsMap, "postgres")
	assert.Contains(t, node2.ModuleConfiguration.ProviderSubsMap, "postgres")
}

func TestAddProviderMappingToNodes_different_configs_different_hash(t *testing.T) {
	// Create a graph with two nodes using different provider configurations
	g := &platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			{Type: "database", Class: "default", Id: "db1"}: {
				ModuleConfiguration: &GraphNodeModuleConfig{
					DefinitionId: "postgres-module",
					VersionId:    "v1",
					Definition: &genclient.InternalModuleCatalogueModule{
						ProviderMapping: map[string]string{
							"postgres": "postgresql.prod",
						},
					},
				},
			},
			{Type: "database", Class: "default", Id: "db2"}: {
				ModuleConfiguration: &GraphNodeModuleConfig{
					DefinitionId: "postgres-module",
					VersionId:    "v1",
					Definition: &genclient.InternalModuleCatalogueModule{
						ProviderMapping: map[string]string{
							"postgres": "postgresql.dev",
						},
					},
				},
			},
		},
		Edges: map[platform_orchestrator_graph.ResourceCoordinate]map[string]platform_orchestrator_graph.ResourceCoordinate{},
	}

	providers := []genclient.ModuleProvider{
		{
			ProviderType:      "postgresql",
			Id:                "prod",
			Source:            "cyrilgdn/postgresql",
			VersionConstraint: "~> 1.0",
			Configuration: map[string]interface{}{
				"host": "prod.example.com",
			},
		},
		{
			ProviderType:      "postgresql",
			Id:                "dev",
			Source:            "cyrilgdn/postgresql",
			VersionConstraint: "~> 1.0",
			Configuration: map[string]interface{}{
				"host": "dev.example.com",
			},
		},
	}

	matrix := BuildGraphDistanceMatrix(g)
	err := AddProviderMappingToNodes(g, providers, matrix)
	require.NoError(t, err)

	// Both nodes should have different hashes since they use different provider configurations
	node1 := g.Nodes[platform_orchestrator_graph.ResourceCoordinate{Type: "database", Class: "default", Id: "db1"}]
	node2 := g.Nodes[platform_orchestrator_graph.ResourceCoordinate{Type: "database", Class: "default", Id: "db2"}]

	hash1 := node1.ModuleConfiguration.ProviderFullIdToHashVariantMapping["postgresql.prod"]
	hash2 := node2.ModuleConfiguration.ProviderFullIdToHashVariantMapping["postgresql.dev"]

	assert.NotEqual(t, hash1, hash2, "Different provider configurations should result in different hashes")

	// Verify ProviderSubsMap is set for both nodes (different configurations, different substitutions)
	assert.Contains(t, node1.ModuleConfiguration.ProviderSubsMap, "postgres")
	assert.Contains(t, node2.ModuleConfiguration.ProviderSubsMap, "postgres")
	// The substitutions should be different since the configurations are different
	subs1 := node1.ModuleConfiguration.ProviderSubsMap["postgres"]
	subs2 := node2.ModuleConfiguration.ProviderSubsMap["postgres"]
	assert.NotNil(t, subs1)
	assert.NotNil(t, subs2)
}

func TestAddProviderMappingToNodes_unknown_provider_error(t *testing.T) {
	// Create a graph with a node that references a non-existent provider
	g := &platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			{Type: "database", Class: "default", Id: "mydb"}: {
				ModuleConfiguration: &GraphNodeModuleConfig{
					DefinitionId: "postgres-module",
					VersionId:    "v1",
					Definition: &genclient.InternalModuleCatalogueModule{
						ProviderMapping: map[string]string{
							"postgres": "postgresql.nonexistent",
						},
					},
				},
			},
		},
		Edges: map[platform_orchestrator_graph.ResourceCoordinate]map[string]platform_orchestrator_graph.ResourceCoordinate{},
	}

	// No providers provided
	providers := []genclient.ModuleProvider{}

	matrix := BuildGraphDistanceMatrix(g)
	err := AddProviderMappingToNodes(g, providers, matrix)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown provider 'postgresql.nonexistent'")
}

func TestAddProviderMappingToNodes_multiple_providers_same_node(t *testing.T) {
	// Create a graph with a node that uses multiple providers
	g := &platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			{Type: "app", Class: "default", Id: "myapp"}: {
				ModuleConfiguration: &GraphNodeModuleConfig{
					DefinitionId: "app-module",
					VersionId:    "v1",
					Definition: &genclient.InternalModuleCatalogueModule{
						ProviderMapping: map[string]string{
							"aws":   "aws.default",
							"vault": "vault.default",
						},
					},
				},
			},
		},
		Edges: map[platform_orchestrator_graph.ResourceCoordinate]map[string]platform_orchestrator_graph.ResourceCoordinate{},
	}

	providers := []genclient.ModuleProvider{
		{
			ProviderType:      "aws",
			Id:                "default",
			Source:            "hashicorp/aws",
			VersionConstraint: "~> 5.0",
			Configuration: map[string]interface{}{
				"region": "us-east-1",
			},
		},
		{
			ProviderType:      "vault",
			Id:                "default",
			Source:            "hashicorp/vault",
			VersionConstraint: "~> 3.0",
			Configuration: map[string]interface{}{
				"address": "https://vault.example.com",
			},
		},
	}

	matrix := BuildGraphDistanceMatrix(g)
	err := AddProviderMappingToNodes(g, providers, matrix)
	require.NoError(t, err)

	// Verify both providers were mapped
	node := g.Nodes[platform_orchestrator_graph.ResourceCoordinate{Type: "app", Class: "default", Id: "myapp"}]
	assert.Len(t, node.ModuleConfiguration.ProviderFullIdToHashVariantMapping, 2)
	assert.Contains(t, node.ModuleConfiguration.ProviderFullIdToHashVariantMapping, "aws.default")
	assert.Contains(t, node.ModuleConfiguration.ProviderFullIdToHashVariantMapping, "vault.default")

	// Verify ProviderSubsMap has both localRefs
	assert.Len(t, node.ModuleConfiguration.ProviderSubsMap, 2)
	assert.Contains(t, node.ModuleConfiguration.ProviderSubsMap, "aws", "ProviderSubsMap should use localRef 'aws' as key")
	assert.Contains(t, node.ModuleConfiguration.ProviderSubsMap, "vault", "ProviderSubsMap should use localRef 'vault' as key")
}

func TestAddProviderMappingToNodes_empty_provider_mapping(t *testing.T) {
	// Create a graph with a node that has no provider mapping
	g := &platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
			{Type: "database", Class: "default", Id: "mydb"}: {
				ModuleConfiguration: &GraphNodeModuleConfig{
					DefinitionId: "simple-module",
					VersionId:    "v1",
					Definition: &genclient.InternalModuleCatalogueModule{
						ProviderMapping: map[string]string{},
					},
				},
			},
		},
		Edges: map[platform_orchestrator_graph.ResourceCoordinate]map[string]platform_orchestrator_graph.ResourceCoordinate{},
	}

	providers := []genclient.ModuleProvider{}

	matrix := BuildGraphDistanceMatrix(g)
	err := AddProviderMappingToNodes(g, providers, matrix)
	require.NoError(t, err)

	// Node should not have any provider mappings
	node := g.Nodes[platform_orchestrator_graph.ResourceCoordinate{Type: "database", Class: "default", Id: "mydb"}]
	// The node's ModuleConfiguration should remain unchanged (no ProviderLocalRefToHashVariantMapping field set)
	assert.Nil(t, node.ModuleConfiguration.ProviderFullIdToHashVariantMapping)
}
