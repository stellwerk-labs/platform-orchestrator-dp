package graphs

import (
	"testing"

	"github.com/google/uuid"
	"github.com/hashicorp/hcl/v2/hclwrite"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

func Test_writeStateStorageBlock(t *testing.T) {
	f := hclwrite.NewEmptyFile()
	b := f.Body().AppendNewBlock("terraform", []string{})
	var ssc platformorchestratorcp.StateStorageConfiguration
	require.NoError(t, ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "ns"}))
	r := platformorchestratorcp.Runner{
		StateStorageConfiguration: ssc,
	}
	require.NoError(t, writeStateStorageBlock(r, b, uuid.Nil))
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "00000000-0000-0000-0000-000000000000"
    namespace         = "ns"
    in_cluster_config = true
  }
}
`, string(f.Bytes()))
}

func Test_writeStateStorageBlock_gcs(t *testing.T) {
	f := hclwrite.NewEmptyFile()
	b := f.Body().AppendNewBlock("terraform", []string{})
	var ssc platformorchestratorcp.StateStorageConfiguration
	require.NoError(t, ssc.FromGCSStorageConfiguration(platformorchestratorcp.GCSStorageConfiguration{Bucket: "my-bucket"}))
	r := platformorchestratorcp.Runner{
		StateStorageConfiguration: ssc,
	}
	require.NoError(t, writeStateStorageBlock(r, b, uuid.Nil))
	assert.Equal(t, `terraform {
  backend "gcs" {
    bucket = "my-bucket"
    prefix = "00000000-0000-0000-0000-000000000000"
  }
}
`, string(f.Bytes()))
}

func Test_writeStateStorageBlock_gcs_with_path_prefix(t *testing.T) {
	f := hclwrite.NewEmptyFile()
	b := f.Body().AppendNewBlock("terraform", []string{})
	var ssc platformorchestratorcp.StateStorageConfiguration
	require.NoError(t, ssc.FromGCSStorageConfiguration(platformorchestratorcp.GCSStorageConfiguration{Bucket: "my-bucket", PathPrefix: ref.Ref("some/prefix")}))
	r := platformorchestratorcp.Runner{
		StateStorageConfiguration: ssc,
	}
	require.NoError(t, writeStateStorageBlock(r, b, uuid.Nil))
	assert.Equal(t, `terraform {
  backend "gcs" {
    bucket = "my-bucket"
    prefix = "some/prefix/00000000-0000-0000-0000-000000000000"
  }
}
`, string(f.Bytes()))
}

func Test_writeStateStorageBlock_azurerm(t *testing.T) {
	f := hclwrite.NewEmptyFile()
	b := f.Body().AppendNewBlock("terraform", []string{})
	var ssc platformorchestratorcp.StateStorageConfiguration
	require.NoError(t, ssc.FromAzureRMStorageConfiguration(platformorchestratorcp.AzureRMStorageConfiguration{
		ResourceGroupName:  ref.Ref("rg"),
		StorageAccountName: "sa",
		ContainerName:      "cont",
		PathPrefix:         ref.Ref("some/prefix"),
	}))
	r := platformorchestratorcp.Runner{
		StateStorageConfiguration: ssc,
	}
	require.NoError(t, writeStateStorageBlock(r, b, uuid.Nil))
	assert.Equal(t, `terraform {
  backend "azurerm" {
    resource_group_name  = "rg"
    storage_account_name = "sa"
    container_name       = "cont"
    key                  = "some/prefix/00000000-0000-0000-0000-000000000000/terraform.tfstate"
  }
}
`, string(f.Bytes()))
}

func Test_writeStateStorageBlock_azurerm_no_resource_group(t *testing.T) {
	f := hclwrite.NewEmptyFile()
	b := f.Body().AppendNewBlock("terraform", []string{})
	var ssc platformorchestratorcp.StateStorageConfiguration
	require.NoError(t, ssc.FromAzureRMStorageConfiguration(platformorchestratorcp.AzureRMStorageConfiguration{
		StorageAccountName: "sa",
		ContainerName:      "cont",
		PathPrefix:         ref.Ref("some/prefix"),
	}))
	r := platformorchestratorcp.Runner{
		StateStorageConfiguration: ssc,
	}
	require.NoError(t, writeStateStorageBlock(r, b, uuid.Nil))
	assert.Equal(t, `terraform {
  backend "azurerm" {
    storage_account_name = "sa"
    container_name       = "cont"
    key                  = "some/prefix/00000000-0000-0000-0000-000000000000/terraform.tfstate"
  }
}
`, string(f.Bytes()))
}

func TestBuildTofuFromGraph_with_selectors(t *testing.T) {
	g, err := platform_orchestrator_graph.SeedAndExpandAll[*GraphNodeModuleConfig](
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Resources: map[string]platform_orchestrator_graph.ManifestResource{
						"thing1": {Type: "thing"},
						"thing2": {Type: "thing"},
					},
				},
			},
		},
		*platform_orchestrator_graph.NewModuleDefinitionIndex[*GraphNodeModuleConfig]([]platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig]{
			{ResourceType: "thing", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
				DefinitionId: "my-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
					Id: "my-def", VersionId: "v1", ModuleSource: "some/source",
					Dependencies: map[string]platformorchestratorcp.ModuleDependencyManifest{
						"c": {Type: "child"},
					},
					ModuleInputs: map[string]interface{}{
						"x": "${select.dependencies('child').outputs.a}",
					},
				},
			}},
			{ResourceType: "child", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
				DefinitionId: "my-child-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
					Id: "my-child-def", VersionId: "v1", ModuleSource: "some/source",
				},
			}},
		}),
		nil,
	)
	require.NoError(t, err)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "ns"})

	// Build distance matrix
	matrix := BuildGraphDistanceMatrix(&g)

	out, err := BuildTofuFromGraph(&g, nil, platformorchestratorcp.Runner{StateStorageConfiguration: ssc}, uuid.Nil, nil, matrix)
	require.NoError(t, err)
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "00000000-0000-0000-0000-000000000000"
    namespace         = "ns"
    in_cluster_config = true
  }
  required_providers {
  }
}

module "child_default_workloadsworkthing1_a3431705" {
  source = "some/source"
}

module "thing_default_workloadsworkthing1_3ac0c440" {
  source = "some/source"

  x = [module.child_default_workloadsworkthing1_a3431705.a]

  depends_on = [module.child_default_workloadsworkthing1_a3431705]
}

module "child_default_workloadsworkthing2_bb5a41e3" {
  source = "some/source"
}

module "thing_default_workloadsworkthing2_9ef68a3f" {
  source = "some/source"

  x = [module.child_default_workloadsworkthing2_bb5a41e3.a]

  depends_on = [module.child_default_workloadsworkthing2_bb5a41e3]
}

output "platform_orchestrator_metadata" {
  value = {
    "759f8726a215a812ebaed113331079d4d2d95e6c906c9f96ac4d7726870db831" = lookup(module.child_default_workloadsworkthing1_a3431705, "platform_orchestrator_metadata", {})
    "c0c7d5e8ead7e05de8e38d5e7a65a9842fec416cb9a8f997991d91b1216c9e5b" = lookup(module.thing_default_workloadsworkthing1_3ac0c440, "platform_orchestrator_metadata", {})
    "5e035ba4f00de4926e7f5e5c94ef2122802051ac524b9a3f0bc35b872f2269a4" = lookup(module.child_default_workloadsworkthing2_bb5a41e3, "platform_orchestrator_metadata", {})
    "0de78b7aa8c9377b621691d279e058d18a72a45ee5d05243d03dd7744be0667a" = lookup(module.thing_default_workloadsworkthing2_9ef68a3f, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "work" {
  value       = {}
  description = "The output variables for workload 'work'"
  sensitive   = true
}
`, string(out))
}

func TestBuildTofuFromGraph_prevent_selectors_in_vars(t *testing.T) {
	g, err := platform_orchestrator_graph.SeedAndExpandAll[*GraphNodeModuleConfig](
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Outputs: map[string]string{
						"x": "${select.dependencies('child').outputs.a}",
					},
				},
			},
		},
		*platform_orchestrator_graph.NewModuleDefinitionIndex[*GraphNodeModuleConfig]([]platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig]{}),
		nil,
	)
	require.NoError(t, err)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "ns"})

	// Build distance matrix
	matrix := g.BuildAdjacencyMatrix()
	fillInResourceParamDependencies(&g, matrix)
	matrix = matrix.FillDistanceMatrix()

	_, err = BuildTofuFromGraph(&g, nil, platformorchestratorcp.Runner{StateStorageConfiguration: ssc}, uuid.Nil, nil, matrix)
	require.EqualError(t, err, "failed to create HCL tokens for variable 'x': 'select' placeholders are not supported here")
}

func TestBuildTofuFromGraph_with_dynamic_providers(t *testing.T) {
	g, err := platform_orchestrator_graph.SeedAndExpandAll(
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Resources: map[string]platform_orchestrator_graph.ManifestResource{
						"thing1": {Type: "thing"},
						"thing2": {Type: "thing"},
					},
				},
			},
		},
		*platform_orchestrator_graph.NewModuleDefinitionIndex([]platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig]{
			{ResourceType: "thing", Rules: []platform_orchestrator_graph.Rule{{}},
				Configuration: &GraphNodeModuleConfig{
					DefinitionId: "my-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
						Id: "my-def", VersionId: "v1", ModuleSource: "some/source",
						ProviderMapping: map[string]string{
							"example": "example.dynamic",
						},
						Dependencies: map[string]platformorchestratorcp.ModuleDependencyManifest{
							"c": {Type: "child"},
						},
					},
				}},
			{ResourceType: "child", Rules: []platform_orchestrator_graph.Rule{{}},
				Configuration: &GraphNodeModuleConfig{
					DefinitionId: "my-child-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
						Id: "my-child-def", VersionId: "v1", ModuleSource: "some/source",
						ProviderMapping: map[string]string{
							"example": "example.static",
						},
					},
				}},
		}),
		nil,
	)
	require.NoError(t, err)
	providers := []platformorchestratorcp.ModuleProvider{
		{ProviderType: "example", Id: "static", Configuration: map[string]interface{}{"foo": "bar"}, Source: "a/b", VersionConstraint: "c"},
		{ProviderType: "example", Id: "dynamic", Configuration: map[string]interface{}{"foo": "${resources.c.outputs.thing}"}, Source: "a/b", VersionConstraint: "c"},
	}
	matrix := BuildGraphDistanceMatrix(&g)
	err = AddProviderMappingToNodes(&g, providers, matrix)
	require.NoError(t, err)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "ns"})
	out, err := BuildTofuFromGraph(&g, providers, platformorchestratorcp.Runner{StateStorageConfiguration: ssc}, uuid.Nil, nil, matrix)
	require.NoError(t, err)
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "00000000-0000-0000-0000-000000000000"
    namespace         = "ns"
    in_cluster_config = true
  }
  required_providers {
    example-static-e3e89394 = {
      source  = "a/b"
      version = "c"
    }
    example-dynamic-28002a35 = {
      source  = "a/b"
      version = "c"
    }
    example-dynamic-c75aa0e0 = {
      source  = "a/b"
      version = "c"
    }
  }
}

provider "example-static-e3e89394" {
  alias = "example-static-e3e89394"
  foo   = "bar"
}

module "child_default_workloadsworkthing1_a3431705" {
  source = "some/source"
  providers = {
    example = example-static-e3e89394.example-static-e3e89394
  }
}

provider "example-dynamic-28002a35" {
  alias = "example-dynamic-28002a35"
  foo   = module.child_default_workloadsworkthing1_a3431705.thing
}

module "thing_default_workloadsworkthing1_3ac0c440" {
  source = "some/source"
  providers = {
    example = example-dynamic-28002a35.example-dynamic-28002a35
  }

  depends_on = [module.child_default_workloadsworkthing1_a3431705]
}

module "child_default_workloadsworkthing2_bb5a41e3" {
  source = "some/source"
  providers = {
    example = example-static-e3e89394.example-static-e3e89394
  }
}

provider "example-dynamic-c75aa0e0" {
  alias = "example-dynamic-c75aa0e0"
  foo   = module.child_default_workloadsworkthing2_bb5a41e3.thing
}

module "thing_default_workloadsworkthing2_9ef68a3f" {
  source = "some/source"
  providers = {
    example = example-dynamic-c75aa0e0.example-dynamic-c75aa0e0
  }

  depends_on = [module.child_default_workloadsworkthing2_bb5a41e3]
}

output "platform_orchestrator_metadata" {
  value = {
    "759f8726a215a812ebaed113331079d4d2d95e6c906c9f96ac4d7726870db831" = lookup(module.child_default_workloadsworkthing1_a3431705, "platform_orchestrator_metadata", {})
    "c0c7d5e8ead7e05de8e38d5e7a65a9842fec416cb9a8f997991d91b1216c9e5b" = lookup(module.thing_default_workloadsworkthing1_3ac0c440, "platform_orchestrator_metadata", {})
    "5e035ba4f00de4926e7f5e5c94ef2122802051ac524b9a3f0bc35b872f2269a4" = lookup(module.child_default_workloadsworkthing2_bb5a41e3, "platform_orchestrator_metadata", {})
    "0de78b7aa8c9377b621691d279e058d18a72a45ee5d05243d03dd7744be0667a" = lookup(module.thing_default_workloadsworkthing2_9ef68a3f, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "work" {
  value       = {}
  description = "The output variables for workload 'work'"
  sensitive   = true
}
`, string(out))
}

func TestBuildTofuFromGraph_with_bad_dynamic_providers(t *testing.T) {
	g, err := platform_orchestrator_graph.SeedAndExpandAll[*GraphNodeModuleConfig](
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Resources: map[string]platform_orchestrator_graph.ManifestResource{
						"thing": {Type: "thing"},
					},
				},
			},
		},
		*platform_orchestrator_graph.NewModuleDefinitionIndex[*GraphNodeModuleConfig]([]platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig]{
			{ResourceType: "thing", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
				DefinitionId: "my-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
					Id: "my-def", VersionId: "v1", ModuleSource: "some/source",
					ProviderMapping: map[string]string{
						"example": "example.dynamic",
					},
				},
			}},
		}),
		nil,
	)
	require.NoError(t, err)
	providers := []platformorchestratorcp.ModuleProvider{
		{ProviderType: "example", Id: "dynamic", Configuration: map[string]interface{}{"foo": "${resources.c.outputs.thing}"}, Source: "a/b", VersionConstraint: "c"},
	}
	matrix := BuildGraphDistanceMatrix(&g)
	err = AddProviderMappingToNodes(&g, providers, matrix)
	require.EqualError(t, err, "provider 'example.dynamic' for module 'my-def@v1' in context of node 'type=thing,class=default,id=workloads.work.thing': failed to resolve placeholders: invalid placeholder '${resources.c.outputs.thing}': no resource dependency with alias 'c' exists")
}

func TestBuildTofuFromGraph_with_deleted_resource_static_provider(t *testing.T) {
	index := *platform_orchestrator_graph.NewModuleDefinitionIndex[*GraphNodeModuleConfig]([]platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig]{
		{ResourceType: "thing", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
			DefinitionId: "my-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
				Id: "my-def", VersionId: "v1", ModuleSource: "some/source",
				ProviderMapping: map[string]string{
					"example": "example.static",
				},
			},
		}},
	})
	providers := []platformorchestratorcp.ModuleProvider{
		{ProviderType: "example", Id: "static", Configuration: map[string]interface{}{"foo": "bar"}, Source: "a/b", VersionConstraint: "c"},
	}

	graphA, err := platform_orchestrator_graph.SeedAndExpandAll[*GraphNodeModuleConfig](
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Resources: map[string]platform_orchestrator_graph.ManifestResource{
						"t": {Type: "thing"},
					},
				},
			},
		}, index, nil,
	)
	require.NoError(t, err)

	graphB, err := platform_orchestrator_graph.SeedAndExpandAll[*GraphNodeModuleConfig](
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Resources: map[string]platform_orchestrator_graph.ManifestResource{},
				},
			},
		}, index, &graphA,
	)
	require.NoError(t, err)
	AddDeletedNodes(&graphB, &graphA, false)
	matrix := BuildGraphDistanceMatrix(&graphB)
	err = AddProviderMappingToNodes(&graphB, providers, matrix)
	require.NoError(t, err)

	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "ns"})
	out, err := BuildTofuFromGraph(&graphB, providers, platformorchestratorcp.Runner{StateStorageConfiguration: ssc}, uuid.Nil, nil, matrix)
	require.NoError(t, err)
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "00000000-0000-0000-0000-000000000000"
    namespace         = "ns"
    in_cluster_config = true
  }
  required_providers {
    example-static-e3e89394 = {
      source  = "a/b"
      version = "c"
    }
  }
}

provider "example-static-e3e89394" {
  alias = "example-static-e3e89394"
  foo   = "bar"
}

output "platform_orchestrator_metadata" {
  value       = {}
  description = "The metadata output from the modules involved in the deployment"
}

output "work" {
  value       = {}
  description = "The output variables for workload 'work'"
  sensitive   = true
}
`, string(out))
}

func TestBuildTofuFromGraph_with_deleted_resource_dynamic_provider(t *testing.T) {
	index := *platform_orchestrator_graph.NewModuleDefinitionIndex[*GraphNodeModuleConfig]([]platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig]{
		{ResourceType: "thing", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
			DefinitionId: "my-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
				Id: "my-def", VersionId: "v1", ModuleSource: "some/source",
				ProviderMapping: map[string]string{
					"example": "example.dynamic",
				},
				Dependencies: map[string]platformorchestratorcp.ModuleDependencyManifest{
					"c": {Type: "child"},
				},
			},
		}},
		{ResourceType: "child", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
			DefinitionId: "my-child-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
				Id: "my-child-def", VersionId: "v1", ModuleSource: "some/source",
				ProviderMapping: map[string]string{
					"example": "example.static",
				},
			},
		}},
	})
	providers := []platformorchestratorcp.ModuleProvider{
		{ProviderType: "example", Id: "dynamic", Configuration: map[string]interface{}{"foo": "${resources.c.outputs.thing}"}, Source: "a/b", VersionConstraint: "c"},
		{ProviderType: "example", Id: "static", Configuration: map[string]interface{}{"foo": "bar"}, Source: "a/b", VersionConstraint: "c"},
	}

	graphA, err := platform_orchestrator_graph.SeedAndExpandAll[*GraphNodeModuleConfig](
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Resources: map[string]platform_orchestrator_graph.ManifestResource{
						"t": {Type: "thing"},
					},
				},
			},
		}, index, nil,
	)
	require.NoError(t, err)

	graphB, err := platform_orchestrator_graph.SeedAndExpandAll[*GraphNodeModuleConfig](
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Resources: map[string]platform_orchestrator_graph.ManifestResource{},
				},
			},
		}, index, &graphA,
	)
	require.NoError(t, err)
	AddDeletedNodes(&graphB, &graphA, false)

	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "ns"})
	matrix := BuildGraphDistanceMatrix(&graphB)
	err = AddProviderMappingToNodes(&graphB, providers, matrix)
	require.EqualError(t, err, "provider 'example.dynamic' for module 'my-def@v1' in context of node 'type=thing,class=default,id=workloads.work.t': "+
		"failed to resolve placeholders: placeholder '${resources.c.outputs.thing}' resolves to a deleted node 'type=child,class=default,id=workloads.work.t' which must be anchored in the graph")
}

func TestBuildTofuFromGraph_with_provider_block(t *testing.T) {
	g, err := platform_orchestrator_graph.SeedAndExpandAll[*GraphNodeModuleConfig](
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Resources: map[string]platform_orchestrator_graph.ManifestResource{
						"t": {Type: "thing"},
					},
				},
			},
		},
		*platform_orchestrator_graph.NewModuleDefinitionIndex[*GraphNodeModuleConfig]([]platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig]{
			{ResourceType: "thing", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
				DefinitionId: "my-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
					Id: "my-def", VersionId: "v1", ModuleSource: "some/source",
					ProviderMapping: map[string]string{
						"eg": "eg.default",
					},
				},
			}},
		}),
		nil,
	)
	require.NoError(t, err)
	providers := []platformorchestratorcp.ModuleProvider{
		{ProviderType: "eg", Id: "default", Configuration: map[string]interface{}{"foo": "bar", "thing[0]": map[string]interface{}{}, "thing[1]": map[string]interface{}{"a": "b"}}, Source: "a/b", VersionConstraint: "c"},
	}
	matrix := BuildGraphDistanceMatrix(&g)
	err = AddProviderMappingToNodes(&g, providers, matrix)
	require.NoError(t, err)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "ns"})
	out, err := BuildTofuFromGraph(&g, providers, platformorchestratorcp.Runner{StateStorageConfiguration: ssc}, uuid.Nil, nil, matrix)
	require.NoError(t, err)
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "00000000-0000-0000-0000-000000000000"
    namespace         = "ns"
    in_cluster_config = true
  }
  required_providers {
    eg-default-c8102dba = {
      source  = "a/b"
      version = "c"
    }
  }
}

provider "eg-default-c8102dba" {
  alias = "eg-default-c8102dba"
  foo   = "bar"
  thing {
  }
  thing {
    a = "b"
  }
}

module "thing_default_workloadsworkt_186ba20e" {
  source = "some/source"
  providers = {
    eg = eg-default-c8102dba.eg-default-c8102dba
  }
}

output "platform_orchestrator_metadata" {
  value = {
    "a389b969c568fd6faeacf9aa67abff9546374228cef1c95be9757caa7eedab74" = lookup(module.thing_default_workloadsworkt_186ba20e, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "work" {
  value       = {}
  description = "The output variables for workload 'work'"
  sensitive   = true
}
`, string(out))
}

func TestBuildTofuFromGraph_with_self_placeholder(t *testing.T) {
	g, err := platform_orchestrator_graph.SeedAndExpandAll[*GraphNodeModuleConfig](
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Resources: map[string]platform_orchestrator_graph.ManifestResource{
						"parent": {Type: "parent"},
					},
				},
			},
		},
		*platform_orchestrator_graph.NewModuleDefinitionIndex[*GraphNodeModuleConfig]([]platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig]{
			{ResourceType: "parent", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
				DefinitionId: "parent-dev", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
					Id: "parent-def", VersionId: "v1", ModuleSource: "some/source",
					Coprovisioned: []platformorchestratorcp.ModuleCoProvisionManifest{
						{
							Type: "child",
							Params: map[string]interface{}{
								"x": "${self.outputs.x}",
							},
						},
					},
				},
			}},
			{ResourceType: "child", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
				DefinitionId: "child-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
					Id: "child-def", VersionId: "v1", ModuleSource: "some/source",
				},
			}},
		}),
		nil,
	)
	require.NoError(t, err)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "ns"})

	// Build distance matrix
	matrix := BuildGraphDistanceMatrix(&g)

	out, err := BuildTofuFromGraph(&g, nil, platformorchestratorcp.Runner{StateStorageConfiguration: ssc}, uuid.Nil, nil, matrix)
	require.NoError(t, err)
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "00000000-0000-0000-0000-000000000000"
    namespace         = "ns"
    in_cluster_config = true
  }
  required_providers {
  }
}

module "child_default_workloadsworkparent_7038d4cb" {
  source = "some/source"

  x = module.parent_default_workloadsworkparent_ad2fc672.x

  depends_on = [module.parent_default_workloadsworkparent_ad2fc672]
}

module "parent_default_workloadsworkparent_ad2fc672" {
  source = "some/source"
}

output "platform_orchestrator_metadata" {
  value = {
    "95f221afa982e8e72c9ba3ecab6ccf0daa9c503a9f0089ef0a832dd7dd33dd82" = lookup(module.child_default_workloadsworkparent_7038d4cb, "platform_orchestrator_metadata", {})
    "9abf2f65dff5258519f4e03422a0c665953e793d93e1d05edbee7f0e7c9601a4" = lookup(module.parent_default_workloadsworkparent_ad2fc672, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "work" {
  value       = {}
  description = "The output variables for workload 'work'"
  sensitive   = true
}
`, string(out))
}

func TestBuildTofuFromGraph_with_self_placeholder_cycle(t *testing.T) {
	g, err := platform_orchestrator_graph.SeedAndExpandAll[*GraphNodeModuleConfig](
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Resources: map[string]platform_orchestrator_graph.ManifestResource{
						"parent": {Type: "parent"},
					},
				},
			},
		},
		*platform_orchestrator_graph.NewModuleDefinitionIndex[*GraphNodeModuleConfig]([]platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig]{
			{ResourceType: "parent", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
				DefinitionId: "parent-dev", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
					Id: "parent-def", VersionId: "v1", ModuleSource: "some/source",
					ModuleInputs: map[string]interface{}{
						"x": "${self.outputs.x}",
					},
				},
			}},
		}),
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, map[platform_orchestrator_graph.ResourceCoordinate]map[string]platform_orchestrator_graph.ResourceCoordinate{
		platform_orchestrator_graph.ResourceCoordinate{Type: "parent", Class: "default", Id: "workloads.work.parent"}: {},
		platform_orchestrator_graph.ResourceCoordinate{Type: "workload", Class: "default", Id: "work"}: {
			"parent": platform_orchestrator_graph.ResourceCoordinate{Type: "parent", Class: "default", Id: "workloads.work.parent"},
		},
	}, g.Edges)
}

func TestBuildTofuFromGraph_with_duplicate_dynamic_providers(t *testing.T) {
	g, err := platform_orchestrator_graph.SeedAndExpandAll[*GraphNodeModuleConfig](
		t.Context(),
		platform_orchestrator_graph.Manifest{
			Workloads: map[string]platform_orchestrator_graph.ManifestWorkload{
				"work": {
					Resources: map[string]platform_orchestrator_graph.ManifestResource{
						"thing1": {Type: "thing"},
						"thing2": {Type: "thing"},
					},
				},
			},
		},
		*platform_orchestrator_graph.NewModuleDefinitionIndex[*GraphNodeModuleConfig]([]platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig]{
			{ResourceType: "thing", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
				DefinitionId: "my-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
					Id: "my-def", VersionId: "v1", ModuleSource: "some/source",
					ProviderMapping: map[string]string{
						"example": "example.dynamic",
					},
					Dependencies: map[string]platformorchestratorcp.ModuleDependencyManifest{
						"c": {Type: "child", Id: ref.Ref("specific")},
					},
				},
			}},
			{ResourceType: "child", Rules: []platform_orchestrator_graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
				DefinitionId: "my-child-def", VersionId: "v1", Definition: &platformorchestratorcp.InternalModuleCatalogueModule{
					Id: "my-child-def", VersionId: "v1", ModuleSource: "some/source",
				},
			}},
		}),
		nil,
	)
	require.NoError(t, err)
	providers := []platformorchestratorcp.ModuleProvider{
		{ProviderType: "example", Id: "static", Configuration: map[string]interface{}{"foo": "bar"}, Source: "a/b", VersionConstraint: "c"},
		{ProviderType: "example", Id: "dynamic", Configuration: map[string]interface{}{"foo": "${resources.c.outputs.thing}"}, Source: "a/b", VersionConstraint: "c"},
	}
	matrix := BuildGraphDistanceMatrix(&g)
	err = AddProviderMappingToNodes(&g, providers, matrix)
	require.NoError(t, err)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "ns"})
	out, err := BuildTofuFromGraph(&g, providers, platformorchestratorcp.Runner{StateStorageConfiguration: ssc}, uuid.Nil, nil, matrix)
	require.NoError(t, err)
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "00000000-0000-0000-0000-000000000000"
    namespace         = "ns"
    in_cluster_config = true
  }
  required_providers {
    example-dynamic-72648817 = {
      source  = "a/b"
      version = "c"
    }
  }
}

module "child_default_specific_93425fb3" {
  source = "some/source"
}

provider "example-dynamic-72648817" {
  alias = "example-dynamic-72648817"
  foo   = module.child_default_specific_93425fb3.thing
}

module "thing_default_workloadsworkthing1_3ac0c440" {
  source = "some/source"
  providers = {
    example = example-dynamic-72648817.example-dynamic-72648817
  }

  depends_on = [module.child_default_specific_93425fb3]
}

module "thing_default_workloadsworkthing2_9ef68a3f" {
  source = "some/source"
  providers = {
    example = example-dynamic-72648817.example-dynamic-72648817
  }

  depends_on = [module.child_default_specific_93425fb3]
}

output "platform_orchestrator_metadata" {
  value = {
    "0fdea5beac6d72b2a360c434b6576bcec624e049926cc08565dbf40374d0acde" = lookup(module.child_default_specific_93425fb3, "platform_orchestrator_metadata", {})
    "c0c7d5e8ead7e05de8e38d5e7a65a9842fec416cb9a8f997991d91b1216c9e5b" = lookup(module.thing_default_workloadsworkthing1_3ac0c440, "platform_orchestrator_metadata", {})
    "0de78b7aa8c9377b621691d279e058d18a72a45ee5d05243d03dd7744be0667a" = lookup(module.thing_default_workloadsworkthing2_9ef68a3f, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "work" {
  value       = {}
  description = "The output variables for workload 'work'"
  sensitive   = true
}
`, string(out))
}
