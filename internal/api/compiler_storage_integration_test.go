//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/errcodes"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/genevents"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	iamapi "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var withoutQueue = ref.Ref(Reject)

func TestCreateDeployment_success_graph_and_redeploy(t *testing.T) {
	f := newCompilerFixture(t)
	f.core.set(func(*queueCore) { f.unassigned = true })
	var dep1, dep2 Deployment
	if !t.Run("first deployment", func(t *testing.T) {
		f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
			Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
				{
					ResourceType: "postgres",
					Id:           "d1", VersionId: "v1",
					ModuleSource:    "git::ssh://git@github.com/tofu-org/tofu-registry.git",
					ProviderMapping: map[string]string{"a1": "aws.default"},
					Rules:           []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
				},
				{
					ResourceType: "s3",
					Id:           "d2", VersionId: "v1",
					ModuleSource: "registry.tofu.com/ns/prov/aws@v1",
					ModuleParams: map[string]platformorchestratorcp.ModuleParamItem{
						"n": {Type: "string", IsOptional: true},
					},
					ModuleInputs: map[string]interface{}{
						"a": "b",
						"b": 42,
						"c": map[string]interface{}{
							"d": "e",
							"f": 42,
							"h": map[string]interface{}{
								"x y z": 2,
							},
							"s": []interface{}{"a", 15, true},
							"z": "${resources.linked.outputs.one}",
						},
						"t": []interface{}{"d", 2, true, "one=${resources.linked.outputs.one}"},
						"x": "${resources.linked.outputs.one}",
						"y": `one:${resources.linked.outputs.one},two:${resources.linked.outputs.two},three:$\{HOSTNAME}`,
						"z": `$\{HOSTNAME}`,
					},
					Dependencies: map[string]platformorchestratorcp.ModuleDependencyManifest{
						"linked": {
							Type: "fruit",
						},
					},
					Rules: []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
				},
				{
					ResourceType: "fruit",
					Id:           "d3", VersionId: "v1",
					ModuleSource: "/some/source@v1",
					Rules:        []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
				},
			},
			Providers: []platformorchestratorcp.ModuleProvider{
				{ProviderType: "aws", Id: "default", Source: "hashicorp/aws", VersionConstraint: ">= 1", Configuration: map[string]interface{}{"region": "us-east-1"}},
			},
		})
		dep1 = f.create(t, &CreateDeploymentJSONRequestBody{
			ProjectId: queueProject, EnvId: queueEnv,
			Mode: "deploy", WhenBusy: withoutQueue,
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {
						Resources: map[string]DeploymentManifestResource{
							"first": {Type: "postgres"},
						},
						Outputs: map[string]string{
							"foo": "bar",
							"x":   "${resources.first.outputs.x}",
							"y":   "${shared.three.outputs.y}",
							"z":   `$\{HOSTNAME}`,
						},
					},
					"two": {
						Resources: map[string]DeploymentManifestResource{
							"first": {Type: "postgres"},
							"second": {
								Type: "s3",
								Params: map[string]interface{}{
									"n": "${resources.first.outputs.n}",
								},
							},
						},
						Outputs: map[string]string{},
					},
				},
				Shared: map[string]DeploymentManifestResource{
					"three": {
						Type: "postgres",
					},
				},
			},
			EncryptedOutputsRecipient: ref.Ref("age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"),
		})
		p := f.stored(t, dep1.Id)
		assert.Equal(t, model.DeploymentModeDeploy, p.Mode)
		assert.Equal(t, "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p", p.EncryptedOutputsRecipient.Or(""))
		assert.Equal(t, uuid.NewMD5(uuid.Nil, []byte("my-env")), p.DeploymentEnvUuid)
		g, err := graphs.FromJson(bytes.NewReader(p.Graph))
		require.NoError(t, err)
		var order []string
		for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
			order = append(order, rc.String())
		}
		assert.Equal(t, []string{
			"type=postgres,class=default,id=shared.three",
			"type=postgres,class=default,id=workloads.one.first",
			"type=workload,class=default,id=one",
			"type=postgres,class=default,id=workloads.two.first",
			"type=fruit,class=default,id=workloads.two.second",
			"type=s3,class=default,id=workloads.two.second",
			"type=workload,class=default,id=two",
		}, order)

		assert.Equal(t, fmt.Sprintf(`terraform {
  backend "kubernetes" {
    secret_suffix     = "77222212-278c-3ccc-b429-5d1508454b60"
    namespace         = "some-ns"
    in_cluster_config = true
  }
  required_providers {
    aws-default-4dff076a = {
      source  = "hashicorp/aws"
      version = ">= 1"
    }
  }
}

provider "aws-default-4dff076a" {
  alias  = "aws-default-4dff076a"
  region = "us-east-1"
}

module "postgres_default_sharedthree_13ad39fd" {
  source = "git::ssh://git@github.com/tofu-org/tofu-registry.git"
  providers = {
    a1 = aws-default-4dff076a.aws-default-4dff076a
  }
}

module "postgres_default_workloadsonefirst_ef074abc" {
  source = "git::ssh://git@github.com/tofu-org/tofu-registry.git"
  providers = {
    a1 = aws-default-4dff076a.aws-default-4dff076a
  }
}

module "postgres_default_workloadstwofirst_a97eb69a" {
  source = "git::ssh://git@github.com/tofu-org/tofu-registry.git"
  providers = {
    a1 = aws-default-4dff076a.aws-default-4dff076a
  }
}

module "fruit_default_workloadstwosecond_67534ef5" {
  source = "/some/source@v1"
}

module "s3_default_workloadstwosecond_0b6c8b72" {
  source  = "registry.tofu.com/ns/prov/aws"
  version = "v1"

  a = "b"
  b = 42
  c = {
    d = "e"
    f = 42
    h = {
      "x y z" = 2
    }
    s = [
      "a",
      15,
      true
    ]
    z = module.fruit_default_workloadstwosecond_67534ef5.one
  }
  t = [
    "d",
    2,
    true,
    "one=${module.fruit_default_workloadstwosecond_67534ef5.one}"
  ]
  x = module.fruit_default_workloadstwosecond_67534ef5.one
  y = "one:${module.fruit_default_workloadstwosecond_67534ef5.one},two:${module.fruit_default_workloadstwosecond_67534ef5.two},three:$${HOSTNAME}"
  z = "$${HOSTNAME}"

  n = module.postgres_default_workloadstwofirst_a97eb69a.n

  depends_on = [module.fruit_default_workloadstwosecond_67534ef5, module.postgres_default_workloadstwofirst_a97eb69a]
}

output "platform_orchestrator_metadata" {
  value = {
    "%s" = lookup(module.postgres_default_sharedthree_13ad39fd, "platform_orchestrator_metadata", {})
    "%s" = lookup(module.postgres_default_workloadsonefirst_ef074abc, "platform_orchestrator_metadata", {})
    "%s" = lookup(module.postgres_default_workloadstwofirst_a97eb69a, "platform_orchestrator_metadata", {})
    "%s" = lookup(module.fruit_default_workloadstwosecond_67534ef5, "platform_orchestrator_metadata", {})
    "%s" = lookup(module.s3_default_workloadstwosecond_0b6c8b72, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "one" {
  value = {
    foo = "bar"
    x   = module.postgres_default_workloadsonefirst_ef074abc.x
    y   = module.postgres_default_sharedthree_13ad39fd.y
    z   = "$${HOSTNAME}"
  }
  description = "The output variables for workload 'one'"
  sensitive   = true
}

output "two" {
  value       = {}
  description = "The output variables for workload 'two'"
  sensitive   = true
}
`,
			util.GenerateNodeHash(p.DeploymentEnvUuid, "postgres", "default", "shared.three"),
			util.GenerateNodeHash(p.DeploymentEnvUuid, "postgres", "default", "workloads.one.first"),
			util.GenerateNodeHash(p.DeploymentEnvUuid, "postgres", "default", "workloads.two.first"),
			util.GenerateNodeHash(p.DeploymentEnvUuid, "fruit", "default", "workloads.two.second"),
			util.GenerateNodeHash(p.DeploymentEnvUuid, "s3", "default", "workloads.two.second"),
		), string(p.Tofu))
		compilerCreatedEvents(t, f, dep1, string(genevents.IoPlatformOrchestratorDeploymentCreated))
		f.core.set(func(*queueCore) { require.False(t, f.unassigned) })
		f.completeCompilerDeployment(t, dep1)
	}) {
		return
	}
	if !t.Run("second deployment", func(t *testing.T) {
		f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
			Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
				{
					ResourceType: "postgres",
					Id:           "d1", VersionId: "v2",
					// NOTICE that this version is slightly different
					ModuleSource: "/some/source@v1.2.4",
					ModuleInputs: map[string]interface{}{
						"context": "${context.org_id} ${context.project_id} ${context.env_id} ${context.env_type_id} ${context.res_type} ${context.res_class} ${context.res_id}",
					},
					ProviderMapping: map[string]string{"a1": "aws.default"},
					Rules:           []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
				},
				{
					ResourceType: "postgres",
					Id:           "d1", VersionId: "v1",
					ModuleSource:    "/some/source@v1.2.3",
					ProviderMapping: map[string]string{"a1": "aws.default"},
					// no rules because it was pinned
				},
				{
					ResourceType: "s3",
					Id:           "d2", VersionId: "v1",
					ModuleSource: "/some/source",
					ModuleInputs: map[string]interface{}{"a": "b", "b": 42},
					Dependencies: map[string]platformorchestratorcp.ModuleDependencyManifest{
						"linked": {
							Type: "fruit",
						},
					},
					// no rules because it was pinned
				},
				{
					ResourceType: "fruit",
					Id:           "d3", VersionId: "v1",
					ModuleSource: "/some/source",
					// no rules because it was pinned
				},
			},
			Providers: []platformorchestratorcp.ModuleProvider{
				{ProviderType: "aws", Id: "default", Source: "hashicorp/aws", VersionConstraint: ">= 1", Configuration: map[string]interface{}{"region": "us-east-1"}},
			},
		})
		dep2 = f.create(t, &CreateDeploymentJSONRequestBody{
			ProjectId: queueProject, EnvId: queueEnv,
			Mode: "plan_only",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {
						Resources: map[string]DeploymentManifestResource{
							"first": {Type: "postgres"},
						},
						Outputs: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
		})
		p := f.stored(t, dep2.Id)
		assert.Equal(t, model.DeploymentModeDeployPlan, p.Mode)

		g, err := graphs.FromJson(bytes.NewReader(p.Graph))
		require.NoError(t, err)
		var order []string
		for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
			x := rc.String()
			if n := g.Nodes[rc]; n.ModuleConfiguration != nil && n.ModuleConfiguration.Deleted {
				x += " (deleted)"
			}
			order = append(order, x)
		}
		assert.Equal(t, []string{
			"type=postgres,class=default,id=workloads.one.first",
			"type=workload,class=default,id=one",
			"type=fruit,class=default,id=workloads.two.second (deleted)",
			"type=postgres,class=default,id=shared.three (deleted)",
			"type=postgres,class=default,id=workloads.two.first (deleted)",
			"type=s3,class=default,id=workloads.two.second (deleted)",
		}, order)

		assert.Equal(t, fmt.Sprintf(`terraform {
  backend "kubernetes" {
    secret_suffix     = "77222212-278c-3ccc-b429-5d1508454b60"
    namespace         = "some-ns"
    in_cluster_config = true
  }
  required_providers {
    aws-default-4dff076a = {
      source  = "hashicorp/aws"
      version = ">= 1"
    }
  }
}

provider "aws-default-4dff076a" {
  alias  = "aws-default-4dff076a"
  region = "us-east-1"
}

module "postgres_default_workloadsonefirst_ef074abc" {
  source = "/some/source@v1.2.4"
  providers = {
    a1 = aws-default-4dff076a.aws-default-4dff076a
  }

  context = "${"queue-org"} ${"project"} ${"environment"} ${""} ${"postgres"} ${"default"} ${"workloads.one.first"}"
}

output "platform_orchestrator_metadata" {
  value = {
    "%s" = lookup(module.postgres_default_workloadsonefirst_ef074abc, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "one" {
  value = {
    foo = "bar"
  }
  description = "The output variables for workload 'one'"
  sensitive   = true
}
`, util.GenerateNodeHash(p.DeploymentEnvUuid, "postgres", "default", "workloads.one.first")), string(p.Tofu))
		compilerCreatedEvents(t, f, dep2, string(genevents.IoPlatformOrchestratorDeploymentCreated), string(genevents.IoPlatformOrchestratorDeploymentUpdated), string(genevents.IoPlatformOrchestratorModuleVersionAdoptionChanged), string(genevents.IoPlatformOrchestratorDeploymentCreated))
		require.Nil(t, dep2.PendingChanges, "this graph flow stays queue-free; the exact queued warning is covered by TestDeploymentQueueAnswersPlansAndDryRunsWithTheirPendingChanges")
		f.completeCompilerDeployment(t, dep2)
	}) {
		return
	}
	if !t.Run("destroy", func(t *testing.T) {
		f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
			Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
				{
					ResourceType: "postgres",
					Id:           "d1", VersionId: "v1",
					ModuleSource:    "/some/source@v1.2.3",
					ProviderMapping: map[string]string{"a1": "aws.default"},
				},
				{
					ResourceType: "s3",
					Id:           "d2", VersionId: "v1",
					ModuleSource: "registry.tofu.com/ns/prov/aws@v1",
					ModuleInputs: map[string]interface{}{
						"a": "b",
						"b": 42,
						"c": map[string]interface{}{
							"d": "e",
							"f": 42,
							"h": map[string]interface{}{
								"i": 2,
							},
							"s": []interface{}{"a", 15, true},
							"z": "${resources.linked.outputs.one}",
						},
						"t": []interface{}{"d", 2, true, "one=${resources.linked.outputs.one}"},
						"x": "${resources.linked.outputs.one}",
						"y": `one:${resources.linked.outputs.one},two:${resources.linked.outputs.two},three:$\{HOSTNAME}`,
						"z": `$\{HOSTNAME}`,
					},
					Dependencies: map[string]platformorchestratorcp.ModuleDependencyManifest{
						"linked": {
							Type: "fruit",
						},
					},
					Rules: []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
				},
				{
					ResourceType: "fruit",
					Id:           "d3", VersionId: "v1",
					ModuleSource: "/some/source",
				},
			},
			Providers: []platformorchestratorcp.ModuleProvider{
				{ProviderType: "aws", Id: "default", Source: "hashicorp/aws", VersionConstraint: ">= 1", Configuration: map[string]interface{}{"region": "us-east-1"}},
			},
		})
		body := &CreateDeploymentJSONRequestBody{ProjectId: queueProject, EnvId: queueEnv, Mode: Destroy}
		public := f.deployTyped(body)
		require.Equal(t, http.StatusBadRequest, public.Code, public.Body.String())
		ctx := context.WithValue(f.ctx, hecho.ContextKeyUserID, f.actor.String())
		created, err := f.server.CreateDeployment(ctx, CreateDeploymentRequestObject{OrgId: queueOrg, Body: body})
		require.NoError(t, err)
		require.IsType(t, CreateDeployment201JSONResponse{}, created)
		dep3 := Deployment(created.(CreateDeployment201JSONResponse))
		p := f.stored(t, dep3.Id)
		assert.Equal(t, model.DeploymentModeDestroy, p.Mode)
		assert.NotEmpty(t, p.Manifest)
		assert.NotEqual(t, "{}", p.Manifest)

		g, err := graphs.FromJson(bytes.NewReader(p.Graph))
		require.NoError(t, err)
		var order []string
		for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
			x := rc.String()
			if n := g.Nodes[rc]; n.ModuleConfiguration != nil && n.ModuleConfiguration.Deleted {
				x += " (deleted)"
			}
			order = append(order, x)
		}
		assert.Equal(t, []string{
			"type=postgres,class=default,id=shared.three",
			"type=postgres,class=default,id=workloads.one.first",
			"type=workload,class=default,id=one",
			"type=postgres,class=default,id=workloads.two.first",
			"type=fruit,class=default,id=workloads.two.second",
			"type=s3,class=default,id=workloads.two.second",
			"type=workload,class=default,id=two",
		}, order)
		compilerCreatedEvents(
			t,
			f,
			dep3,
			string(genevents.IoPlatformOrchestratorDeploymentCreated),
			string(genevents.IoPlatformOrchestratorDeploymentUpdated),
			string(genevents.IoPlatformOrchestratorModuleVersionAdoptionChanged),
			string(genevents.IoPlatformOrchestratorDeploymentCreated),
			string(genevents.IoPlatformOrchestratorDeploymentUpdated),
			string(genevents.IoPlatformOrchestratorDeploymentCreated),
		)
	}) {
		return
	}
	require.Empty(t, f.queued())
}

func TestCreateDeployment_with_rollback(t *testing.T) {
	f := newCompilerFixture(t)
	outputsKeyIdentity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	logsKeyIdentity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	encryptedOutputsRecipient := outputsKeyIdentity.Recipient().String()
	encryptedLogsRecipient := logsKeyIdentity.Recipient().String()

	f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
		Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
			{
				ResourceType: "thing",
				Id:           "some-def", VersionId: "some-ver",
				ModuleSource: "git::ssh://git@github.com/tofu-org/tofu-registry.git",
				Rules:        []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
			},
		},
	})
	baseBody := &CreateDeploymentJSONRequestBody{ProjectId: queueProject, EnvId: queueEnv, Mode: "deploy", Manifest: &DeploymentManifest{Workloads: map[string]DeploymentManifestWorkload{}, Shared: map[string]DeploymentManifestResource{"thing": {Type: "thing"}}}}
	base := f.create(t, baseBody)
	f.completeCompilerDeployment(t, base)
	current := f.create(t, baseBody)
	require.NotEqual(t, base.Id, current.Id)
	f.completeCompilerDeployment(t, current)
	rollbackToId := base.Id
	dep := f.create(t, &CreateDeploymentJSONRequestBody{
		ProjectId: queueProject, EnvId: queueEnv,
		Mode: "rollback", WhenBusy: withoutQueue,
		RollbackToDeploymentId:    &rollbackToId,
		EncryptedOutputsRecipient: ref.Ref(encryptedOutputsRecipient),
		EncryptedLogsRecipient:    ref.Ref(encryptedLogsRecipient),
	})
	p := f.stored(t, dep.Id)
	g, err := graphs.FromJson(bytes.NewReader(p.Graph))
	require.NoError(t, err)
	var order []string
	for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
		order = append(order, rc.String())
	}
	assert.Equal(t, []string{
		"type=thing,class=default,id=shared.thing",
	}, order)
	assert.Equal(t, encryptedOutputsRecipient, p.EncryptedOutputsRecipient.Must())
	assert.Equal(t, encryptedLogsRecipient, p.EncryptedLogsRecipient.Must())
	assert.Equal(t, map[platform_orchestrator_graph.ResourceCoordinate]map[string]platform_orchestrator_graph.ResourceCoordinate{
		{Type: "thing", Class: "default", Id: "shared.thing"}: {},
	}, g.Edges)
	assert.Equal(t, fmt.Sprintf(`terraform {
  backend "kubernetes" {
    secret_suffix     = "77222212-278c-3ccc-b429-5d1508454b60"
    namespace         = "some-ns"
    in_cluster_config = true
  }
  required_providers {
  }
}

module "thing_default_sharedthing_985a74ae" {
  source = "git::ssh://git@github.com/tofu-org/tofu-registry.git"
}

output "platform_orchestrator_metadata" {
  value = {
    "%s" = lookup(module.thing_default_sharedthing_985a74ae, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}
`,
		util.GenerateNodeHash(p.DeploymentEnvUuid, "thing", "default", "shared.thing"),
	), string(p.Tofu))
	require.Equal(t, model.DeploymentModeRollback, p.Mode)
}

func TestCreateDeployment_with_co_provisioned_resource(t *testing.T) {
	f := newCompilerFixture(t)
	outputsKeyIdentity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	logsKeyIdentity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	encryptedOutputsRecipient := outputsKeyIdentity.Recipient().String()
	encryptedLogsRecipient := logsKeyIdentity.Recipient().String()

	f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
		Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
			{
				ResourceType: "thing",
				Id:           "d1", VersionId: "v1",
				ModuleSource:  "git::ssh://git@github.com/tofu-org/tofu-registry.git",
				Rules:         []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
				Coprovisioned: []platformorchestratorcp.ModuleCoProvisionManifest{{Type: "skyhook", IsDependentOnCurrent: true, CopyDependentsFromCurrent: true}},
			},
			{
				ResourceType: "skyhook",
				Id:           "d2", VersionId: "v1",
				ModuleSource: "registry.tofu.com/ns/prov/aws@v1",
				Rules:        []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
			},
		},
	})
	dep := f.create(t, &CreateDeploymentJSONRequestBody{
		ProjectId: queueProject, EnvId: queueEnv,
		Mode: "deploy", WhenBusy: withoutQueue,
		Manifest: &DeploymentManifest{
			Workloads: map[string]DeploymentManifestWorkload{
				"one": {
					Resources: map[string]DeploymentManifestResource{
						"first": {Type: "thing"},
					},
				},
			},
		},
		EncryptedOutputsRecipient: ref.Ref(encryptedOutputsRecipient),
		EncryptedLogsRecipient:    ref.Ref(encryptedLogsRecipient),
	})
	p := f.stored(t, dep.Id)
	g, err := graphs.FromJson(bytes.NewReader(p.Graph))
	require.NoError(t, err)
	var order []string
	for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
		order = append(order, rc.String())
	}
	assert.Equal(t, []string{
		"type=thing,class=default,id=workloads.one.first",
		"type=skyhook,class=default,id=workloads.one.first",
		"type=workload,class=default,id=one",
	}, order)
	assert.Equal(t, encryptedOutputsRecipient, p.EncryptedOutputsRecipient.Must())
	assert.Equal(t, encryptedLogsRecipient, p.EncryptedLogsRecipient.Must())
	assert.Equal(t, map[platform_orchestrator_graph.ResourceCoordinate]map[string]platform_orchestrator_graph.ResourceCoordinate{
		{Type: "skyhook", Class: "default", Id: "workloads.one.first"}: {"EUcAR+Ue7PkouJGC19+pLUGefZjcZdzWqbXYjm4AjKY": {Type: "thing", Class: "default", Id: "workloads.one.first"}},
		{Type: "thing", Class: "default", Id: "workloads.one.first"}:   {},
		{Type: "workload", Class: "default", Id: "one"}: {
			"first": {Type: "thing", Class: "default", Id: "workloads.one.first"},
			"HwCdB5WY2ptjN0CHkg10M0qxZUUmDL0vy0xW2pFMzm8": {Type: "skyhook", Class: "default", Id: "workloads.one.first"},
		},
	}, g.Edges)
	assert.Equal(t, fmt.Sprintf(`terraform {
  backend "kubernetes" {
    secret_suffix     = "77222212-278c-3ccc-b429-5d1508454b60"
    namespace         = "some-ns"
    in_cluster_config = true
  }
  required_providers {
  }
}

module "thing_default_workloadsonefirst_12f02beb" {
  source = "git::ssh://git@github.com/tofu-org/tofu-registry.git"
}

module "skyhook_default_workloadsonefirst_a8b799eb" {
  source  = "registry.tofu.com/ns/prov/aws"
  version = "v1"

  depends_on = [module.thing_default_workloadsonefirst_12f02beb]
}

output "platform_orchestrator_metadata" {
  value = {
    "%s" = lookup(module.thing_default_workloadsonefirst_12f02beb, "platform_orchestrator_metadata", {})
    "%s" = lookup(module.skyhook_default_workloadsonefirst_a8b799eb, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "one" {
  value       = {}
  description = "The output variables for workload 'one'"
  sensitive   = true
}
`,
		util.GenerateNodeHash(p.DeploymentEnvUuid, "thing", "default", "workloads.one.first"),
		util.GenerateNodeHash(p.DeploymentEnvUuid, "skyhook", "default", "workloads.one.first"),
	), string(p.Tofu))
}

func TestCreateDeployment_with_inline_module_code(t *testing.T) {
	f := newCompilerFixture(t)
	f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
		Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
			{
				ResourceType: "thing",
				Id:           "d1", VersionId: "v1",
				ModuleSourceCode: ref.Ref(`variable "in" {}`),
				Rules:            []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
			},
		},
	})
	dep := f.create(t, &CreateDeploymentJSONRequestBody{
		ProjectId: queueProject, EnvId: queueEnv,
		Mode: "deploy", WhenBusy: withoutQueue,
		Manifest: &DeploymentManifest{
			Workloads: map[string]DeploymentManifestWorkload{
				"one": {
					Resources: map[string]DeploymentManifestResource{
						"first": {Type: "thing"},
					},
				},
			},
		},
	})
	p := f.stored(t, dep.Id)
	assert.Equal(t, fmt.Sprintf(`terraform {
  backend "kubernetes" {
    secret_suffix     = "77222212-278c-3ccc-b429-5d1508454b60"
    namespace         = "some-ns"
    in_cluster_config = true
  }
  required_providers {
  }
}

module "thing_default_workloadsonefirst_12f02beb" {
  source = "./modules/d1/v1"
}

output "platform_orchestrator_metadata" {
  value = {
    "%s" = lookup(module.thing_default_workloadsonefirst_12f02beb, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "one" {
  value       = {}
  description = "The output variables for workload 'one'"
  sensitive   = true
}
`, util.GenerateNodeHash(p.DeploymentEnvUuid, "thing", "default", "workloads.one.first")), string(p.Tofu))
}

func TestCreateDeployment_with_deprecated_variables(t *testing.T) {
	f := newCompilerFixture(t)
	f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
		Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
			{
				ResourceType: "thing",
				Id:           "d1", VersionId: "v1",
				ModuleSourceCode: ref.Ref(`variable "in" {}`),
				Rules:            []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
			},
		},
	})
	dep := f.create(t, &CreateDeploymentJSONRequestBody{
		ProjectId: queueProject, EnvId: queueEnv,
		Mode: "deploy", WhenBusy: withoutQueue,
		Manifest: &DeploymentManifest{
			Workloads: map[string]DeploymentManifestWorkload{
				"one": {
					Resources: map[string]DeploymentManifestResource{
						"first": {Type: "thing"},
					},
					Variables: map[string]string{
						"foo": "bar",
					},
				},
			},
		},
	})
	p := f.stored(t, dep.Id)
	assert.Equal(t, fmt.Sprintf(`terraform {
  backend "kubernetes" {
    secret_suffix     = "77222212-278c-3ccc-b429-5d1508454b60"
    namespace         = "some-ns"
    in_cluster_config = true
  }
  required_providers {
  }
}

module "thing_default_workloadsonefirst_12f02beb" {
  source = "./modules/d1/v1"
}

output "platform_orchestrator_metadata" {
  value = {
    "%s" = lookup(module.thing_default_workloadsonefirst_12f02beb, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "one" {
  value = {
    foo = "bar"
  }
  description = "The output variables for workload 'one'"
  sensitive   = true
}
`, util.GenerateNodeHash(p.DeploymentEnvUuid, "thing", "default", "workloads.one.first")), string(p.Tofu))
}

func TestCreateDeployment_success_graph_test_mode(t *testing.T) {
	f := newCompilerFixture(t)
	f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
		Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
			{
				ResourceType: "postgres",
				Id:           "d1", VersionId: "v1",
				ModuleSource:    "git::ssh://git@github.com/tofu-org/tofu-registry.git",
				ProviderMapping: map[string]string{"a1": "aws.default"},
				Rules:           []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
			},
		},
		Providers: []platformorchestratorcp.ModuleProvider{
			{ProviderType: "aws", Id: "default", Source: "hashicorp/aws", VersionConstraint: ">= 1", Configuration: map[string]interface{}{"region": "us-east-1", "thing": "${var.XYZ} ${context.env_id}"}},
		},
	})
	dep := f.create(t, &CreateDeploymentJSONRequestBody{
		ProjectId: queueProject, EnvId: queueEnv,
		Mode: "deploy", WhenBusy: withoutQueue,
		Manifest: &DeploymentManifest{
			Workloads: map[string]DeploymentManifestWorkload{
				"one": {
					Resources: map[string]DeploymentManifestResource{
						"first": {Type: "postgres"},
					},
					Outputs: map[string]string{
						"foo":  "bar",
						"fizz": "buzz\n${resources.first.outputs.x.y}\n%{x}buzz",
						"x":    "${resources.first.outputs.x.y}",
					},
				},
			},
		},
	})
	p := f.stored(t, dep.Id)
	assert.Equal(t, model.DeploymentModeDeploy, p.Mode)
	g, err := graphs.FromJson(bytes.NewReader(p.Graph))
	require.NoError(t, err)
	var order []string
	for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
		order = append(order, rc.String())
	}
	assert.Equal(t, []string{
		"type=postgres,class=default,id=workloads.one.first",
		"type=workload,class=default,id=one",
	}, order)

	assert.Equal(t, fmt.Sprintf(`terraform {
  backend "kubernetes" {
    secret_suffix     = "77222212-278c-3ccc-b429-5d1508454b60"
    namespace         = "some-ns"
    in_cluster_config = true
  }
  required_providers {
    aws-default-4dff076a = {
      source  = "hashicorp/aws"
      version = ">= 1"
    }
  }
}

provider "aws-default-4dff076a" {
  alias  = "aws-default-4dff076a"
  region = "us-east-1"
  thing  = "${var.XYZ} ${"environment"}"
}

variable "XYZ" {
  type      = string
  sensitive = true
}

module "postgres_default_workloadsonefirst_ef074abc" {
  source = "git::ssh://git@github.com/tofu-org/tofu-registry.git"
  providers = {
    a1 = aws-default-4dff076a.aws-default-4dff076a
  }
}

output "platform_orchestrator_metadata" {
  value = {
    "%s" = lookup(module.postgres_default_workloadsonefirst_ef074abc, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "one" {
  value = {
    fizz = "buzz\n${module.postgres_default_workloadsonefirst_ef074abc.x.y}\n%%%%{x}buzz"
    foo  = "bar"
    x    = module.postgres_default_workloadsonefirst_ef074abc.x.y
  }
  description = "The output variables for workload 'one'"
  sensitive   = true
}
`, util.GenerateNodeHash(p.DeploymentEnvUuid, "postgres", "default", "workloads.one.first")), string(p.Tofu))
	compilerCreatedEvents(t, f, dep, string(genevents.IoPlatformOrchestratorDeploymentCreated))
}

func TestCreateDeployment_fail_graph_error(t *testing.T) {
	f := newCompilerFixture(t)
	f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
		Modules:   []platformorchestratorcp.InternalModuleCatalogueModule{},
		Providers: []platformorchestratorcp.ModuleProvider{},
	})
	response := f.deployTyped(&CreateDeploymentJSONRequestBody{
		ProjectId: queueProject, EnvId: queueEnv,
		Mode: "deploy", WhenBusy: withoutQueue,
		Manifest: &DeploymentManifest{
			Workloads: map[string]DeploymentManifestWorkload{
				"one": {
					Resources: map[string]DeploymentManifestResource{
						"first": {Type: "postgres"},
					},
				},
			},
		},
	})
	compilerRejected(t, f, response, http.StatusBadRequest, "graph contains 1 errors:\n\ttype=postgres,class=default,id=workloads.one.first: no module definition matches this resource\n")
}

func TestCreateDeployment_fail_missing_provider(t *testing.T) {
	f := newCompilerFixture(t)
	f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
		Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
			{
				ResourceType: "postgres",
				Id:           "d1", VersionId: "v1",
				ModuleSource:    "/some/source@v1.2.3",
				ProviderMapping: map[string]string{"a1": "aws.default"},
				Rules:           []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
			},
		},
		Providers: []platformorchestratorcp.ModuleProvider{},
	})
	response := f.deployTyped(&CreateDeploymentJSONRequestBody{
		ProjectId: queueProject, EnvId: queueEnv,
		Mode: "deploy", WhenBusy: withoutQueue,
		Manifest: &DeploymentManifest{
			Workloads: map[string]DeploymentManifestWorkload{
				"one": {
					Resources: map[string]DeploymentManifestResource{
						"first": {Type: "postgres"},
					},
				},
			},
		},
	})
	compilerRejected(t, f, response, http.StatusBadRequest, "unknown provider 'aws.default' used by module definition d1@v1 for type=postgres,class=default,id=workloads.one.first")
}

func TestCreateDeployment_catalogue_fail(t *testing.T) {
	f := newCompilerFixture(t)
	f.core.set(func(*queueCore) {
		f.catalogueError = &platformorchestratorcp.Error{Error: string(errcodes.PinnedModuleMissingProvider), Message: "conflict", Details: &map[string]interface{}{"missing_providers": []interface{}{"example.default"}}}
	})
	response := f.deployTyped(&CreateDeploymentJSONRequestBody{
		ProjectId: queueProject, EnvId: queueEnv,
		Mode: "deploy", WhenBusy: withoutQueue,
		Manifest: &DeploymentManifest{
			Workloads: map[string]DeploymentManifestWorkload{
				"one": {
					Resources: map[string]DeploymentManifestResource{
						"first": {Type: "postgres"},
					},
				},
			},
		},
	})
	compilerRejected(t, f, response, http.StatusConflict, "MOD-001: existing module(s) depend on providers that no longer exist: [example.default]")
}

func TestCreateDeployment_authorization_fallback_to_org(t *testing.T) {
	f := newCompilerFixture(t)
	outputsKeyIdentity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	logsKeyIdentity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	encryptedOutputsRecipient := outputsKeyIdentity.Recipient().String()
	encryptedLogsRecipient := logsKeyIdentity.Recipient().String()

	var environmentChecks, organizationChecks atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body iamapi.InternalAuthorizeBody
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&body)) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		assert.Equal(t, f.actor, body.UserId)
		switch {
		case reflect.DeepEqual(body.Checks, []iamapi.ResourcePermissionCheck{environmentCheck(f.core.envUUID, PermissionDeploymentWrite)}):
			environmentChecks.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			assert.NoError(t, json.NewEncoder(w).Encode(iamapi.Error{Error: "HTTP-403", Message: "user cannot write to environment", Details: &map[string]interface{}{"message": "user cannot write to environment"}}))
		case reflect.DeepEqual(body.Checks, []iamapi.ResourcePermissionCheck{orgCheck(queueOrg, PermissionDeploymentWrite)}):
			organizationChecks.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected scoped IAM check: %v", body.Checks)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(peer.Close)
	iam, err := iamapi.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	f.server.IamClient = iam

	f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
		Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
			{
				ResourceType: "postgres",
				Id:           "d1", VersionId: "v1",
				ModuleSource: "registry.tofu.com/ns/prov/aws@v1",
				Rules:        []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
			},
		},
	})
	dep := f.create(t, &CreateDeploymentJSONRequestBody{
		ProjectId: queueProject, EnvId: queueEnv,
		Mode: "deploy", WhenBusy: withoutQueue,
		Manifest: &DeploymentManifest{
			Workloads: map[string]DeploymentManifestWorkload{
				"one": {
					Resources: map[string]DeploymentManifestResource{
						"first": {Type: "postgres"},
					},
				},
			},
		},
		EncryptedOutputsRecipient: ref.Ref(encryptedOutputsRecipient),
		EncryptedLogsRecipient:    ref.Ref(encryptedLogsRecipient),
	})
	p := f.stored(t, dep.Id)
	require.Equal(t, encryptedOutputsRecipient, p.EncryptedOutputsRecipient.Or(""))
	require.Equal(t, encryptedLogsRecipient, p.EncryptedLogsRecipient.Or(""))
	require.EqualValues(t, 1, environmentChecks.Load())
	require.EqualValues(t, 1, organizationChecks.Load())
}

func TestCreateDeployment_invalid_placeholder(t *testing.T) {
	tests := []struct {
		name   string
		inputs map[string]interface{}
		err    string
	}{
		{
			name: "unknown alias in placeholder",
			inputs: map[string]interface{}{
				"a": "${resources.unknown.outputs.a}",
			},
			err: "node 'type=postgres,class=default,id=workloads.one.first': failed to resolve placeholders for module input 'a': invalid placeholder '${resources.unknown.outputs.a}': no resource dependency with alias 'unknown' exists",
		},
		{
			name: "placeholder without output",
			inputs: map[string]interface{}{
				"a": "${resources.unknown.outputs.}",
			},
			err: "node 'type=postgres,class=default,id=workloads.one.first': failed to parse placeholders for module input 'a': invalid placeholder 'resources.unknown.outputs.': @26: expected key; got end of line",
		},
		{
			name: "incomplete placeholder",
			inputs: map[string]interface{}{
				"a": "${resources.unknown}",
			},
			err: "node 'type=postgres,class=default,id=workloads.one.first': failed to parse placeholders for module input 'a': invalid placeholder 'resources.unknown': @17: expected \".outputs.\"; got end of line",
		},
		{
			name: "placeholder with invalid first part",
			inputs: map[string]interface{}{
				"a": "${resource.linked.outputs.a}",
			},
			err: "node 'type=postgres,class=default,id=workloads.one.first': failed to parse placeholders for module input 'a': invalid placeholder 'resource.linked.outputs.a': @0: expected one of context., self.outputs., var., resources., shared., or select.; got \"resource\"",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newCompilerFixture(t)
			f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
				Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
					{
						ResourceType: "postgres",
						Id:           "d1", VersionId: "v1",
						ModuleSource:    "/some/source@v1.2.3",
						ProviderMapping: map[string]string{"a1": "aws.default"},
						Rules:           []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
						ModuleInputs:    test.inputs,
						Dependencies: map[string]platformorchestratorcp.ModuleDependencyManifest{
							"linked": {
								Type: "fruit",
							},
						},
					},
					{
						ResourceType: "fruit",
						Id:           "d3", VersionId: "v1",
						ModuleSource: "/some/source",
						Rules:        []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
					},
				},
				Providers: []platformorchestratorcp.ModuleProvider{
					{ProviderType: "aws", Id: "default", Source: "hashicorp/aws", VersionConstraint: ">= 1", Configuration: map[string]interface{}{"region": "us-east-1"}},
				},
			})
			response := f.deployTyped(&CreateDeploymentJSONRequestBody{
				ProjectId: queueProject, EnvId: queueEnv,
				Mode: "deploy", WhenBusy: withoutQueue,
				Manifest: &DeploymentManifest{
					Workloads: map[string]DeploymentManifestWorkload{
						"one": {
							Resources: map[string]DeploymentManifestResource{
								"first": {Type: "postgres"},
							},
						},
					},
				},
			})
			compilerRejected(t, f, response, http.StatusBadRequest, test.err)
		})
	}
}

func TestCreateDeployment_invalid_module_param(t *testing.T) {
	f := newCompilerFixture(t)
	f.setCatalogue(&platformorchestratorcp.InternalModuleCatalogue{
		Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
			{
				ResourceType: "thing",
				Id:           "d1", VersionId: "v1",
				ModuleSource: "/some/source@v1.2.3",
				Rules:        []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
				ModuleParams: map[string]platformorchestratorcp.ModuleParamItem{
					"animal": {Type: platformorchestratorcp.String},
				},
			},
		},
	})
	response := f.deployTyped(&CreateDeploymentJSONRequestBody{
		ProjectId: queueProject, EnvId: queueEnv,
		Mode: "deploy", WhenBusy: withoutQueue,
		Manifest: &DeploymentManifest{
			Workloads: map[string]DeploymentManifestWorkload{
				"one": {
					Resources: map[string]DeploymentManifestResource{
						"t": {Type: "thing", Params: map[string]interface{}{"fruit": "cat"}},
					},
				},
			},
		},
	})
	compilerRejected(t, f, response, http.StatusBadRequest, "node 'type=thing,class=default,id=workloads.one.t': uses module d1@v1 which requires string param 'animal' to be set")
}

func compilerCreatedEvents(t *testing.T, f *compilerFixture, dep Deployment, subjects ...string) {
	t.Helper()
	messages := f.server.EventPublisher.(*hmessaging.RecordingPublisher).Messages()
	actual := make([]string, len(messages))
	for i, message := range messages {
		actual[i] = message.Subject
	}
	require.Equal(t, subjects, actual)
	stored, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, dep.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, 1, stored.Revision)
	require.False(t, stored.CreatedAt.IsZero())
	expected := fmt.Sprintf(`{
  "specversion":"1.0","type":"io.platform-orchestrator.deployment.created","time":%q,
  "data":{"org_id":%q,"project_id":%q,"env_id":%q,"deployment_id":%q,"env_uuid":%q,"revision":1,"status":"executing"}
}`, stored.CreatedAt.Format(time.RFC3339Nano), queueOrg, queueProject, queueEnv, dep.Id.String(), f.core.envUUID.String())
	require.JSONEq(t, expected, string(messages[len(messages)-1].Data))
	pending, _, err := f.store.AsReliableOutboxStore().LoadPage(f.ctx)
	require.NoError(t, err)
	require.Empty(t, pending)
}

func compilerRejected(t *testing.T, f *compilerFixture, response *httptest.ResponseRecorder, status int, message string) {
	t.Helper()
	require.Equal(t, status, response.Code, response.Body.String())
	var problem Error
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &problem))
	require.Equal(t, message, problem.Message)
	for _, table := range []string{"deployments", "environment_deployment_leases", "pending_event_messages"} {
		require.Zero(t, f.count("SELECT count(*) FROM "+table), table)
	}
}
