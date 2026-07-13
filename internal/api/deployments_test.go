package api

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/herrors"
	"github.com/stellwerk-labs/golib/hrabbitmq"
	"github.com/stellwerk-labs/golib/hrabbitmq/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardreliableoutbox"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/errcodes"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/authz"
	platformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/mock/gomock"

	mockplatformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratorcp/mocks"
	mockplatformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratoriam/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	mockmodel "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/completionhooks"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/genevents"
)

const sampleLogs = "test log content"

func TestCreateDeployment_success_graph_and_redeploy(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	mdb := s.Database.(*mockmodel.MockDatabaser)
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)

	depId, dep2Id, envUuid := uuid.New(), uuid.New(), uuid.NewMD5(uuid.Nil, []byte("my-env"))

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(3)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	var envRunnerId string
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", "my-env").
		DoAndReturn(func(_ context.Context, _, _, _ string, _ ...platformorchestratorcp.RequestEditorFn) (*platformorchestratorcp.GetEnvironmentResponse, error) {
			return &platformorchestratorcp.GetEnvironmentResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200: &platformorchestratorcp.Environment{
					Id:       "my-env",
					Uuid:     envUuid,
					Status:   platformorchestratorcp.EnvironmentStatusActive,
					RunnerId: ref.RefStringEmptyNil(envRunnerId),
				},
			}, nil
		}).Times(3)
	cpClient.EXPECT().UpdateRunnerInAnEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", "my-env", &platformorchestratorcp.UpdateRunnerInAnEnvironmentParams{}).
		DoAndReturn(func(_ context.Context, _, _, _ string, _ *platformorchestratorcp.UpdateRunnerInAnEnvironmentParams, _ ...platformorchestratorcp.RequestEditorFn) (*platformorchestratorcp.UpdateRunnerInAnEnvironmentResponse, error) {
			envRunnerId = "my-runner"
			return &platformorchestratorcp.UpdateRunnerInAnEnvironmentResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200:      &platformorchestratorcp.RefreshRunnerActionResult{RunnerId: envRunnerId},
			}, nil
		}).Times(3)

	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "some-ns"})
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200: &platformorchestratorcp.Runner{
			StateStorageConfiguration: ssc,
		},
	}, nil).Times(3)

	store := new(reliableoutbox.InMemoryStorage[*hstandardreliableoutbox.PendingEventMessage])
	mdb.EXPECT().InsertPendingEventMessages(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, m []*hstandardreliableoutbox.PendingEventMessage) ([]*hstandardreliableoutbox.PendingEventMessage, error) {
			store.Put(m)
			return m, nil
		}).Times(3)
	mdb.EXPECT().AsReliableOutboxStore().Return(store).Times(3)
	mdb.EXPECT().InitActiveResourcesFromGraph(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)

	var dep1Graph model.EncodedDeploymentGraph
	t.Run("first deployment", func(t *testing.T) {
		mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
			Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

		cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{}).
			Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200: &platformorchestratorcp.InternalModuleCatalogue{
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
				},
			}, nil)

		mdb.EXPECT().CreateDeployment(gomock.Any(), gomock.Any(), "my-org", "my-project", "my-env", gomock.Any()).
			DoAndReturn(func(_ context.Context, _ model.Tx, _, _, _ string, p model.CreateDeploymentParams) (*model.DeploymentSummary, error) {
				assert.Equal(t, model.DeploymentModeDeploy, p.Mode)
				assert.Equal(t, "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p", p.EncryptedOutputsRecipient.Or(""))
				assert.Equal(t, uuid.NewMD5(uuid.Nil, []byte("my-env")), p.DeploymentEnvUuid)

				dep1Graph = p.Graph
				if g, err := graphs.FromJson(bytes.NewReader(p.Graph)); assert.NoError(t, err) {
					assert.Equal(t, []string{
						"type=postgres,class=default,id=shared.three",
						"type=postgres,class=default,id=workloads.one.first",
						"type=workload,class=default,id=one",
						"type=postgres,class=default,id=workloads.two.first",
						"type=fruit,class=default,id=workloads.two.second",
						"type=s3,class=default,id=workloads.two.second",
						"type=workload,class=default,id=two",
					}, slices.Collect(func(yield func(string) bool) {
						for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
							yield(rc.String())
						}
					}))
				}

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

				return &model.DeploymentSummary{
					OrgId:             "my-org",
					ProjectId:         "my-project",
					EnvId:             "my-env",
					Id:                depId,
					DeploymentEnvUuid: envUuid,
					Status:            model.DeploymentStatusExecuting,
				}, nil
			})

		r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
			OrgId: "my-org",
			Body: &CreateDeploymentJSONRequestBody{
				ProjectId: "my-project", EnvId: "my-env",
				Mode: "deploy",
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
			},
		})
		require.NoError(t, err)
		require.IsTypef(t, CreateDeployment201JSONResponse{}, r, "unexpected %v", r)
		r201 := r.(CreateDeployment201JSONResponse)
		if assert.NotNil(t, r201) {
			assert.Equal(t, depId, r201.Id)
		}

		// assert that we published the message
		if rec := s.RabbitMqPublisher.(*hrabbitmq.NoOpPublisher).GetRecorded(); assert.Len(t, rec, 1) {
			assert.Equal(t, string(genevents.IoPlatformOrchestratorDeploymentCreated), rec[0].Keys[0])
			assert.JSONEq(t, `{
  "specversion": "1.0",
  "type": "io.platform-orchestrator.deployment.created",
  "time": "0001-01-01T00:00:00Z",
  "data": {
    "org_id": "my-org", "project_id": "my-project", "env_id": "my-env",
    "deployment_id": "`+depId.String()+`",
    "env_uuid": "`+envUuid.String()+`",
    "revision": 0,
    "status": "executing"
  }
}`, string(rec[0].Data))
		}

		// assert that the store is now empty
		if p, _, err := store.LoadPage(ctx); assert.NoError(t, err) {
			assert.Empty(t, p)
		}

		t.Run("second deployment", func(t *testing.T) {
			mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
				Return(&model.DeploymentSummary{DeploymentEnvUuid: envUuid, Id: depId}, model.EncodedDeploymentManifest(`{}`), nil, dep1Graph, nil)

			cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{
				PinnedModuleVersions: []string{"d1@v1", "d2@v1", "d3@v1"},
			}).
				Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
					HTTPResponse: &http.Response{StatusCode: http.StatusOK},
					JSON200: &platformorchestratorcp.InternalModuleCatalogue{
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
					},
				}, nil)

			mdb.EXPECT().CreateDeployment(gomock.Any(), gomock.Any(), "my-org", "my-project", "my-env", gomock.Any()).
				DoAndReturn(func(_ context.Context, _ model.Tx, _, _, _ string, p model.CreateDeploymentParams) (*model.DeploymentSummary, error) {
					assert.Equal(t, model.DeploymentModeDeployPlan, p.Mode)

					if g, err := graphs.FromJson(bytes.NewReader(p.Graph)); assert.NoError(t, err) {
						assert.Equal(t, []string{
							"type=postgres,class=default,id=workloads.one.first",
							"type=workload,class=default,id=one",
							"type=fruit,class=default,id=workloads.two.second (deleted)",
							"type=postgres,class=default,id=shared.three (deleted)",
							"type=postgres,class=default,id=workloads.two.first (deleted)",
							"type=s3,class=default,id=workloads.two.second (deleted)",
						}, slices.Collect(func(yield func(string) bool) {
							for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
								x := rc.String()
								if n := g.Nodes[rc]; n.ModuleConfiguration != nil && n.ModuleConfiguration.Deleted {
									x += " (deleted)"
								}
								yield(x)
							}
						}))
					}

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

  context = "${"my-org"} ${"my-project"} ${"my-env"} ${""} ${"postgres"} ${"default"} ${"workloads.one.first"}"
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

					return &model.DeploymentSummary{
						OrgId:             "my-org",
						ProjectId:         "my-project",
						EnvId:             "my-env",
						Id:                dep2Id,
						DeploymentEnvUuid: envUuid,
						Status:            model.DeploymentStatusExecuting,
					}, nil
				})

			// REDEPLOY this time without the second workload
			r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
				OrgId: "my-org",
				Body: &CreateDeploymentJSONRequestBody{
					ProjectId: "my-project", EnvId: "my-env",
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
				},
			})
			require.NoError(t, err)

			require.IsTypef(t, CreateDeployment201JSONResponse{}, r, "unexpected %v", r)
			r201 := r.(CreateDeployment201JSONResponse)
			if assert.NotNil(t, r201) {
				assert.Equal(t, dep2Id, r201.Id)
			}

			// assert that we published the message
			if rec := s.RabbitMqPublisher.(*hrabbitmq.NoOpPublisher).GetRecorded(); assert.Len(t, rec, 2) {
				assert.Equal(t, string(genevents.IoPlatformOrchestratorDeploymentCreated), rec[1].Keys[0])
				assert.JSONEq(t, `{
  "specversion": "1.0",
  "type": "io.platform-orchestrator.deployment.created",
  "time": "0001-01-01T00:00:00Z",
  "data": {
    "org_id": "my-org", "project_id": "my-project", "env_id": "my-env",
    "deployment_id": "`+dep2Id.String()+`",
    "env_uuid": "`+envUuid.String()+`",
    "revision": 0,
    "status": "executing"
  }
}`, string(rec[1].Data))
			}

			// assert that the store is now empty
			if p, _, err := store.LoadPage(ctx); assert.NoError(t, err) {
				assert.Empty(t, p)
			}
		})

		t.Run("destroy", func(t *testing.T) {
			mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
				Return(&model.DeploymentSummary{DeploymentEnvUuid: envUuid, Id: depId}, model.EncodedDeploymentManifest(`{}`), nil, dep1Graph, nil)

			cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{
				PinnedModuleVersions: []string{"d1@v1", "d2@v1", "d3@v1"},
				AreRulesIgnored:      true,
			}).
				Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
					HTTPResponse: &http.Response{StatusCode: http.StatusOK},
					JSON200: &platformorchestratorcp.InternalModuleCatalogue{
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
					},
				}, nil)

			mdb.EXPECT().CreateDeployment(gomock.Any(), gomock.Any(), "my-org", "my-project", "my-env", gomock.Any()).
				DoAndReturn(func(_ context.Context, _ model.Tx, _, _, _ string, p model.CreateDeploymentParams) (*model.DeploymentSummary, error) {
					assert.Equal(t, model.DeploymentModeDestroy, p.Mode)
					assert.NotEmpty(t, p.Manifest)
					assert.NotEqual(t, "{}", p.Manifest)

					if g, err := graphs.FromJson(bytes.NewReader(p.Graph)); assert.NoError(t, err) {
						assert.Equal(t, []string{
							"type=postgres,class=default,id=shared.three",
							"type=postgres,class=default,id=workloads.one.first",
							"type=workload,class=default,id=one",
							"type=postgres,class=default,id=workloads.two.first",
							"type=fruit,class=default,id=workloads.two.second",
							"type=s3,class=default,id=workloads.two.second",
							"type=workload,class=default,id=two",
						}, slices.Collect(func(yield func(string) bool) {
							for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
								x := rc.String()
								if n := g.Nodes[rc]; n.ModuleConfiguration != nil && n.ModuleConfiguration.Deleted {
									x += " (deleted)"
								}
								yield(x)
							}
						}))
					}

					return &model.DeploymentSummary{
						OrgId:             "my-org",
						ProjectId:         "my-project",
						EnvId:             "my-env",
						Id:                dep2Id,
						DeploymentEnvUuid: envUuid,
					}, nil
				})

			// REDEPLOY this time in destroy mode - note this is internal only really
			r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
				OrgId: "my-org",
				Body: &CreateDeploymentJSONRequestBody{
					ProjectId: "my-project", EnvId: "my-env",
					Mode: Destroy,
				},
			})
			require.NoError(t, err)

			if assert.IsTypef(t, CreateDeployment201JSONResponse{}, r, "unexpected %v", r) {
				r201 := r.(CreateDeployment201JSONResponse)
				if assert.NotNil(t, r201) {
					assert.Equal(t, dep2Id, r201.Id)
				}
			}
		})
	})
}

func TestCreateDeployment_with_rollback(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	outputsKeyIdentity, _ := age.GenerateX25519Identity()
	logsKeyIdentity, _ := age.GenerateX25519Identity()

	encryptedOutputsRecipient := outputsKeyIdentity.Recipient().String()
	encryptedLogsRecipient := logsKeyIdentity.Recipient().String()

	depId, envUuid, rollbackToId := uuid.New(), uuid.NewMD5(uuid.Nil, []byte("my-env")), uuid.New()

	mdb := s.Database.(*mockmodel.MockDatabaser)
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", "my-env").
		DoAndReturn(func(_ context.Context, _, _, _ string, _ ...platformorchestratorcp.RequestEditorFn) (*platformorchestratorcp.GetEnvironmentResponse, error) {
			return &platformorchestratorcp.GetEnvironmentResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: envUuid, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
			}, nil
		}).Times(1)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "some-ns"})
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Runner{StateStorageConfiguration: ssc},
	}, nil).Times(1)

	store := new(reliableoutbox.InMemoryStorage[*hstandardreliableoutbox.PendingEventMessage])
	mdb.EXPECT().InsertPendingEventMessages(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, m []*hstandardreliableoutbox.PendingEventMessage) ([]*hstandardreliableoutbox.PendingEventMessage, error) {
			store.Put(m)
			return m, nil
		}).Times(1)
	mdb.EXPECT().AsReliableOutboxStore().Return(store).Times(1)

	encodedManifest, _ := json.Marshal(DeploymentManifest{})
	encodedGraph, _ := json.Marshal(&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			platform_orchestrator_graph.ResourceCoordinate{Type: "thing", Class: "default", Id: "shared.thing"}: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{
				DefinitionId: "some-def", VersionId: "some-ver",
			}},
		},
	})
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Not(nil), "my-org", rollbackToId, model.GetModeDefault).
		Return(&model.DeploymentSummary{DeploymentEnvUuid: envUuid}, encodedManifest, nil, encodedGraph, nil)

	cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{
		PinnedModuleVersions: []string{"some-def@some-ver"},
		AreRulesIgnored:      true,
	}).
		Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &platformorchestratorcp.InternalModuleCatalogue{
				Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
					{
						ResourceType: "thing",
						Id:           "some-def", VersionId: "some-ver",
						ModuleSource: "git::ssh://git@github.com/tofu-org/tofu-registry.git",
						Rules:        []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
					},
				},
			},
		}, nil)

	mdb.EXPECT().CreateDeployment(gomock.Any(), gomock.Any(), "my-org", "my-project", "my-env", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, _, _, _ string, p model.CreateDeploymentParams) (*model.DeploymentSummary, error) {
			if g, err := graphs.FromJson(bytes.NewReader(p.Graph)); assert.NoError(t, err) {
				assert.Equal(t, []string{
					"type=thing,class=default,id=shared.thing",
				}, slices.Collect(func(yield func(string) bool) {
					for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
						yield(rc.String())
					}
				}))
				assert.Equal(t, encryptedOutputsRecipient, p.EncryptedOutputsRecipient.Must())
				assert.Equal(t, encryptedLogsRecipient, p.EncryptedLogsRecipient.Must())
				assert.Empty(t, g.Edges)
			}
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

			return &model.DeploymentSummary{
				OrgId:                     "my-org",
				ProjectId:                 "my-project",
				EnvId:                     "my-env",
				Id:                        depId,
				DeploymentEnvUuid:         envUuid,
				EncryptedOutputsRecipient: opt.Of(encryptedOutputsRecipient),
				EncryptedLogsRecipient:    opt.Of(encryptedLogsRecipient),
			}, nil
		})

	mdb.EXPECT().InitActiveResourcesFromGraph(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode:                      "rollback",
			RollbackToDeploymentId:    &rollbackToId,
			EncryptedOutputsRecipient: ref.Ref(encryptedOutputsRecipient),
			EncryptedLogsRecipient:    ref.Ref(encryptedLogsRecipient),
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment201JSONResponse{}, r, "unexpected %v", r)
	r201 := r.(CreateDeployment201JSONResponse)
	if assert.NotNil(t, r201) {
		assert.Equal(t, depId, r201.Id)
	}
}

func TestCreateDeployment_with_co_provisioned_resource(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	outputsKeyIdentity, _ := age.GenerateX25519Identity()
	logsKeyIdentity, _ := age.GenerateX25519Identity()

	encryptedOutputsRecipient := outputsKeyIdentity.Recipient().String()
	encryptedLogsRecipient := logsKeyIdentity.Recipient().String()

	depId, envUuid := uuid.New(), uuid.NewMD5(uuid.Nil, []byte("my-env"))

	mdb := s.Database.(*mockmodel.MockDatabaser)
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", "my-env").
		DoAndReturn(func(_ context.Context, _, _, _ string, _ ...platformorchestratorcp.RequestEditorFn) (*platformorchestratorcp.GetEnvironmentResponse, error) {
			return &platformorchestratorcp.GetEnvironmentResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: envUuid, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
			}, nil
		}).Times(1)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "some-ns"})
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Runner{StateStorageConfiguration: ssc},
	}, nil).Times(1)

	store := new(reliableoutbox.InMemoryStorage[*hstandardreliableoutbox.PendingEventMessage])
	mdb.EXPECT().InsertPendingEventMessages(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, m []*hstandardreliableoutbox.PendingEventMessage) ([]*hstandardreliableoutbox.PendingEventMessage, error) {
			store.Put(m)
			return m, nil
		}).Times(1)
	mdb.EXPECT().AsReliableOutboxStore().Return(store).Times(1)

	mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
		Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

	cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{}).
		Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &platformorchestratorcp.InternalModuleCatalogue{
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
			},
		}, nil)

	mdb.EXPECT().CreateDeployment(gomock.Any(), gomock.Any(), "my-org", "my-project", "my-env", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, _, _, _ string, p model.CreateDeploymentParams) (*model.DeploymentSummary, error) {
			if g, err := graphs.FromJson(bytes.NewReader(p.Graph)); assert.NoError(t, err) {
				assert.Equal(t, []string{
					"type=thing,class=default,id=workloads.one.first",
					"type=skyhook,class=default,id=workloads.one.first",
					"type=workload,class=default,id=one",
				}, slices.Collect(func(yield func(string) bool) {
					for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
						yield(rc.String())
					}
				}))
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
			}
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

			return &model.DeploymentSummary{
				OrgId:                     "my-org",
				ProjectId:                 "my-project",
				EnvId:                     "my-env",
				Id:                        depId,
				DeploymentEnvUuid:         envUuid,
				EncryptedOutputsRecipient: opt.Of(encryptedOutputsRecipient),
				EncryptedLogsRecipient:    opt.Of(encryptedLogsRecipient),
			}, nil
		})

	mdb.EXPECT().InitActiveResourcesFromGraph(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
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
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment201JSONResponse{}, r, "unexpected %v", r)
	r201 := r.(CreateDeployment201JSONResponse)
	if assert.NotNil(t, r201) {
		assert.Equal(t, depId, r201.Id)
	}
}

func TestCreateDeployment_with_inline_module_code(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	mdb := s.Database.(*mockmodel.MockDatabaser)
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _, envId string, _ ...platformorchestratorcp.RequestEditorFn) (*platformorchestratorcp.GetEnvironmentResponse, error) {
			return &platformorchestratorcp.GetEnvironmentResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200:      &platformorchestratorcp.Environment{Id: envId, Uuid: uuid.NewMD5(uuid.Nil, []byte(envId)), RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
			}, nil
		}).Times(1)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "some-ns"})
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Runner{StateStorageConfiguration: ssc},
	}, nil).Times(1)

	store := new(reliableoutbox.InMemoryStorage[*hstandardreliableoutbox.PendingEventMessage])
	mdb.EXPECT().InsertPendingEventMessages(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, m []*hstandardreliableoutbox.PendingEventMessage) ([]*hstandardreliableoutbox.PendingEventMessage, error) {
			store.Put(m)
			return m, nil
		}).Times(1)
	mdb.EXPECT().AsReliableOutboxStore().Return(store).Times(1)

	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))

	mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
		Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

	cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{}).
		Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &platformorchestratorcp.InternalModuleCatalogue{
				Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
					{
						ResourceType: "thing",
						Id:           "d1", VersionId: "v1",
						ModuleSourceCode: ref.Ref(`variable "in" {}`),
						Rules:            []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
					},
				},
			},
		}, nil)

	mdb.EXPECT().CreateDeployment(gomock.Any(), gomock.Any(), "my-org", "my-project", "my-env", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, _, _, _ string, p model.CreateDeploymentParams) (*model.DeploymentSummary, error) {
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

			return &model.DeploymentSummary{
				OrgId:             "my-org",
				ProjectId:         "my-project",
				EnvId:             "my-env",
				Id:                depId,
				DeploymentEnvUuid: envUuid,
			}, nil
		})

	mdb.EXPECT().InitActiveResourcesFromGraph(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {
						Resources: map[string]DeploymentManifestResource{
							"first": {Type: "thing"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment201JSONResponse{}, r, "unexpected %v", r)
	r201 := r.(CreateDeployment201JSONResponse)
	if assert.NotNil(t, r201) {
		assert.Equal(t, depId, r201.Id)
	}
}

func TestCreateDeployment_with_deprecated_variables(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	mdb := s.Database.(*mockmodel.MockDatabaser)
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _, envId string, _ ...platformorchestratorcp.RequestEditorFn) (*platformorchestratorcp.GetEnvironmentResponse, error) {
			return &platformorchestratorcp.GetEnvironmentResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200:      &platformorchestratorcp.Environment{Id: envId, Uuid: uuid.NewMD5(uuid.Nil, []byte(envId)), RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
			}, nil
		}).Times(1)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "some-ns"})
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Runner{StateStorageConfiguration: ssc},
	}, nil).Times(1)

	store := new(reliableoutbox.InMemoryStorage[*hstandardreliableoutbox.PendingEventMessage])
	mdb.EXPECT().InsertPendingEventMessages(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, m []*hstandardreliableoutbox.PendingEventMessage) ([]*hstandardreliableoutbox.PendingEventMessage, error) {
			store.Put(m)
			return m, nil
		}).Times(1)
	mdb.EXPECT().AsReliableOutboxStore().Return(store).Times(1)

	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))

	mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
		Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

	cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{}).
		Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &platformorchestratorcp.InternalModuleCatalogue{
				Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
					{
						ResourceType: "thing",
						Id:           "d1", VersionId: "v1",
						ModuleSourceCode: ref.Ref(`variable "in" {}`),
						Rules:            []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
					},
				},
			},
		}, nil)

	mdb.EXPECT().CreateDeployment(gomock.Any(), gomock.Any(), "my-org", "my-project", "my-env", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, _, _, _ string, p model.CreateDeploymentParams) (*model.DeploymentSummary, error) {
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

			return &model.DeploymentSummary{
				OrgId:             "my-org",
				ProjectId:         "my-project",
				EnvId:             "my-env",
				Id:                depId,
				DeploymentEnvUuid: envUuid,
			}, nil
		})

	mdb.EXPECT().InitActiveResourcesFromGraph(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
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
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment201JSONResponse{}, r, "unexpected %v", r)
	r201 := r.(CreateDeployment201JSONResponse)
	if assert.NotNil(t, r201) {
		assert.Equal(t, depId, r201.Id)
	}
}

func TestCreateDeployment_success_graph_test_mode(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	mdb := s.Database.(*mockmodel.MockDatabaser)
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _, envId string, _ ...platformorchestratorcp.RequestEditorFn) (*platformorchestratorcp.GetEnvironmentResponse, error) {
			return &platformorchestratorcp.GetEnvironmentResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200: &platformorchestratorcp.Environment{
					Id:       envId,
					Uuid:     uuid.NewMD5(uuid.Nil, []byte(envId)),
					RunnerId: ref.Ref("my-runner"),
					Status:   platformorchestratorcp.EnvironmentStatusActive,
				},
			}, nil
		}).Times(1)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "some-ns"})
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200: &platformorchestratorcp.Runner{
			StateStorageConfiguration: ssc,
		},
	}, nil).Times(1)

	store := new(reliableoutbox.InMemoryStorage[*hstandardreliableoutbox.PendingEventMessage])
	mdb.EXPECT().InsertPendingEventMessages(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, m []*hstandardreliableoutbox.PendingEventMessage) ([]*hstandardreliableoutbox.PendingEventMessage, error) {
			store.Put(m)
			return m, nil
		}).Times(1)
	mdb.EXPECT().AsReliableOutboxStore().Return(store).Times(1)
	mdb.EXPECT().InitActiveResourcesFromGraph(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))

	mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
		Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

	cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{}).
		Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &platformorchestratorcp.InternalModuleCatalogue{
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
			},
		}, nil)

	mdb.EXPECT().CreateDeployment(gomock.Any(), gomock.Any(), "my-org", "my-project", "my-env", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, _, _, _ string, p model.CreateDeploymentParams) (*model.DeploymentSummary, error) {
			assert.Equal(t, model.DeploymentModeDeploy, p.Mode)
			if g, err := graphs.FromJson(bytes.NewReader(p.Graph)); assert.NoError(t, err) {
				assert.Equal(t, []string{
					"type=postgres,class=default,id=workloads.one.first",
					"type=workload,class=default,id=one",
				}, slices.Collect(func(yield func(string) bool) {
					for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
						yield(rc.String())
					}
				}))
			}

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
  thing  = "${var.XYZ} ${"my-env"}"
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

			return &model.DeploymentSummary{
				OrgId:             "my-org",
				ProjectId:         "my-project",
				EnvId:             "my-env",
				Id:                depId,
				DeploymentEnvUuid: envUuid,
				Status:            model.DeploymentStatusExecuting,
			}, nil
		})

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
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
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment201JSONResponse{}, r, "unexpected %v", r)
	r201 := r.(CreateDeployment201JSONResponse)
	if assert.NotNil(t, r201) {
		assert.Equal(t, depId, r201.Id)
	}

	// assert that we published the message
	if rec := s.RabbitMqPublisher.(*hrabbitmq.NoOpPublisher).GetRecorded(); assert.Len(t, rec, 1) {
		assert.Equal(t, string(genevents.IoPlatformOrchestratorDeploymentCreated), rec[0].Keys[0])
		assert.JSONEq(t, `{
  "specversion": "1.0",
  "type": "io.platform-orchestrator.deployment.created",
  "time": "0001-01-01T00:00:00Z",
  "data": {
    "org_id": "my-org", "project_id": "my-project", "env_id": "my-env",
    "deployment_id": "`+depId.String()+`",
    "env_uuid": "`+envUuid.String()+`",
    "revision": 0,
    "status": "executing"
  }
}`, string(rec[0].Data))
	}

	// assert that the store is now empty
	if p, _, err := store.LoadPage(t.Context()); assert.NoError(t, err) {
		assert.Empty(t, p)
	}
}

func TestCreateDeployment_fail_graph_error(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	mdb := s.Database.(*mockmodel.MockDatabaser)

	mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
		Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{}).
		Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &platformorchestratorcp.InternalModuleCatalogue{
				Modules:   []platformorchestratorcp.InternalModuleCatalogueModule{},
				Providers: []platformorchestratorcp.ModuleProvider{},
			},
		}, nil)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", gomock.Any()).Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
	}, nil).Times(1)
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Runner{},
	}, nil)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {
						Resources: map[string]DeploymentManifestResource{
							"first": {Type: "postgres"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment400JSONResponse{}, r, "unexpected %v", r)
	r400 := r.(CreateDeployment400JSONResponse)
	if assert.NotNil(t, r400) {
		assert.Equal(t, "graph contains 1 errors:\n\ttype=postgres,class=default,id=workloads.one.first: no module definition matches this resource\n", r400.Message)
	}
}

func TestCreateDeployment_fail_missing_provider(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	mdb := s.Database.(*mockmodel.MockDatabaser)

	mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
		Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{}).
		Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &platformorchestratorcp.InternalModuleCatalogue{
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
			},
		}, nil)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", gomock.Any()).Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
	}, nil).Times(1)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "some-ns"})
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200: &platformorchestratorcp.Runner{
			StateStorageConfiguration: ssc,
		},
	}, nil)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {
						Resources: map[string]DeploymentManifestResource{
							"first": {Type: "postgres"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment400JSONResponse{}, r, "unexpected %v", r)
	r400 := r.(CreateDeployment400JSONResponse)
	if assert.NotNil(t, r400) {
		assert.Equal(t, "unknown provider 'aws.default' used by module definition d1@v1 for type=postgres,class=default,id=workloads.one.first", r400.Message)
	}
}

func TestCreateDeployment_catalogue_fail(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	mdb := s.Database.(*mockmodel.MockDatabaser)

	mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
		Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{}).
		Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusConflict},
			JSON409: &platformorchestratorcp.Error{
				Error:   string(errcodes.PinnedModuleMissingProvider),
				Message: "conflict",
				Details: &map[string]interface{}{
					"missing_providers": []interface{}{"example.default"},
				},
			},
		}, nil)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", gomock.Any()).Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
	}, nil).Times(1)
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Runner{},
	}, nil)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {
						Resources: map[string]DeploymentManifestResource{
							"first": {Type: "postgres"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment409JSONResponse{}, r, "unexpected %v", r)
	r409 := r.(CreateDeployment409JSONResponse)
	if assert.NotNil(t, r409) {
		assert.Equal(t, "MOD-001: existing module(s) depend on providers that no longer exist: [example.default]", r409.Message)
	}
}

func TestCreateDeployment_fail_env_deleting(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", gomock.Any()).Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusDeleting},
	}, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {
						Resources: map[string]DeploymentManifestResource{
							"first": {Type: "postgres"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment409JSONResponse{}, r, "unexpected %v", r)
	r2 := r.(CreateDeployment409JSONResponse)
	if assert.NotNil(t, r2) {
		assert.Equal(t, fmt.Sprintf("environment is in status '%s'", platformorchestratorcp.EnvironmentStatusDeleting), r2.Message)
	}
}

func TestCreateDeployment_age_keys(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", "my-env").Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, Status: platformorchestratorcp.EnvironmentStatusActive},
	}, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {},
				},
			},
			EncryptedOutputsRecipient: ref.Ref("bananas"),
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment400JSONResponse{}, r, "unexpected %v", r)
	r400 := r.(CreateDeployment400JSONResponse)
	if assert.NotNil(t, r400) {
		assert.Equal(t, "encrypted_outputs_recipient: failed to parse 'bananas' as age public key", r400.Message)
	}
}

func TestCreateDeployment_authorization_fallback_to_org(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	mdb := s.Database.(*mockmodel.MockDatabaser)
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)

	depId, envUuid := uuid.New(), uuid.NewMD5(uuid.Nil, []byte("my-env"))
	outputsKeyIdentity, _ := age.GenerateX25519Identity()
	logsKeyIdentity, _ := age.GenerateX25519Identity()
	encryptedOutputsRecipient := outputsKeyIdentity.Recipient().String()
	encryptedLogsRecipient := logsKeyIdentity.Recipient().String()

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)

	// First call: CanWriteEnvironmentCheck fails with 403
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
		JSON403: &platformorchestratoriam.Error{
			Error:   "HTTP-403",
			Message: "user cannot write to environment",
			Details: &map[string]interface{}{
				"message": "user cannot write to environment",
			},
		},
	}, nil).Times(1)

	// Second call (fallback): CanWriteOrgCheck succeeds with 204
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteOrgCheck("my-org")},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", "my-env").
		Return(&platformorchestratorcp.GetEnvironmentResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: envUuid, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
		}, nil).Times(1)

	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "some-ns"})
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Runner{StateStorageConfiguration: ssc},
	}, nil).Times(1)

	store := new(reliableoutbox.InMemoryStorage[*hstandardreliableoutbox.PendingEventMessage])
	mdb.EXPECT().InsertPendingEventMessages(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, m []*hstandardreliableoutbox.PendingEventMessage) ([]*hstandardreliableoutbox.PendingEventMessage, error) {
			store.Put(m)
			return m, nil
		}).Times(1)
	mdb.EXPECT().AsReliableOutboxStore().Return(store).Times(1)

	mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", model.GetLastDeploymentParams{StateChangeOnly: true}).
		Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

	cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{}).
		Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &platformorchestratorcp.InternalModuleCatalogue{
				Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
					{
						ResourceType: "postgres",
						Id:           "d1", VersionId: "v1",
						ModuleSource: "registry.tofu.com/ns/prov/aws@v1",
						Rules:        []platformorchestratorcp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
					},
				},
			},
		}, nil)

	mdb.EXPECT().CreateDeployment(gomock.Any(), gomock.Any(), "my-org", "my-project", "my-env", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ model.Tx, _, _, _ string, p model.CreateDeploymentParams) (*model.DeploymentSummary, error) {
			return &model.DeploymentSummary{
				OrgId:                     "my-org",
				ProjectId:                 "my-project",
				EnvId:                     "my-env",
				Id:                        depId,
				DeploymentEnvUuid:         envUuid,
				EncryptedOutputsRecipient: opt.Of(encryptedOutputsRecipient),
				EncryptedLogsRecipient:    opt.Of(encryptedLogsRecipient),
			}, nil
		})

	mdb.EXPECT().InitActiveResourcesFromGraph(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
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
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, CreateDeployment201JSONResponse{}, r, "unexpected %v", r)
	r201 := r.(CreateDeployment201JSONResponse)
	if assert.NotNil(t, r201) {
		assert.Equal(t, depId, r201.Id)
	}
}

func TestCreateDeployment_authorization_failure(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)

	// First call: CanWriteEnvironmentCheck fails with 403
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
		JSON403: &platformorchestratoriam.Error{
			Error:   "HTTP-403",
			Message: "user cannot write to environment",
			Details: &map[string]interface{}{
				"message": "user cannot write to environment",
			},
		},
	}, nil).Times(1)

	// Second call (fallback): CanWriteOrgCheck also fails with 403
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteOrgCheck("my-org")},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
		JSON403: &platformorchestratoriam.Error{
			Error:   "HTTP-403",
			Message: "user cannot write to organization",
			Details: &map[string]interface{}{
				"message": "user cannot write to organization",
			},
		},
	}, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", "my-env").
		Return(&platformorchestratorcp.GetEnvironmentResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: envUuid, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
		}, nil).Times(1)

	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {
						Resources: map[string]DeploymentManifestResource{
							"first": {Type: "postgres"},
						},
					},
				},
			},
		},
	})
	// Authorization failure should return an error, not a response
	require.Error(t, err)
	require.Nil(t, r)
	// Check it's a PlatformOrchestratorError with status 403
	var herr *herrors.PlatformOrchestratorError
	if assert.ErrorAs(t, err, &herr) {
		assert.Equal(t, http.StatusForbidden, herr.StatusCode)
		assert.NotNil(t, herr.Details)
		assert.Contains(t, herr.Details, "message")
	}
}

func TestWaitForDeploymentComplete_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(nil, nil, nil, nil, model.NewErrNotFound("deployment not found"))

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete404JSONResponse{}, r) {
		r404 := r.(WaitForDeploymentComplete404JSONResponse)
		assert.Equal(t, "deployment not found", r404.Message)
	}
}

func TestWaitForDeploymentComplete_already_complete(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(&model.DeploymentSummary{
		Status:            model.DeploymentStatusSucceeded,
		DeploymentEnvUuid: envUuid,
	}, nil, nil, nil, nil).Times(2)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete200JSONResponse{}, r) {
		r200 := r.(WaitForDeploymentComplete200JSONResponse)
		assert.Equal(t, "succeeded", r200.Status)
	}
}

func TestWaitForDeploymentComplete_req_timeout(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).DoAndReturn(func(_ context.Context, _ model.Tx, _ string, _ uuid.UUID, _ model.GetMode) (*model.DeploymentSummary, model.EncodedDeploymentManifest, []byte, model.EncodedDeploymentGraph, error) {
		go cancel()
		return &model.DeploymentSummary{
			Status:            model.DeploymentStatusExecuting,
			DeploymentEnvUuid: envUuid,
		}, nil, nil, nil, nil
	}).Times(2)

	ctx = context.WithValue(ctx, hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete408JSONResponse{}, r) {
		r408 := r.(WaitForDeploymentComplete408JSONResponse)
		assert.Equal(t, "deployment has not completed yet", r408.Message)
	}
}

func TestWaitForDeploymentComplete_event_trigger(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	counter := atomic.NewInt32(0)

	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).DoAndReturn(func(_ context.Context, _ model.Tx, _ string, _ uuid.UUID, _ model.GetMode) (*model.DeploymentSummary, model.EncodedDeploymentManifest, []byte, model.EncodedDeploymentGraph, error) {
		if counter.Inc() == 1 {
			go s.DeploymentCompletedHooks.Notify(completionhooks.DeploymentOrgAndId{OrgId: "my-org", DeploymentId: depId.String()}, struct{}{})
			return &model.DeploymentSummary{
				Status:            model.DeploymentStatusExecuting,
				DeploymentEnvUuid: envUuid,
			}, nil, nil, nil, nil
		}
		return &model.DeploymentSummary{
			Status:            model.DeploymentStatusSucceeded,
			DeploymentEnvUuid: envUuid,
		}, nil, nil, nil, nil
	}).Times(2)

	ctx = context.WithValue(ctx, hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete200JSONResponse{}, r) {
		r200 := r.(WaitForDeploymentComplete200JSONResponse)
		assert.Equal(t, "succeeded", r200.Status)
	}
}

func TestWaitForDeploymentComplete_max_waiters(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)

	s.DeploymentCompletedHooks = &completionhooks.CompletionHooks[completionhooks.DeploymentOrgAndId, struct{}]{MaximumWaitersPerHandle: 1}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	counter := atomic.NewInt32(0)

	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).DoAndReturn(func(_ context.Context, _ model.Tx, _ string, _ uuid.UUID, _ model.GetMode) (*model.DeploymentSummary, model.EncodedDeploymentManifest, []byte, model.EncodedDeploymentGraph, error) {
		if counter.Inc() == 2 {
			go func() {
				_, fin = s.DeploymentCompletedHooks.AddWaiter(completionhooks.DeploymentOrgAndId{OrgId: "my-org", DeploymentId: depId.String()})
				defer fin()
			}()
		}
		return &model.DeploymentSummary{
			Status:            model.DeploymentStatusExecuting,
			DeploymentEnvUuid: envUuid,
		}, nil, nil, nil, nil
	}).Times(3)

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx = context.WithValue(ctx, hecho.ContextKeyUserID, userId.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete408JSONResponse{}, r) {
		r408 := r.(WaitForDeploymentComplete408JSONResponse)
		assert.Equal(t, "deployment has not completed yet", r408.Message)
	}
}

func TestWaitForDeploymentComplete_authorization_fallback_to_org(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)

	// First call: CanWriteEnvironmentCheck fails with 403
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
		JSON403: &platformorchestratoriam.Error{
			Error:   "HTTP-403",
			Message: "user cannot write to environment",
			Details: &map[string]interface{}{
				"message": "user cannot write to environment",
			},
		},
	}, nil).Times(1)

	// Second call (fallback): CanWriteOrgCheck succeeds with 204
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteOrgCheck("my-org")},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)

	// First GetDeployment call for authorization check
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(&model.DeploymentSummary{
		Status:            model.DeploymentStatusSucceeded,
		DeploymentEnvUuid: envUuid,
		ProjectId:         "my-project",
		EnvId:             "my-env",
	}, nil, nil, nil, nil).Times(1)

	// Second GetDeployment call in the wait loop
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(&model.DeploymentSummary{
		Status:            model.DeploymentStatusSucceeded,
		DeploymentEnvUuid: envUuid,
		ProjectId:         "my-project",
		EnvId:             "my-env",
	}, nil, nil, nil, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	if assert.NoError(t, err) && assert.IsType(t, WaitForDeploymentComplete200JSONResponse{}, r) {
		r200 := r.(WaitForDeploymentComplete200JSONResponse)
		assert.Equal(t, "succeeded", r200.Status)
	}
}

func TestWaitForDeploymentComplete_authorization_failure(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	depId := uuid.New()
	envUuid := uuid.NewMD5(uuid.Nil, []byte("my-env"))
	mdb := s.Database.(*mockmodel.MockDatabaser)

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)

	// First call: CanWriteEnvironmentCheck fails with 403
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteEnvironmentCheck(envUuid)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
		JSON403: &platformorchestratoriam.Error{
			Error:   "HTTP-403",
			Message: "user cannot write to environment",
			Details: &map[string]interface{}{
				"message": "user cannot write to environment",
			},
		},
	}, nil).Times(1)

	// Second call (fallback): CanWriteOrgCheck also fails with 403
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanWriteOrgCheck("my-org")},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
		JSON403: &platformorchestratoriam.Error{
			Error:   "HTTP-403",
			Message: "user cannot write to organization",
			Details: &map[string]interface{}{
				"message": "user cannot write to organization",
			},
		},
	}, nil).Times(1)

	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(&model.DeploymentSummary{
		Status:            model.DeploymentStatusSucceeded,
		DeploymentEnvUuid: envUuid,
		ProjectId:         "my-project",
		EnvId:             "my-env",
	}, nil, nil, nil, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
	r, err := s.WaitForDeploymentComplete(ctx, WaitForDeploymentCompleteRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})
	// Authorization failure should return an error, not a response
	require.Error(t, err)
	require.Nil(t, r)
	// Check it's a PlatformOrchestratorError with status 403
	var herr *herrors.PlatformOrchestratorError
	if assert.ErrorAs(t, err, &herr) {
		assert.Equal(t, http.StatusForbidden, herr.StatusCode)
		assert.NotNil(t, herr.Details)
		assert.Contains(t, herr.Details, "message")
	}
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
			_, s, fin := MockServer(t)
			defer fin()

			mdb := s.Database.(*mockmodel.MockDatabaser)

			mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", gomock.Any()).
				Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

			cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
			cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{}).
				Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
					HTTPResponse: &http.Response{StatusCode: http.StatusOK},
					JSON200: &platformorchestratorcp.InternalModuleCatalogue{
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
					},
				}, nil)
			cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", gomock.Any()).Return(&platformorchestratorcp.GetEnvironmentResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
			}, nil).Times(1)
			var ssc platformorchestratorcp.StateStorageConfiguration
			_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "some-ns"})
			cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200: &platformorchestratorcp.Runner{
					StateStorageConfiguration: ssc,
				},
			}, nil).Times(1)

			ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
			r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
				OrgId: "my-org",
				Body: &CreateDeploymentJSONRequestBody{
					ProjectId: "my-project", EnvId: "my-env",
					Mode: "deploy",
					Manifest: &DeploymentManifest{
						Workloads: map[string]DeploymentManifestWorkload{
							"one": {
								Resources: map[string]DeploymentManifestResource{
									"first": {Type: "postgres"},
								},
							},
						},
					},
				},
			})
			require.NoError(t, err)

			r400 := r.(CreateDeployment400JSONResponse)
			if assert.NotNil(t, r400) {
				assert.Equal(t, test.err, r400.Message)
			}
		})
	}
}

func TestCreateDeployment_invalid_module_param(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	mdb := s.Database.(*mockmodel.MockDatabaser)

	mdb.EXPECT().GetLastDeployment(gomock.Any(), gomock.Not(nil), "my-org", "my-project", "my-env", gomock.Any()).
		Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GenerateInternalModuleCatalogueWithResponse(gomock.Any(), "my-org", "my-project", "my-env", platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{}).
		Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &platformorchestratorcp.InternalModuleCatalogue{
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
			},
		}, nil)
	cpClient.EXPECT().GetEnvironmentWithResponse(gomock.Any(), "my-org", "my-project", gomock.Any()).Return(&platformorchestratorcp.GetEnvironmentResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &platformorchestratorcp.Environment{Id: "my-env", Uuid: uuid.Nil, RunnerId: ref.Ref("my-runner"), Status: platformorchestratorcp.EnvironmentStatusActive},
	}, nil).Times(1)
	var ssc platformorchestratorcp.StateStorageConfiguration
	_ = ssc.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "some-ns"})
	cpClient.EXPECT().GetRunnerWithResponse(gomock.Any(), "my-org", "my-runner").Return(&platformorchestratorcp.GetRunnerResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200: &platformorchestratorcp.Runner{
			StateStorageConfiguration: ssc,
		},
	}, nil).Times(1)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.CreateDeployment(ctx, CreateDeploymentRequestObject{
		OrgId: "my-org",
		Body: &CreateDeploymentJSONRequestBody{
			ProjectId: "my-project", EnvId: "my-env",
			Mode: "deploy",
			Manifest: &DeploymentManifest{
				Workloads: map[string]DeploymentManifestWorkload{
					"one": {
						Resources: map[string]DeploymentManifestResource{
							"t": {Type: "thing", Params: map[string]interface{}{"fruit": "cat"}},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	r400 := r.(CreateDeployment400JSONResponse)
	if assert.NotNil(t, r400) {
		assert.Equal(t, "node 'type=thing,class=default,id=workloads.one.t': uses module d1@v1 which requires string param 'animal' to be set", r400.Message)
	}
}

func TestGetDeployment_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanReadOrgCheck("my-org")},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	mdb := s.Database.(*mockmodel.MockDatabaser)
	deploymentId := uuid.New()
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", deploymentId, model.GetModeDefault).
		Return(nil, nil, nil, nil, model.NewErrNotFound("deployment not found"))

	r, err := s.GetDeployment(ctx, GetDeploymentRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeployment404JSONResponse{}, r, "unexpected %v", r)
	r404 := r.(GetDeployment404JSONResponse)
	if assert.NotNil(t, r404) {
		assert.Equal(t, "deployment not found", r404.Message)
	}
}

func TestGetDeploymentBundle(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	depId := uuid.New()

	rawGraph, _ := graphs.ToJson(&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			{Type: "workload", Class: "default", Id: "workloads.one"}: {
				ModuleConfiguration: nil,
			},
			{Type: "thing", Class: "default", Id: "workloads.one.a"}: {
				ModuleConfiguration: &graphs.GraphNodeModuleConfig{
					DefinitionId: "thing1", VersionId: "v1",
				},
			},
			{Type: "thing", Class: "default", Id: "workloads.one.b"}: {
				ModuleConfiguration: &graphs.GraphNodeModuleConfig{
					DefinitionId: "thing1", VersionId: "v2",
					HasInlineSource: true,
				},
			},
		},
	})

	s.Database.(*mockmodel.MockDatabaser).EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).Return(&model.DeploymentSummary{
		ProjectId: "my-project", EnvId: "my-env",
	}, nil, model.RawTofu(`some-tofu`), rawGraph, nil)

	s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface).EXPECT().GenerateInternalModuleCatalogueWithResponse(
		gomock.Any(), "my-org", "my-project", "my-env",
		platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{AreRulesIgnored: true, PinnedModuleVersions: []string{"thing1@v2"}},
	).Return(&platformorchestratorcp.GenerateInternalModuleCatalogueResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200: &platformorchestratorcp.InternalModuleCatalogue{
			Modules: []platformorchestratorcp.InternalModuleCatalogueModule{
				{
					ResourceType: "thing",
					Id:           "thing1", VersionId: "v2",
					ModuleSourceCode: ref.Ref("some-module-source"),
				},
			},
		},
	}, nil)

	r, err := s.GetDeploymentBundle(t.Context(), GetDeploymentBundleRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
		Params: GetDeploymentBundleParams{
			XDeploymentToken: util.GenerateHashedRunnerToken(s.RunnerTokenSalt, "my-org", depId.String()),
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentBundle200ApplicationxGzipResponse{}, r, "%+v", r)

	gr, err := gzip.NewReader(r.(GetDeploymentBundle200ApplicationxGzipResponse).Body)
	require.NoError(t, err)
	tr := tar.NewReader(gr)

	var fileTree []string
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of tar archive
		}
		require.NoError(t, err)
		fileTree = append(fileTree, header.Name)
	}
	assert.Equal(t, []string{
		"main.tf",
		"modules/thing1/v2/",
		"modules/thing1/v2/main.tf",
	}, fileTree)
}

func TestGetDeploymentLogs_deployment_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanReadOrgCheck("my-org")},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(nil, nil, nil, nil, model.NewErrNotFound("not found"))

	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: uuid.New(),
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs404JSONResponse{}, r, "%+v", r)
}

func TestGetDeploymentLogs_logs_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	deploymentEnvUuid := uuid.New()

	deploymentId := uuid.New()
	s.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		assert.Condition(t, func() bool {
			return filename == deploymentEnvUuid.String()+"/"+deploymentId.String() || filename == "my-org/"+deploymentId.String()
		}, "filename is not correct")
		return nil, storage.ErrObjectNotExist
	}

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).
		Return(&model.DeploymentSummary{Id: deploymentId, DeploymentEnvUuid: deploymentEnvUuid}, nil, nil, nil, nil)

	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs404JSONResponse{}, r, "%+v", r)
}

func TestGetDeploymentLogs_logs_are_stored_in_deprecated_location(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	deploymentEnvUuid := uuid.New()

	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	deploymentId := uuid.New()
	s.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		if filename == deploymentEnvUuid.String()+"/"+deploymentId.String() {
			return nil, storage.ErrObjectNotExist
		} else if filename == "my-org/"+deploymentId.String() {
			var encryptedData = &bytes.Buffer{}
			w, err := age.Encrypt(encryptedData, id.Recipient())
			require.NoError(t, err)
			_, err = w.Write([]byte(sampleLogs))
			require.NoError(t, err)
			require.NoError(t, w.Close())
			return io.NopCloser(strings.NewReader(base64.StdEncoding.EncodeToString(encryptedData.Bytes()))), nil
		}
		assert.Fail(t, "filename is not correct", filename)
		return nil, errors.New("unexpected filename")
	}

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).
		Return(&model.DeploymentSummary{Id: deploymentId, DeploymentEnvUuid: deploymentEnvUuid}, nil, nil, nil, nil)

	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs200TextResponse{}, r, "%+v", r)
}

func TestGetDeploymentLogs_no_encryption_key(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	deploymentEnvUuid := uuid.New()

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	deploymentId := uuid.New()
	s.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		assert.Equal(t, deploymentEnvUuid.String()+"/"+deploymentId.String(), filename)
		var encryptedData = &bytes.Buffer{}
		w, err := age.Encrypt(encryptedData, id.Recipient())
		require.NoError(t, err)
		_, err = w.Write([]byte(sampleLogs))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		return io.NopCloser(strings.NewReader(base64.StdEncoding.EncodeToString(encryptedData.Bytes()))), nil
	}

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(&model.DeploymentSummary{Id: deploymentId, DeploymentEnvUuid: deploymentEnvUuid}, nil, nil, nil, nil)

	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs200TextResponse{}, r, "%+v", r)
	require.NotEqual(t, sampleLogs, r.(GetDeploymentLogs200TextResponse).Body)
}

func TestGetDeploymentLogs_improper_key(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	deploymentEnvUuid := uuid.New()

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	deploymentId := uuid.New()
	s.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		assert.Equal(t, deploymentEnvUuid.String()+"/"+deploymentId.String(), filename)
		sampleLogs := sampleLogs
		var encryptedData = &bytes.Buffer{}
		w, err := age.Encrypt(encryptedData, id.Recipient())
		require.NoError(t, err)
		_, err = w.Write([]byte(sampleLogs))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		return io.NopCloser(strings.NewReader(base64.StdEncoding.EncodeToString(encryptedData.Bytes()))), nil
	}

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(&model.DeploymentSummary{Id: deploymentId, DeploymentEnvUuid: deploymentEnvUuid}, nil, nil, nil, nil)

	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
		Params: GetDeploymentLogsParams{
			DecryptKey: ref.Ref("ageDumbKey"),
		},
	})

	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs400JSONResponse{}, r, "%+v", r)
	require.Equal(t, "the supplied key is not a valid 'age' private key", r.(GetDeploymentLogs400JSONResponse).Message)
}

func TestGetDeploymentLogs_private_key_not_matching(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	deploymentEnvUuid := uuid.New()

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	deploymentId := uuid.New()
	s.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		assert.Equal(t, deploymentEnvUuid.String()+"/"+deploymentId.String(), filename)
		sampleLogs := sampleLogs
		var encryptedData = &bytes.Buffer{}
		w, err := age.Encrypt(encryptedData, id.Recipient())
		require.NoError(t, err)
		_, err = w.Write([]byte(sampleLogs))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		return io.NopCloser(strings.NewReader(base64.StdEncoding.EncodeToString(encryptedData.Bytes()))), nil
	}

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(&model.DeploymentSummary{Id: deploymentId, DeploymentEnvUuid: deploymentEnvUuid}, nil, nil, nil, nil)
	anotherId, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
		Params: GetDeploymentLogsParams{
			DecryptKey: ref.Ref(anotherId.String()),
		},
	})

	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs400JSONResponse{}, r, "%+v", r)
	require.Contains(t, r.(GetDeploymentLogs400JSONResponse).Message, "logs can't be decrypted with the supplied key")
}

func TestGetDeploymentLogs_valid_key(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	deploymentEnvUuid := uuid.New()

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	deploymentId := uuid.New()
	s.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		assert.Equal(t, deploymentEnvUuid.String()+"/"+deploymentId.String(), filename)
		sampleLogs := sampleLogs
		var encryptedData = &bytes.Buffer{}
		w, err := age.Encrypt(encryptedData, id.Recipient())
		require.NoError(t, err)
		_, err = w.Write([]byte(sampleLogs))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		return io.NopCloser(strings.NewReader(base64.StdEncoding.EncodeToString(encryptedData.Bytes()))), nil
	}

	mdb := s.Database.(*mockmodel.MockDatabaser)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(&model.DeploymentSummary{Id: deploymentId, DeploymentEnvUuid: deploymentEnvUuid}, nil, nil, nil, nil)

	r, err := s.GetDeploymentLogs(ctx, GetDeploymentLogsRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
		Params: GetDeploymentLogsParams{
			DecryptKey: ref.Ref(id.String()),
		},
	})
	require.NoError(t, err)
	require.IsTypef(t, GetDeploymentLogs200TextResponse{}, r, "%+v", r)
	require.Equal(t, sampleLogs, r.(GetDeploymentLogs200TextResponse).Body)
}

func TestGetDeploymentOutputs(t *testing.T) {
	deploymentId := uuid.New()

	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		model    model.DeploymentSummary
		expected GetDeploymentEncryptedOutputsResponseObject
	}{
		{name: "success plan", model: model.DeploymentSummary{Mode: "plan_only", CompletedAt: opt.Of(time.Now()), Status: "succeeded", EncryptedOutputsRecipient: opt.Of(id.Recipient().String())}, expected: GetDeploymentEncryptedOutputs200JSONResponse{Raw: "encrypted-outputs"}},
		{name: "success deploy", model: model.DeploymentSummary{Mode: "deploy", CompletedAt: opt.Of(time.Now()), Status: "succeeded", EncryptedOutputsRecipient: opt.Of(id.Recipient().String())}, expected: GetDeploymentEncryptedOutputs200JSONResponse{Raw: "encrypted-outputs"}},
		{name: "fail destroy", model: model.DeploymentSummary{Mode: "destroy", CompletedAt: opt.Of(time.Now()), Status: "succeeded", EncryptedOutputsRecipient: opt.Of(id.Recipient().String())}, expected: GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404Response("outputs are not captured for deployments with mode 'destroy'")}},
		{name: "fail not completed", model: model.DeploymentSummary{Mode: "deploy", Status: "executing", EncryptedOutputsRecipient: opt.Of(id.Recipient().String())}, expected: GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404Response("deployment '00000000-0000-0000-0000-000000000000' has not completed yet")}},
		{name: "fail not succeeded", model: model.DeploymentSummary{Mode: "deploy", CompletedAt: opt.Of(time.Now()), Status: "failed", EncryptedOutputsRecipient: opt.Of(id.Recipient().String())}, expected: GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404Response("outputs are not captured for deployments with status 'failed'")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, s, fin := MockServer(t)
			defer fin()

			ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())

			mdb := s.Database.(*mockmodel.MockDatabaser)
			mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Any(), "my-org", gomock.Any(), model.GetModeDefault).Return(&tc.model, nil, nil, nil, nil)
			mdb.EXPECT().GetDeploymentEncryptedOutputs(gomock.Any(), gomock.Any(), "my-org", deploymentId).Return("encrypted-outputs", nil).Times(1)

			r, err := s.GetDeploymentEncryptedOutputs(ctx, GetDeploymentEncryptedOutputsRequestObject{
				OrgId:        "my-org",
				DeploymentId: deploymentId,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.expected, r)
		})
	}
}

func TestCalculateDeploymentDiff_specific(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	fromDepId := uuid.New()
	toDepEnvUuid := uuid.New()
	toDepId := uuid.New()

	mdb := s.Database.(*mockmodel.MockDatabaser)
	encodedFromManifest, _ := json.Marshal(&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
		{Type: "a", Class: "default", Id: "x"}: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"}},
		{Type: "a", Class: "default", Id: "y"}: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"}},
	}})
	encodedToManifest, _ := json.Marshal(&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
		{Type: "a", Class: "default", Id: "y"}: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "2"}},
		{Type: "a", Class: "default", Id: "z"}: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "d", VersionId: "1"}},
	}})

	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Nil(), "my-org", toDepId, model.GetModeDefault).Return(&model.DeploymentSummary{DeploymentEnvUuid: toDepEnvUuid, Id: toDepId}, nil, nil, encodedToManifest, nil)
	mdb.EXPECT().GetDeployment(gomock.Any(), gomock.Nil(), "my-org", fromDepId, model.GetModeDefault).Return(&model.DeploymentSummary{DeploymentEnvUuid: toDepEnvUuid, Id: fromDepId}, nil, nil, encodedFromManifest, nil)

	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	r, err := s.CalculateDeploymentDiff(ctx, CalculateDeploymentDiffRequestObject{OrgId: "my-org", DeploymentId: toDepId, Params: CalculateDeploymentDiffParams{FromDeploymentId: &fromDepId}})
	require.NoError(t, err)
	require.Equal(t, CalculateDeploymentDiff200JSONResponse{
		FromDeploymentId: &fromDepId,
		ToDeploymentId:   &toDepId,
		NumRemoved:       1,
		NumChanged:       1,
		NumAdded:         1,
		Changes: []DeploymentDiffChange{
			{Id: util.GenerateNodeHash(toDepEnvUuid, "a", "default", "y"), Resource: "a.default@y", Type: DeploymentDiffChangeTypeModuleChanged, Summary: "module changed from d@1 to d@2"},
			{Id: util.GenerateNodeHash(toDepEnvUuid, "a", "default", "z"), Resource: "a.default@z", Type: DeploymentDiffChangeTypeAdded, Summary: "add resource using module d@1"},
			{Id: util.GenerateNodeHash(toDepEnvUuid, "a", "default", "x"), Resource: "a.default@x", Type: DeploymentDiffChangeTypeRemoved, Summary: "remove resource using module d@1"},
		},
	}, r)
}

func TestEnrichDeploymentError_ModuleEnrichment(t *testing.T) {
	tests := []struct {
		name              string
		entityId          string
		entityVersion     string
		graphNodes        map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]
		expectedModuleId  string
		expectedModuleVer string
		shouldEnrich      bool
	}{
		{
			name:              "enriches error when entity_id and entity_version are both provided",
			entityId:          "postgres_default_mydb_direct",
			entityVersion:     "v2.5.0",
			graphNodes:        map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{},
			expectedModuleId:  "postgres_default_mydb_direct",
			expectedModuleVer: "v2.5.0",
			shouldEnrich:      true,
		},
		{
			name:              "enriches error with entity_version directly without graph lookup",
			entityId:          "module-abc-123",
			entityVersion:     "v1.0.0",
			graphNodes:        map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{},
			expectedModuleId:  "module-abc-123",
			expectedModuleVer: "v1.0.0",
			shouldEnrich:      true,
		},
		{
			name:     "enriches error with matching module node",
			entityId: "postgres_default_mydb_123",
			graphNodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				{Type: "postgres", Class: "default", Id: "mydb"}: {
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{
						DefinitionId: "postgres-module-def",
						VersionId:    "v1.2.3",
					},
				},
			},
			expectedModuleId:  "postgres-module-def",
			expectedModuleVer: "v1.2.3",
			shouldEnrich:      true,
		},
		{
			name:     "enriches error with special characters stripped",
			entityId: "postgres_default_my-db_456",
			graphNodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				{Type: "postgres", Class: "default", Id: "my-db"}: {
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{
						DefinitionId: "postgres-v2",
						VersionId:    "v2.0.0",
					},
				},
			},
			expectedModuleId:  "postgres-v2",
			expectedModuleVer: "v2.0.0",
			shouldEnrich:      true,
		},
		{
			name:          "falls back to graph lookup when entity_version is empty",
			entityId:      "postgres_default_mydb_123",
			entityVersion: "", // Empty version should trigger graph lookup
			graphNodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				{Type: "postgres", Class: "default", Id: "mydb"}: {
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{
						DefinitionId: "postgres-module-def",
						VersionId:    "v1.2.3",
					},
				},
			},
			expectedModuleId:  "postgres-module-def",
			expectedModuleVer: "v1.2.3",
			shouldEnrich:      true,
		},
		{
			name:     "does not enrich when node not found",
			entityId: "postgres_default_nonexistent_789",
			graphNodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				{Type: "postgres", Class: "default", Id: "mydb"}: {
					ModuleConfiguration: &graphs.GraphNodeModuleConfig{
						DefinitionId: "postgres-module-def",
						VersionId:    "v1.2.3",
					},
				},
			},
			shouldEnrich: false,
		},
		{
			name:     "does not enrich when node has no module configuration",
			entityId: "workload_default_app_123",
			graphNodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
				{Type: "workload", Class: "default", Id: "app"}: {
					ModuleConfiguration: nil,
				},
			},
			shouldEnrich: false,
		},
		{
			name:         "does not enrich when entity_id has no underscore",
			entityId:     "nounderscore",
			graphNodes:   map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{},
			shouldEnrich: false,
		},
		{
			name:         "returns original JSON when entity_id is empty",
			entityId:     "",
			graphNodes:   map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{},
			shouldEnrich: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mdb := mockmodel.NewMockDatabaser(ctrl)
			ctx := context.Background()
			deploymentId := uuid.New()

			// Create graph and encode it
			graph := &platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
				Nodes: tc.graphNodes,
			}
			encodedGraph, err := graphs.ToJson(graph)
			require.NoError(t, err)

			// Mock GetDeployment - only called if entityId is not empty
			if tc.entityId != "" {
				mdb.EXPECT().GetDeployment(gomock.Any(), nil, orgId, deploymentId, model.GetModeDefault).
					Return(nil, nil, nil, encodedGraph, nil).Times(1)
			}

			// Create error message
			errorMsg := map[string]interface{}{
				"entity_id":   tc.entityId,
				"entity_type": "module",
				"summary":     "Module error occurred",
			}
			if tc.entityVersion != "" {
				errorMsg["entity_version"] = tc.entityVersion
			}
			errorMsgJson, err := json.Marshal(errorMsg)
			require.NoError(t, err)

			// Call enrichDeploymentError
			result, err := enrichDeploymentError(ctx, mdb, orgId, deploymentId, "TF_DIAGNOSTIC_ERROR", string(errorMsgJson))
			require.NoError(t, err)

			// Verify the result
			if tc.shouldEnrich {
				var enrichedError map[string]interface{}
				err = json.Unmarshal([]byte(result), &enrichedError)
				require.NoError(t, err)

				assert.Equal(t, tc.expectedModuleId, enrichedError["module_id"])
				assert.Equal(t, tc.expectedModuleVer, enrichedError["module_version"])
				assert.Equal(t, tc.entityId, enrichedError["entity_id"])
				assert.Equal(t, "module", enrichedError["entity_type"])
			} else {
				// Should return JSON without enrichment fields
				var enrichedError map[string]interface{}
				err = json.Unmarshal([]byte(result), &enrichedError)
				require.NoError(t, err)
				assert.Equal(t, tc.entityId, enrichedError["entity_id"])
				assert.Equal(t, "module", enrichedError["entity_type"])
				assert.Nil(t, enrichedError["module_id"])
				assert.Nil(t, enrichedError["module_version"])
			}
		})
	}
}

func TestEnrichDeploymentError_ProviderEnrichment(t *testing.T) {
	tests := []struct {
		name                 string
		providerType         string
		providerId           string
		variantHash          string
		expectedProviderType string
		expectedProviderId   string
		shouldEnrich         bool
	}{
		{
			name:                 "enriches provider error with valid entity_id",
			providerType:         "aws",
			providerId:           "default",
			variantHash:          "abc12345",
			expectedProviderType: "aws",
			expectedProviderId:   "default",
			shouldEnrich:         true,
		},
		{
			name:                 "enriches provider error with underscores in type",
			providerType:         "azure_rm",
			providerId:           "prod",
			variantHash:          "def67890",
			expectedProviderType: "azure_rm",
			expectedProviderId:   "prod",
			shouldEnrich:         true,
		},
		{
			name:                 "enriches provider error with complex provider id",
			providerType:         "google",
			providerId:           "prod-us-east",
			variantHash:          "xyz98765",
			expectedProviderType: "google",
			expectedProviderId:   "prod-us-east",
			shouldEnrich:         true,
		},
		{
			name:         "does not enrich when provider not found in graph",
			providerType: "aws",
			providerId:   "missing",
			variantHash:  "notfound",
			shouldEnrich: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mdb := mockmodel.NewMockDatabaser(ctrl)
			ctx := context.Background()
			deploymentId := uuid.New()

			// Create graph with provider mapping
			localProviderName := graphs.LocalProviderName(tc.providerType, tc.providerId, tc.variantHash)

			// The full provider identifier (e.g., "aws.default")
			fullProviderIdentifier := fmt.Sprintf("%s.%s", tc.providerType, tc.providerId)

			var providerMapping map[string]string

			if tc.shouldEnrich {
				// Map from full provider identifier to hash variant
				providerMapping = map[string]string{
					fullProviderIdentifier: tc.variantHash,
				}
			}

			graph := &platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
				Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
					{Type: "test", Class: "default", Id: "test"}: {
						ModuleConfiguration: &graphs.GraphNodeModuleConfig{
							DefinitionId:                       "test-def",
							VersionId:                          "test-ver",
							ProviderFullIdToHashVariantMapping: providerMapping,
						},
					},
				},
			}
			encodedGraph, err := graphs.ToJson(graph)
			require.NoError(t, err)

			// Mock GetDeployment to return the graph
			mdb.EXPECT().GetDeployment(gomock.Any(), nil, orgId, deploymentId, model.GetModeDefault).
				Return(nil, nil, nil, model.EncodedDeploymentGraph(encodedGraph), nil)

			// Create error message with the hashed local provider name
			errorMsg := map[string]interface{}{
				"entity_id":   localProviderName,
				"entity_type": "provider",
				"summary":     "Provider error occurred",
			}
			errorMsgJson, err := json.Marshal(errorMsg)
			require.NoError(t, err)

			// Call enrichDeploymentError
			result, err := enrichDeploymentError(ctx, mdb, orgId, deploymentId, "TF_DIAGNOSTIC_ERROR", string(errorMsgJson))
			require.NoError(t, err)

			// Verify the result
			if tc.shouldEnrich {
				var enrichedError map[string]interface{}
				err = json.Unmarshal([]byte(result), &enrichedError)
				require.NoError(t, err)

				assert.Equal(t, tc.expectedProviderType, enrichedError["provider_type"])
				assert.Equal(t, tc.expectedProviderId, enrichedError["provider_id"])
				assert.Equal(t, localProviderName, enrichedError["entity_id"])
				assert.Equal(t, "provider", enrichedError["entity_type"])
			} else {
				// Should return JSON without enrichment fields
				var enrichedError map[string]interface{}
				err = json.Unmarshal([]byte(result), &enrichedError)
				require.NoError(t, err)
				assert.Equal(t, localProviderName, enrichedError["entity_id"])
				assert.Equal(t, "provider", enrichedError["entity_type"])
				assert.Nil(t, enrichedError["provider_type"])
				assert.Nil(t, enrichedError["provider_id"])
			}
		})
	}
}

func TestEnrichDeploymentError_UnknownEntityType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mdb := mockmodel.NewMockDatabaser(ctrl)
	deploymentId := uuid.New()

	errorMsg := map[string]interface{}{
		"entity_id":   "some-entity",
		"entity_type": "unknown_type",
		"summary":     "Unknown error",
	}
	errorMsgJson, err := json.Marshal(errorMsg)
	require.NoError(t, err)

	result, err := enrichDeploymentError(t.Context(), mdb, orgId, deploymentId, "TF_DIAGNOSTIC_ERROR", string(errorMsgJson))
	require.NoError(t, err)

	// Should return JSON without enrichment fields
	var enrichedError map[string]interface{}
	err = json.Unmarshal([]byte(result), &enrichedError)
	require.NoError(t, err)
	assert.Equal(t, "some-entity", enrichedError["entity_id"])
	assert.Equal(t, "unknown_type", enrichedError["entity_type"])
	assert.Nil(t, enrichedError["module_id"])
	assert.Nil(t, enrichedError["module_version"])
	assert.Nil(t, enrichedError["provider_type"])
	assert.Nil(t, enrichedError["provider_id"])
}

func TestListDeployments_org_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanReadOrgCheck("my-org")},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), "my-org").Return(&platformorchestratorcp.GetInternalOrganizationResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNotFound},
	}, nil)

	r, err := s.ListDeployments(ctx, ListDeploymentsRequestObject{
		OrgId: "my-org",
	})
	require.NoError(t, err)
	require.IsTypef(t, ListDeployments404JSONResponse{}, r, "unexpected %v", r)
	r404 := r.(ListDeployments404JSONResponse)
	if assert.NotNil(t, r404) {
		assert.Equal(t, "organization my-org not found", r404.Message)
	}
}

func TestListLastDeployments_org_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	userId := userid.NewHumanUserId()
	mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
	mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanReadOrgCheck("my-org")},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil).Times(1)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), "my-org").Return(&platformorchestratorcp.GetInternalOrganizationResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNotFound},
	}, nil)

	r, err := s.ListLastDeployments(ctx, ListLastDeploymentsRequestObject{
		OrgId: "my-org",
	})
	require.NoError(t, err)
	require.IsTypef(t, ListLastDeployments404JSONResponse{}, r, "unexpected %v", r)
	r404 := r.(ListLastDeployments404JSONResponse)
	if assert.NotNil(t, r404) {
		assert.Equal(t, "organization my-org not found", r404.Message)
	}
}
