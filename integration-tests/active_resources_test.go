package integrationtests

import (
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"

	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/genclient"
)

func TestActiveResourceNodes(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	internalDpClient := MustInternalDataPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "app-1").Id
	_ = MustCreateFakeK8sDefaultRunner(t, cpClient, orgId)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	{
		res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, platformorchestratorcp.ResourceTypeCreateBody{Id: "thing", OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	var childModuleDef platformorchestratorcp.Module
	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{Id: "child", ResourceType: "thing", ModuleSource: "acme/thing/generic@v1", Description: ref.Ref("child def")})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		childModuleDef = *res.JSON201
		res2, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: childModuleDef.Id, ResourceClass: ref.Ref(childModuleDef.Id)})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res2.StatusCode(), string(res2.Body))
	}

	var parentModuleDef platformorchestratorcp.Module
	{
		res, err := cpClient.CreateModuleWithResponse(
			t.Context(),
			orgId,
			platformorchestratorcp.ModuleCreateBody{Id: "parent", ResourceType: "thing", ModuleSource: "acme/k8ss/generic@v1", Description: ref.Ref("parent def"), Dependencies: map[string]platformorchestratorcp.ModuleDependencyManifest{"child": {Type: "thing", Class: ref.Ref("child")}}},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		parentModuleDef = *res.JSON201
		res2, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: parentModuleDef.Id, ResourceClass: ref.Ref(parentModuleDef.Id)})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res2.StatusCode(), string(res2.Body))
	}

	var dep serverclient.Deployment
	{
		depRes, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: projectId, EnvId: env.Id,
			Mode: serverclient.DeploymentCreateBodyModePlanOnly,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"a": {
								Type:  "thing",
								Class: ref.Ref("parent"),
							},
							"b": {
								Type:  "thing",
								Class: ref.Ref("child"),
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, depRes.StatusCode(), string(depRes.Body))
		dep = *depRes.JSON201
		res, err := dpClient.UpdateDeploymentResultsWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.UpdateDeploymentResultsParams{XDeploymentToken: generateHashedRunnerToken(orgId, dep.Id.String())}, serverclient.DeploymentResultsUpdateBody{Status: serverclient.Success})
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode(), string(res.Body))
	}

	t.Run("plan only env has empty graph", func(t *testing.T) {
		ar, err := dpClient.ListActiveResourceNodesWithResponse(t.Context(), dep.OrgId, &serverclient.ListActiveResourceNodesParams{
			ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, ar.StatusCode(), string(ar.Body))
		assert.Empty(t, ar.JSON200.Items)
	})

	// NOW we deploy the same graph again, this time in deploy mode.
	{
		depRes, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: projectId, EnvId: env.Id,
			Mode: serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"a": {
								Type:  "thing",
								Class: ref.Ref("parent"),
							},
							"b": {
								Type:  "thing",
								Class: ref.Ref("child"),
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, depRes.StatusCode(), string(depRes.Body))
		dep = *depRes.JSON201
		res, err := dpClient.UpdateDeploymentResultsWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.UpdateDeploymentResultsParams{XDeploymentToken: generateHashedRunnerToken(orgId, dep.Id.String())},
			serverclient.DeploymentResultsUpdateBody{Status: serverclient.Success,
				Metadata: []serverclient.DeploymentResultMetadataPerNode{
					{NodeId: nodeHash(env.Uuid, "thing parent workloads.sample.a"), Metadata: map[string]interface{}{}},
					{NodeId: nodeHash(env.Uuid, "thing child workloads.sample.a"), Metadata: map[string]interface{}{"gcp_project": "test-project"}},
					{NodeId: nodeHash(env.Uuid, "thing child workloads.sample.b"), Metadata: map[string]interface{}{"gcp_project": "another-project"}},
				}})
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode(), string(res.Body))
	}

	sortActiveResourceNodes := func(nodes []serverclient.ActiveResourceNode) {
		slices.SortFunc(nodes, func(a, b serverclient.ActiveResourceNode) int {
			if i := strings.Compare(a.ResourceType, b.ResourceType); i != 0 {
				return i
			} else if i = strings.Compare(a.ResourceClass, b.ResourceClass); i != 0 {
				return i
			}
			return strings.Compare(a.ResourceId, b.ResourceId)
		})
	}

	t.Run("deploy env has graph", func(t *testing.T) {
		ar, err := dpClient.ListActiveResourceNodesWithResponse(t.Context(), dep.OrgId, &serverclient.ListActiveResourceNodesParams{
			ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, ar.StatusCode(), string(ar.Body))

		sortActiveResourceNodes(ar.JSON200.Items)
		assert.Equal(t, []serverclient.ActiveResourceNode{
			{
				Id:            nodeHash(env.Uuid, "thing child workloads.sample.a"),
				ProjectId:     projectId,
				EnvId:         env.Id,
				ResourceType:  "thing",
				ResourceClass: "child",
				ResourceId:    "workloads.sample.a",
				Edges:         map[string]string{},
				DeploymentId:  dep.Id,
				ModuleId:      "child",
				ModuleVersion: childModuleDef.VersionId,
				Metadata:      map[string]interface{}{"gcp_project": "test-project"},
			},
			{
				Id:            nodeHash(env.Uuid, "thing child workloads.sample.b"),
				ProjectId:     projectId,
				EnvId:         env.Id,
				ResourceType:  "thing",
				ResourceClass: "child",
				ResourceId:    "workloads.sample.b",
				Edges:         map[string]string{},
				DeploymentId:  dep.Id,
				ModuleId:      "child",
				ModuleVersion: childModuleDef.VersionId,
				Metadata:      map[string]interface{}{"gcp_project": "another-project"},
			},
			{Id: nodeHash(env.Uuid, "thing parent workloads.sample.a"), ProjectId: projectId, EnvId: env.Id, ResourceType: "thing", ResourceClass: "parent", ResourceId: "workloads.sample.a", Edges: map[string]string{
				"child": nodeHash(env.Uuid, "thing child workloads.sample.a"),
			}, DeploymentId: dep.Id, ModuleId: "parent", ModuleVersion: parentModuleDef.VersionId, Metadata: map[string]interface{}{}},
			{Id: nodeHash(env.Uuid, "workload default sample"), ProjectId: projectId, EnvId: env.Id, ResourceType: "workload", ResourceClass: "default", ResourceId: "sample", Edges: map[string]string{
				"a": nodeHash(env.Uuid, "thing parent workloads.sample.a"),
				"b": nodeHash(env.Uuid, "thing child workloads.sample.b"),
			}, DeploymentId: dep.Id, ModuleId: "", ModuleVersion: "", Metadata: map[string]interface{}{}},
		}, ar.JSON200.Items)
	})

	t.Run("can check usage of a used module", func(t *testing.T) {
		ar, err := internalDpClient.InternalCheckModuleUsageWithResponse(t.Context(), dep.OrgId, "parent", &serverclient.InternalCheckModuleUsageParams{})
		require.NoError(t, err)
		assert.Equal(t, &serverclient.InternalModuleUsage{
			EnvIdsByProjectId: map[string][]string{
				dep.ProjectId: {dep.EnvId},
			},
		}, ar.JSON200, string(ar.Body))
	})

	t.Run("can check usage of un-used module", func(t *testing.T) {
		ar, err := internalDpClient.InternalCheckModuleUsageWithResponse(t.Context(), dep.OrgId, "unused-module", &serverclient.InternalCheckModuleUsageParams{})
		require.NoError(t, err)
		assert.Equal(t, &serverclient.InternalModuleUsage{EnvIdsByProjectId: map[string][]string{}}, ar.JSON200, string(ar.Body))
	})

	// NOW we remove a resource and add another workload and see what happens
	oldDepId := dep.Id
	{
		depRes, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: projectId, EnvId: env.Id,
			Mode: serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"b": {
								Type:  "thing",
								Class: ref.Ref("child"),
							},
						},
					},
					"new": {},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, depRes.StatusCode(), string(depRes.Body))
		dep = *depRes.JSON201
	}

	// Now when we inspect the graph we can clearly see a subgraph of disconnected nodes tied to the old deployment id
	t.Run("check for subgraph", func(t *testing.T) {
		ar, err := dpClient.ListActiveResourceNodesWithResponse(t.Context(), dep.OrgId, &serverclient.ListActiveResourceNodesParams{
			ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, ar.StatusCode(), string(ar.Body))

		sortActiveResourceNodes(ar.JSON200.Items)
		assert.Equal(t, []serverclient.ActiveResourceNode{
			{
				Id:            nodeHash(env.Uuid, "thing child workloads.sample.a"),
				ProjectId:     projectId,
				EnvId:         env.Id,
				ResourceType:  "thing",
				ResourceClass: "child",
				ResourceId:    "workloads.sample.a",
				Edges:         map[string]string{},
				DeploymentId:  oldDepId,
				ModuleId:      "child",
				ModuleVersion: childModuleDef.VersionId,
				Metadata:      map[string]interface{}{"gcp_project": "test-project"},
			},
			{
				Id:            nodeHash(env.Uuid, "thing child workloads.sample.b"),
				ProjectId:     projectId,
				EnvId:         env.Id,
				ResourceType:  "thing",
				ResourceClass: "child",
				ResourceId:    "workloads.sample.b",
				Edges:         map[string]string{},
				DeploymentId:  dep.Id,
				ModuleId:      "child",
				ModuleVersion: childModuleDef.VersionId,
				Metadata:      map[string]interface{}{"gcp_project": "another-project"},
			},
			{Id: nodeHash(env.Uuid, "thing parent workloads.sample.a"), ProjectId: projectId, EnvId: env.Id, ResourceType: "thing", ResourceClass: "parent", ResourceId: "workloads.sample.a", Edges: map[string]string{
				"child": nodeHash(env.Uuid, "thing child workloads.sample.a"),
			}, DeploymentId: oldDepId, ModuleId: "parent", ModuleVersion: parentModuleDef.VersionId, Metadata: map[string]interface{}{}},
			{Id: nodeHash(env.Uuid, "workload default new"), ProjectId: projectId, EnvId: env.Id, ResourceType: "workload", ResourceClass: "default", ResourceId: "new", Edges: map[string]string{}, DeploymentId: dep.Id, ModuleId: "", ModuleVersion: "", Metadata: map[string]interface{}{}},
			{Id: nodeHash(env.Uuid, "workload default sample"), ProjectId: projectId, EnvId: env.Id, ResourceType: "workload", ResourceClass: "default", ResourceId: "sample", Edges: map[string]string{
				"b": nodeHash(env.Uuid, "thing child workloads.sample.b"),
			}, DeploymentId: dep.Id, ModuleId: "", ModuleVersion: "", Metadata: map[string]interface{}{}},
		}, ar.JSON200.Items)
	})

	// Now we "succeed"/ finish the deployment which should remove the old nodes and edges
	{
		res, err := dpClient.UpdateDeploymentResultsWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.UpdateDeploymentResultsParams{XDeploymentToken: generateHashedRunnerToken(orgId, dep.Id.String())}, serverclient.DeploymentResultsUpdateBody{Status: serverclient.Success})
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode(), string(res.Body))
	}

	t.Run("check finished deployment has no subgraph", func(t *testing.T) {
		ar, err := dpClient.ListActiveResourceNodesWithResponse(t.Context(), dep.OrgId, &serverclient.ListActiveResourceNodesParams{
			ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, ar.StatusCode(), string(ar.Body))

		sortActiveResourceNodes(ar.JSON200.Items)
		assert.Equal(t, []serverclient.ActiveResourceNode{
			{
				Id:            nodeHash(env.Uuid, "thing child workloads.sample.b"),
				ProjectId:     projectId,
				EnvId:         env.Id,
				ResourceType:  "thing",
				ResourceClass: "child",
				ResourceId:    "workloads.sample.b",
				Edges:         map[string]string{},
				DeploymentId:  dep.Id,
				ModuleId:      "child",
				ModuleVersion: childModuleDef.VersionId,
				Metadata:      map[string]interface{}{"gcp_project": "another-project"},
			},
			{Id: nodeHash(env.Uuid, "workload default new"), ProjectId: projectId, EnvId: env.Id, ResourceType: "workload", ResourceClass: "default", ResourceId: "new", Edges: map[string]string{}, DeploymentId: dep.Id, ModuleId: "", ModuleVersion: "", Metadata: map[string]interface{}{}},
			{Id: nodeHash(env.Uuid, "workload default sample"), ProjectId: projectId, EnvId: env.Id, ResourceType: "workload", ResourceClass: "default", ResourceId: "sample", Edges: map[string]string{
				"b": nodeHash(env.Uuid, "thing child workloads.sample.b"),
			}, DeploymentId: dep.Id, ModuleId: "", ModuleVersion: "", Metadata: map[string]interface{}{}},
		}, ar.JSON200.Items)
	})

	// NOW finally we do a destroy
	oldDepId = dep.Id
	var destroyDep serverclient.DeploymentSummary
	{
		res, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, dep.ProjectId, dep.EnvId, &platformorchestratorcp.DeleteEnvironmentParams{})
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, res.StatusCode(), string(res.Body))
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			res, err := dpClient.ListLastDeploymentsWithResponse(t.Context(), dep.OrgId, &serverclient.ListLastDeploymentsParams{ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId)})
			require.NoError(collect, err)
			require.Equal(collect, http.StatusOK, res.StatusCode(), string(res.Body))
			require.Equal(collect, "executing", res.JSON200.Items[0].Status)
			destroyDep = res.JSON200.Items[0]
		}, time.Minute, time.Second, "never launched a destroy deployment")
	}

	// Now when we inspect the graph we can clearly see all the nodes tied to the old deployment id
	t.Run("check for subgraph", func(t *testing.T) {
		ar, err := dpClient.ListActiveResourceNodesWithResponse(t.Context(), dep.OrgId, &serverclient.ListActiveResourceNodesParams{
			ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, ar.StatusCode(), string(ar.Body))

		sortActiveResourceNodes(ar.JSON200.Items)
		assert.Equal(t, []serverclient.ActiveResourceNode{
			{
				Id:            nodeHash(env.Uuid, "thing child workloads.sample.b"),
				ProjectId:     projectId,
				EnvId:         env.Id,
				ResourceType:  "thing",
				ResourceClass: "child",
				ResourceId:    "workloads.sample.b",
				Edges:         map[string]string{},
				DeploymentId:  oldDepId,
				ModuleId:      "child",
				ModuleVersion: childModuleDef.VersionId,
				Metadata:      map[string]interface{}{"gcp_project": "another-project"},
			},
			{Id: nodeHash(env.Uuid, "workload default new"), ProjectId: projectId, EnvId: env.Id, ResourceType: "workload", ResourceClass: "default", ResourceId: "new", Edges: map[string]string{}, DeploymentId: oldDepId, ModuleId: "", ModuleVersion: "", Metadata: map[string]interface{}{}},
			{Id: nodeHash(env.Uuid, "workload default sample"), ProjectId: projectId, EnvId: env.Id, ResourceType: "workload", ResourceClass: "default", ResourceId: "sample", Edges: map[string]string{
				"b": nodeHash(env.Uuid, "thing child workloads.sample.b"),
			}, DeploymentId: oldDepId, ModuleId: "", ModuleVersion: "", Metadata: map[string]interface{}{}},
		}, ar.JSON200.Items)
	})

	// Now we "succeed"/ finish the deployment which should remove the old nodes and edges
	{
		res, err := dpClient.UpdateDeploymentResultsWithResponse(t.Context(), dep.OrgId, destroyDep.Id, &serverclient.UpdateDeploymentResultsParams{XDeploymentToken: generateHashedRunnerToken(orgId, destroyDep.Id.String())}, serverclient.DeploymentResultsUpdateBody{Status: serverclient.Success})
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode(), string(res.Body))
	}

	t.Run("check finished deployment has no subgraph", func(t *testing.T) {
		ar, err := dpClient.ListActiveResourceNodesWithResponse(t.Context(), dep.OrgId, &serverclient.ListActiveResourceNodesParams{
			ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId),
		})
		require.NoError(t, err)
		// this test seems to be sensitive to timing, it will either return an empty set or a 404 depending on the background event processing.
		require.Contains(t, []int{http.StatusOK, http.StatusNotFound}, ar.StatusCode(), string(ar.Body))
		if ar.StatusCode() == http.StatusOK {
			assert.Equal(t, []serverclient.ActiveResourceNode{}, ar.JSON200.Items)
		}
	})
}

func TestActiveResourceNodes_404(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)

	orgId := MustCreateOrgId(t, MustInternalControlPlaneClient(t))
	res, err := dpClient.ListActiveResourceNodesWithResponse(t.Context(), orgId, &serverclient.ListActiveResourceNodesParams{ProjectId: ref.Ref("app-1"), EnvId: ref.Ref("env-1")})
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, res.StatusCode(), string(res.Body))
	assert.Equal(t, "no deployments found in env 'env-1' of project 'app-1'", res.JSON404.Message)
}
