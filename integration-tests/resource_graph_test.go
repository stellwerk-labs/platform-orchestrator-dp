package integrationtests

import (
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"

	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genclient"
)

func TestListDeploymentResourceNodes_notFound(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	orgId := MustCreateOrgId(t, MustInternalControlPlaneClient(t))

	res, err := dpClient.ListDeploymentResourceNodesWithResponse(t.Context(), orgId, uuid.New())
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, res.StatusCode(), string(res.Body))
}

func TestListDeploymentResourceNodes_returnsGraphNodes(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)

	// Set up org and environment
	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "app-1").Id
	_ = MustCreateFakeK8sDefaultRunner(t, cpClient, orgId)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	// Set up CP resources: resource type, child and parent modules (parent depends on child)
	MustCreateResourceType(t, cpClient, orgId, "thing")

	var childModuleDef platformorchestratorcp.Module
	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{Id: "child", ResourceType: "thing", ModuleSource: "acme/thing/generic@v1"})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		childModuleDef = *res.JSON201
		res2, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: childModuleDef.Id, ResourceClass: ref.Ref(childModuleDef.Id)})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res2.StatusCode(), string(res2.Body))
	}

	var parentModuleDef platformorchestratorcp.Module
	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{
			Id: "parent", ResourceType: "thing", ModuleSource: "acme/thing/generic@v1",
			Dependencies: map[string]platformorchestratorcp.ModuleDependencyManifest{"child": {Type: "thing", Class: ref.Ref("child")}},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		parentModuleDef = *res.JSON201
		res2, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: parentModuleDef.Id, ResourceClass: ref.Ref(parentModuleDef.Id)})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res2.StatusCode(), string(res2.Body))
	}

	// Create deployment with workload referencing both parent and child resources
	depRes, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
		ProjectId: projectId, EnvId: env.Id,
		Mode: serverclient.DeploymentCreateBodyModeDeploy,
		Manifest: &serverclient.DeploymentManifest{
			Workloads: map[string]serverclient.DeploymentManifestWorkload{
				"sample": {
					Resources: map[string]serverclient.DeploymentManifestResource{
						"a": {Type: "thing", Class: ref.Ref("parent")},
						"b": {Type: "thing", Class: ref.Ref("child")},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, depRes.StatusCode(), string(depRes.Body))
	dep := depRes.JSON201

	res, err := dpClient.ListDeploymentResourceNodesWithResponse(t.Context(), orgId, dep.Id)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))

	items := res.JSON200.Items
	slices.SortFunc(items, func(a, b serverclient.ActiveResourceNode) int {
		return strings.Compare(a.Id, b.Id)
	})

	// Verify all expected node IDs are present
	nodeIds := make([]string, len(items))
	for i, item := range items {
		nodeIds[i] = item.Id
	}
	assert.Contains(t, nodeIds, nodeHash(env.Uuid, "thing parent workloads.sample.a"))
	assert.Contains(t, nodeIds, nodeHash(env.Uuid, "thing child workloads.sample.a"))
	assert.Contains(t, nodeIds, nodeHash(env.Uuid, "thing child workloads.sample.b"))
	assert.Contains(t, nodeIds, nodeHash(env.Uuid, "workload default sample"))

	// Verify node fields and edges for the parent node
	for _, item := range items {
		if item.Id == nodeHash(env.Uuid, "thing parent workloads.sample.a") {
			assert.Equal(t, projectId, item.ProjectId)
			assert.Equal(t, env.Id, item.EnvId)
			assert.Equal(t, dep.Id, item.DeploymentId)
			assert.Equal(t, "thing", item.ResourceType)
			assert.Equal(t, "parent", item.ResourceClass)
			assert.Equal(t, "parent", item.ModuleId)
			assert.Equal(t, parentModuleDef.VersionId, item.ModuleVersion)
			assert.Equal(t, map[string]string{"child": nodeHash(env.Uuid, "thing child workloads.sample.a")}, item.Edges)
		}
	}
}

func TestListDeploymentResourceNodes_matchesActiveResourceNodes(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)

	// Set up org and environment
	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "app-1").Id
	_ = MustCreateFakeK8sDefaultRunner(t, cpClient, orgId)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	// Set up CP resources: resource type and module
	MustCreateResourceType(t, cpClient, orgId, "thing")

	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{Id: "mymod", ResourceType: "thing", ModuleSource: "acme/thing/generic@v1"})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		res2, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: "mymod", ResourceClass: ref.Ref("mymod")})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res2.StatusCode(), string(res2.Body))
	}

	// Create and complete a deployment so nodes are promoted to active resource nodes
	depRes, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
		ProjectId: projectId, EnvId: env.Id,
		Mode: serverclient.DeploymentCreateBodyModeDeploy,
		Manifest: &serverclient.DeploymentManifest{
			Workloads: map[string]serverclient.DeploymentManifestWorkload{
				"sample": {
					Resources: map[string]serverclient.DeploymentManifestResource{
						"r": {Type: "thing", Class: ref.Ref("mymod")},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, depRes.StatusCode(), string(depRes.Body))
	dep := depRes.JSON201

	updateRes, err := dpClient.UpdateDeploymentResultsWithResponse(t.Context(), dep.OrgId, dep.Id,
		&serverclient.UpdateDeploymentResultsParams{XDeploymentToken: generateHashedRunnerToken(orgId, dep.Id.String())},
		serverclient.DeploymentResultsUpdateBody{Status: serverclient.Success})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, updateRes.StatusCode(), string(updateRes.Body))

	activeRes, err := dpClient.ListActiveResourceNodesWithResponse(t.Context(), orgId, &serverclient.ListActiveResourceNodesParams{
		ProjectId: ref.Ref(projectId), EnvId: ref.Ref(env.Id),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, activeRes.StatusCode(), string(activeRes.Body))

	depNodesRes, err := dpClient.ListDeploymentResourceNodesWithResponse(t.Context(), orgId, dep.Id)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, depNodesRes.StatusCode(), string(depNodesRes.Body))

	// Deployment resource nodes don't carry runner-reported metadata — clear it before comparing.
	for i := range activeRes.JSON200.Items {
		activeRes.JSON200.Items[i].Metadata = map[string]interface{}{}
	}

	sortByID := func(items []serverclient.ActiveResourceNode) {
		slices.SortFunc(items, func(a, b serverclient.ActiveResourceNode) int {
			return strings.Compare(a.Id, b.Id)
		})
	}
	sortByID(activeRes.JSON200.Items)
	sortByID(depNodesRes.JSON200.Items)

	assert.Equal(t, activeRes.JSON200.Items, depNodesRes.JSON200.Items)
}
