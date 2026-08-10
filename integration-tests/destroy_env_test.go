package integrationtests

import (
	"net/http"
	"testing"
	"time"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"

	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genclient"
)

func TestDestroyEmptyEnv_delete_rules(t *testing.T) {
	t.Parallel()
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", "", "", nil).Id
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")
	moduleRuleId := MustCreateModuleForEnv(t, cpClient, orgId, projectId, env.Id).Id

	{
		r, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id, &platformorchestratorcp.DeleteEnvironmentParams{DeleteRules: ref.Ref(true)})
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, r.StatusCode())
		assert.Equal(t, platformorchestratorcp.EnvironmentStatusDeleting, r.JSON202.Status)
	}

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		r, err := cpClient.GetEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id)
		require.NoError(collect, err)
		require.Equalf(collect, http.StatusNotFound, r.StatusCode(), "got %s", string(r.Body))
	}, time.Second*10, time.Second, "failed to wait for env to be deleted")

	r, err := cpClient.GetModuleRuleInOrgWithResponse(t.Context(), orgId, moduleRuleId)
	require.NoError(t, err)
	require.Equalf(t, http.StatusNotFound, r.StatusCode(), "got %s", string(r.Body))

	t.Logf("env %s fully deleted", env.Id)
}

func TestDestroyEnv_nominal(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", "", "", nil).Id
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")
	moduleRuleId := MustCreateModuleForEnv(t, cpClient, orgId, projectId, env.Id).Id

	var dep serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{},
						Outputs:   map[string]string{"KEY": "VALUE"},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	}

	require.EventuallyWithTf(t, func(c *assert.CollectT) {
		res, err := dpClient.WaitForDeploymentCompleteWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.WaitForDeploymentCompleteParams{})
		if assert.NoError(c, err) && assert.Equal(c, http.StatusOK, res.StatusCode(), string(res.Body)) {
			dep = *res.JSON200
			assert.Equal(c, "succeeded", res.JSON200.Status)
		}
	}, 2*time.Minute, 2*time.Second, "deployment %s not completed after 2 mins: %s", dep.Id, dep.StatusMessage)

	{
		r, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id, &platformorchestratorcp.DeleteEnvironmentParams{})
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, r.StatusCode())
		assert.Equal(t, platformorchestratorcp.EnvironmentStatusDeleting, r.JSON202.Status)
	}

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		r, err := cpClient.GetEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id)
		require.NoError(collect, err)
		require.Equalf(collect, http.StatusNotFound, r.StatusCode(), "got %s", string(r.Body))
	}, 2*time.Minute, 2*time.Second, "failed to wait for env to be deleted")

	r, err := cpClient.GetModuleRuleInOrgWithResponse(t.Context(), orgId, moduleRuleId)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, r.StatusCode(), "got %s", string(r.Body))
	require.Equal(t, env.Id, *r.JSON200.EnvId)
	t.Logf("env %s fully deleted", env.Id)
}

func TestDestroyEnv_resume_after_fail(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", "", "", nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	var dep serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{},
						Outputs:   map[string]string{"KEY": "VALUE"},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := dpClient.WaitForDeploymentCompleteWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.WaitForDeploymentCompleteParams{})
		if assert.NoError(t, err) && assert.Equal(c, http.StatusOK, res.StatusCode()) {
			dep = *res.JSON200
			assert.Equal(c, "succeeded", res.JSON200.Status)
		}
	}, 2*time.Minute, 2*time.Second, "deployment %s not completed after 2 mins: %s", dep.Id, dep.StatusMessage)

	// We don't actually have a way to fail a deployment right now! So we're simulating a direct failure by status update.
	{
		r, err := internalCpClient.InternalUpdateEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id, platformorchestratorcp.EnvironmentInternalUpdateBody{Status: ref.Ref(platformorchestratorcp.EnvironmentStatusDeleteFailed), StatusMessage: ref.Ref("test")})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, r.StatusCode(), string(r.Body))
		assert.Equal(t, platformorchestratorcp.EnvironmentStatusDeleteFailed, r.JSON200.Status)
	}

	// We should be able to resume deletion when it is in delete failed.
	{
		r, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id, &platformorchestratorcp.DeleteEnvironmentParams{})
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, r.StatusCode(), string(r.Body))
		assert.Equal(t, platformorchestratorcp.EnvironmentStatusDeleting, r.JSON202.Status)
	}

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		r, err := cpClient.GetEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id)
		require.NoError(collect, err)
		require.Equalf(collect, http.StatusNotFound, r.StatusCode(), "got %s", string(r.Body))
	}, 2*time.Minute, 2*time.Second, "failed to wait for env to be deleted")
	t.Logf("env %s fully deleted", env.Id)
}

func TestDestroyEnv_force(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", "", "", nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	{
		res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, platformorchestratorcp.CreateResourceTypeJSONRequestBody{Id: "thing",
			OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{Id: "thing-def", ResourceType: "thing",
			ModuleSource: "inline",
			ModuleSourceCode: ref.Ref(`
resource "terraform_data" "thing" {
  provisioner "local-exec" {
    command = "sleep 180"
  }
}
`),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: "thing-def"})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"t": {Type: "thing"},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	// Should accept force environment deletion request
	{
		r, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id, &platformorchestratorcp.DeleteEnvironmentParams{
			Force: ref.Ref(true),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, r.StatusCode(), string(r.Body))
	}

	// All deployments should be deleted in 2 minutes, despite the last deployment is still running (3 minutes)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := dpClient.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{
			ProjectId: &projectId,
			EnvId:     &env.Id,
		})
		require.NoError(c, err)
		require.NotNil(c, res.JSON200)
		assert.Empty(c, res.JSON200.Items)
	}, 2*time.Minute, 2*time.Second, "failed to wait for env to be deleted")

	// The environment should be deleted
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		r, err := cpClient.GetEnvironmentWithResponse(t.Context(), orgId, projectId, env.Id)
		require.NoError(c, err)
		require.Equalf(c, http.StatusNotFound, r.StatusCode(), "got %s", string(r.Body))
	}, 2*time.Minute, 2*time.Second, "failed to wait for env to be deleted")
	t.Logf("env %s fully deleted", env.Id)
}
