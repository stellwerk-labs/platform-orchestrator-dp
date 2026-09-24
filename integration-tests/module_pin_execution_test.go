package integrationtests

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genclient"
)

func TestManagedModulePinExecutionAndArchivedCarryForward(t *testing.T) {
	cp := MustControlPlaneClient(t)
	dp := MustDataPlaneClient(t)
	orgID := MustCreateOrgId(t, MustInternalControlPlaneClient(t))
	project := MustCreateProject(t, cp, orgID, "pin-adoption")
	envType := MustCreateEnvType(t, cp, orgID, "development")
	MustCreateRunnerWithRule(t, cp, orgID, "pin-runner", "", "", nil)
	environment := MustCreateEnv(t, cp, orgID, envType.Id, project.Id, "pinned")
	resourceType, err := cp.CreateResourceTypeWithResponse(t.Context(), orgID, platformorchestratorcp.ResourceTypeCreateBody{
		Id: "managed-value", OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"value": map[string]interface{}{"type": "string"}}},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resourceType.StatusCode(), string(resourceType.Body))
	const moduleID = "managed-module"
	const source = `variable "value" { type = string }
resource "terraform_data" "value" { input = var.value }
output "value" { value = terraform_data.value.output }`
	created, err := createManagedModuleWithResponse(t, cp, orgID, platformorchestratorcp.ModuleCreateBody{
		Id: moduleID, ResourceType: resourceType.JSON201.Id, ModuleSource: "inline", ModuleSourceCode: ref.Ref(source), ModuleInputs: map[string]interface{}{"value": "initial"},
	}, "value")
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, created.StatusCode(), string(created.Body))
	rule, err := cp.CreateModuleRuleInOrgWithResponse(t.Context(), orgID, platformorchestratorcp.RuleCreateBody{ModuleId: moduleID})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, rule.StatusCode(), string(rule.Body))
	identity, recipient := GetAgeIdentityAndRecipient(t)
	manifest := serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{
		"application": {Resources: map[string]serverclient.DeploymentManifestResource{"value": {Type: resourceType.JSON201.Id}}, Outputs: map[string]string{"VALUE": "${resources.value.outputs.value}"}},
	}}
	deploy := func(expectedValue string, selections map[string]string) {
		response, err := dp.CreateDeploymentWithResponse(t.Context(), orgID, &serverclient.CreateDeploymentParams{}, serverclient.DeploymentCreateBody{
			ProjectId: project.Id, EnvId: environment.Id, Mode: serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &manifest, EncryptedOutputsRecipient: ref.Ref(recipient), ModuleVersions: selections,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode(), string(response.Body))
		completed := MustWaitForDeploymentComplete(t, dp, orgID, response.JSON201.Id)
		require.Equal(t, "succeeded", completed.Status, completed.StatusMessage)
		outputs, err := dp.GetDeploymentEncryptedOutputsWithResponse(t.Context(), orgID, completed.Id)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, outputs.StatusCode())
		assert.JSONEq(t, `{"application":{"VALUE":"`+expectedValue+`"}}`, decryptOutputs(t, identity, []byte(outputs.JSON200.Raw)))
	}
	deploy("initial", nil)
	basePath := "/orgs/" + orgID
	var detail struct {
		Version struct {
			UUID       string `json:"uuid"`
			ModuleUUID string `json:"module_uuid"`
		} `json:"version"`
	}
	coreTestJSON(t, http.MethodGet, basePath+"/modules/"+moduleID+"/versions/1.0.0", nil, http.StatusOK, &detail)
	var pin struct {
		ID                string `json:"id"`
		VersionUUID       string `json:"version_uuid"`
		Status            string `json:"status"`
		ResourceVersion   int64  `json:"resource_version"`
		ActivationEventID string `json:"activation_event_id"`
	}
	coreTestJSON(t, http.MethodPost, basePath+"/module-version-pins", map[string]any{
		"project_uuid": project.Uuid, "environment_uuid": environment.Uuid, "module_uuid": detail.Version.ModuleUUID,
		"version_uuid": detail.Version.UUID, "reason": "Keep the effective deployment during Default advancement",
	}, http.StatusCreated, &pin)
	updated, err := updateManagedModuleWithResponse(t, cp, orgID, moduleID, platformorchestratorcp.ModuleUpdateBody{
		ModuleSource: ref.Ref("inline"), ModuleSourceCode: ref.Ref(source), ModuleInputs: ref.Ref(map[string]interface{}{"value": "advanced"}),
	}, "value")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, updated.StatusCode(), string(updated.Body))
	deploy("initial", nil)
	var catalogue struct {
		ResourceVersion int64 `json:"resource_version"`
	}
	changeArchive := func(action string) {
		coreTestJSON(t, http.MethodGet, basePath+"/modules/"+moduleID+"/catalogue", nil, http.StatusOK, &catalogue)
		coreTestJSON(t, http.MethodPost, basePath+"/modules/"+moduleID+"/catalogue/actions/"+action, map[string]any{
			"expected_resource_version": catalogue.ResourceVersion, "reason": "Verify exact existing Environment carry-forward",
		}, http.StatusOK, nil)
	}
	changeArchive("archive")
	deploy("initial", nil)
	changeArchive("unarchive")
	coreTestJSON(t, http.MethodPost, basePath+"/module-version-pins/"+pin.ID+"/notes", map[string]any{"note": "Keep protected through the demonstration"}, http.StatusCreated, nil)
	before := pin
	coreTestJSON(t, http.MethodGet, basePath+"/module-version-pins/"+pin.ID, nil, http.StatusOK, &pin)
	assert.Equal(t, before, pin, "append-only notes cannot change protection, version, or activation boundary")
	coreTestJSON(t, http.MethodPost, basePath+"/module-version-pins/"+pin.ID+"/actions/unpin", map[string]any{
		"expected_resource_version": pin.ResourceVersion, "reason": "Adopt the new Default deliberately",
	}, http.StatusOK, nil)
	deploy("advanced", map[string]string{moduleID: "1.0.1"})
	changeArchive("archive")
	deploy("advanced", nil)
	newEnvironment := MustCreateEnv(t, cp, orgID, envType.Id, project.Id, "new-adoption")
	rejected, err := dp.CreateDeploymentWithResponse(t.Context(), orgID, &serverclient.CreateDeploymentParams{}, serverclient.DeploymentCreateBody{
		ProjectId: project.Id, EnvId: newEnvironment.Id, Mode: serverclient.DeploymentCreateBodyModeDeploy, Manifest: &manifest,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, rejected.StatusCode(), "archived Modules must not be adopted into a new Environment")
	changeArchive("unarchive")
	coreTestJSON(t, http.MethodGet, basePath+"/modules/"+moduleID+"/versions/1.0.1", nil, http.StatusOK, &detail)
	t.Logf("active Pin fixture: org=%s project_uuid=%s environment_uuid=%s module_uuid=%s version_uuid=%s", orgID, project.Uuid, environment.Uuid, detail.Version.ModuleUUID, detail.Version.UUID)
}

func coreTestJSON(t *testing.T, method, path string, body any, expectedStatus int, result any) {
	t.Helper()
	pinActorJSON(t, uuid.Nil, method, path, body, "", expectedStatus, result)
}
