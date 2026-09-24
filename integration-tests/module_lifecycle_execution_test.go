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

func TestManagedModuleLifecycleExecutionBoundaries(t *testing.T) {
	cp := MustControlPlaneClient(t)
	dp := MustDataPlaneClient(t)
	orgID := MustCreateOrgId(t, MustInternalControlPlaneClient(t))
	project := MustCreateProject(t, cp, orgID, "lifecycle")
	envType := MustCreateEnvType(t, cp, orgID, "development")
	MustCreateRunnerWithRule(t, cp, orgID, "lifecycle-runner", "", "", nil)
	environment := MustCreateEnv(t, cp, orgID, envType.Id, project.Id, "existing")
	newEnvironment := MustCreateEnv(t, cp, orgID, envType.Id, project.Id, "new-adoption")
	resourceType := MustCreateResourceType(t, cp, orgID, "lifecycle-value")
	const moduleID = "lifecycle-module"
	const source = `variable "value" { type = string }
resource "terraform_data" "value" { input = var.value }
output "value" { value = terraform_data.value.output }`
	created, err := createManagedModuleWithResponse(t, cp, orgID, platformorchestratorcp.ModuleCreateBody{
		Id: moduleID, ResourceType: resourceType.Id, ModuleSource: "inline", ModuleSourceCode: ref.Ref(source), ModuleInputs: map[string]interface{}{"value": "initial"},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, created.StatusCode(), string(created.Body))
	rule, err := cp.CreateModuleRuleInOrgWithResponse(t.Context(), orgID, platformorchestratorcp.RuleCreateBody{ModuleId: moduleID})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, rule.StatusCode(), string(rule.Body))
	identity, recipient := GetAgeIdentityAndRecipient(t)
	manifest := serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{
		"application": {Resources: map[string]serverclient.DeploymentManifestResource{"value": {Type: resourceType.Id}}, Outputs: map[string]string{"VALUE": "${resources.value.outputs.value}"}},
	}}
	deploy := func(client serverclient.ClientWithResponsesInterface, envID, selected, value string, confirmations []uuid.UUID, expected int) {
		t.Helper()
		body := serverclient.DeploymentCreateBody{ProjectId: project.Id, EnvId: envID, Mode: serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &manifest, EncryptedOutputsRecipient: ref.Ref(recipient), ConfirmRestrictedModuleVersionUuids: confirmations}
		if selected != "" {
			body.ModuleVersions = map[string]string{moduleID: selected}
		}
		response, err := client.CreateDeploymentWithResponse(t.Context(), orgID, &serverclient.CreateDeploymentParams{}, body)
		require.NoError(t, err)
		require.Equal(t, expected, response.StatusCode(), string(response.Body))
		if expected != http.StatusCreated {
			return
		}
		completed := MustWaitForDeploymentComplete(t, dp, orgID, response.JSON201.Id)
		require.Equal(t, "succeeded", completed.Status, completed.StatusMessage)
		outputs, err := dp.GetDeploymentEncryptedOutputsWithResponse(t.Context(), orgID, completed.Id)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, outputs.StatusCode())
		assert.JSONEq(t, `{"application":{"VALUE":"`+value+`"}}`, decryptOutputs(t, identity, []byte(outputs.JSON200.Raw)))
	}
	basePath := "/orgs/" + orgID
	modulePath := basePath + "/modules/" + moduleID
	type versionDetail struct {
		Version struct {
			UUID            uuid.UUID `json:"uuid"`
			ModuleUUID      uuid.UUID `json:"module_uuid"`
			ResourceVersion int64     `json:"resource_version"`
		} `json:"version"`
	}
	publish := func(version, value string) {
		t.Helper()
		coreTestJSON(t, http.MethodPost, modulePath+"/versions", map[string]any{
			"semantic_version": version, "module_source": "inline", "module_source_code": source,
			"module_inputs": map[string]any{"value": value}, "module_params": map[string]any{},
			"provider_mapping": map[string]any{}, "dependencies": map[string]any{}, "coprovisioned": []any{},
			"output_schema": fixtureOutputSchema(),
		}, http.StatusCreated, nil)
	}
	transition := func(version, action string) {
		t.Helper()
		var detail versionDetail
		coreTestJSON(t, http.MethodGet, modulePath+"/versions/"+version, nil, http.StatusOK, &detail)
		coreTestJSON(t, http.MethodPost, modulePath+"/versions/"+version+"/actions/"+action, map[string]any{
			"expected_resource_version": detail.Version.ResourceVersion, "reason": "Exercise the Core lifecycle boundary",
		}, http.StatusOK, nil)
	}
	deploy(dp, environment.Id, "", "initial", nil, http.StatusCreated)
	publish("1.1.0-rc.1", "candidate")
	deploy(dp, environment.Id, "", "initial", nil, http.StatusCreated)
	ordinaryActor := mustPinScopePrincipal(t, orgID, pinScopeGrant{Scope: "env:" + environment.Uuid.String(), Permissions: []string{"deployment_write"}})
	ordinaryClient := MustDataPlaneClientWithUserId(t, ordinaryActor.String())
	deploy(ordinaryClient, environment.Id, "1.1.0-rc.1", "", nil, http.StatusForbidden)
	proposedActor := mustPinScopePrincipal(t, orgID, pinScopeGrant{Scope: "env:" + environment.Uuid.String(), Permissions: []string{"deployment_write", "module.version.use-proposed"}})
	deploy(MustDataPlaneClientWithUserId(t, proposedActor.String()), environment.Id, "1.1.0-rc.1", "candidate", nil, http.StatusCreated)
	transition("1.1.0-rc.1", "deprecate")
	deploy(dp, newEnvironment.Id, "1.1.0-rc.1", "", nil, http.StatusConflict)
	publish("1.1.0", "stable")
	transition("1.1.0", "promote")
	deploy(dp, environment.Id, "", "stable", nil, http.StatusCreated)
	deploy(dp, newEnvironment.Id, "1.0.0", "", nil, http.StatusConflict)
	var stable versionDetail
	coreTestJSON(t, http.MethodGet, modulePath+"/versions/1.1.0", nil, http.StatusOK, &stable)
	var pin struct {
		ID              string    `json:"id"`
		VersionUUID     uuid.UUID `json:"version_uuid"`
		Status          string    `json:"status"`
		ResourceVersion int64     `json:"resource_version"`
	}
	coreTestJSON(t, http.MethodPost, basePath+"/module-version-pins", map[string]any{
		"project_uuid": project.Uuid, "environment_uuid": environment.Uuid, "module_uuid": stable.Version.ModuleUUID,
		"version_uuid": stable.Version.UUID, "reason": "Protect the current effective stable version",
	}, http.StatusCreated, &pin)
	publish("1.2.0", "safer")
	transition("1.2.0", "promote")
	transition("1.1.0", "mark-defective")
	before := pin
	coreTestJSON(t, http.MethodGet, basePath+"/module-version-pins/"+pin.ID, nil, http.StatusOK, &pin)
	assert.Equal(t, before, pin, "marking the pinned version Defective must not mutate the Pin")
	deploy(dp, environment.Id, "", "", nil, http.StatusConflict)
	deploy(dp, environment.Id, "", "", []uuid.UUID{uuid.New()}, http.StatusConflict)
	confirmation := []uuid.UUID{stable.Version.UUID}
	deploy(ordinaryClient, environment.Id, "", "", confirmation, http.StatusForbidden)
	defectiveActor := mustPinScopePrincipal(t, orgID, pinScopeGrant{Scope: "env:" + environment.Uuid.String(), Permissions: []string{"deployment_write", "module.version.pin-defective"}})
	deploy(MustDataPlaneClientWithUserId(t, defectiveActor.String()), environment.Id, "", "stable", confirmation, http.StatusCreated)
	deploy(dp, newEnvironment.Id, "1.1.0", "", nil, http.StatusConflict)
	deploy(dp, newEnvironment.Id, "1.1.0", "", confirmation, http.StatusConflict)
	deploy(dp, environment.Id, "1.2.0", "", nil, http.StatusConflict)
	coreTestJSON(t, http.MethodPost, basePath+"/module-version-pins/"+pin.ID+"/actions/unpin", map[string]any{
		"expected_resource_version": pin.ResourceVersion, "reason": "Adopt the safer Default explicitly",
	}, http.StatusOK, nil)
	deploy(dp, environment.Id, "1.2.0", "safer", nil, http.StatusCreated)
}
