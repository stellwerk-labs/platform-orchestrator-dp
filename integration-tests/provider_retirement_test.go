//go:build admissionintegration

package integrationtests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	dp "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/genclient"
)

func TestProviderRetirementLifecycle(t *testing.T) {
	control := MustControlPlaneClient(t)
	org := MustCreateOrgId(t, MustInternalControlPlaneClient(t))
	project := MustCreateProject(t, control, org, "provider-retirement")
	envType := MustCreateEnvType(t, control, org, "development")
	MustCreateRunnerWithRule(t, control, org, "retirement-runner", project.Id, envType.Id, nil)
	environment := MustCreateEnv(t, control, org, envType.Id, project.Id, "existing")
	actor := mustPinScopePrincipal(t, org, pinScopeGrant{Scope: "env:" + environment.Uuid.String(), Permissions: []string{"deployment_write"}})
	data := MustDataPlaneClientWithUserId(t, actor.String())
	resourceType := MustCreateResourceType(t, control, org, "retirement-value")
	const module, providerType, providerID = "retirement-module", "faulty", "cleanup"
	providerBody := cp.ModuleProviderCreateBody{
		ProviderType: providerType, Id: providerID, Source: "astromechza/faulty",
		VersionConstraint: "= 0.1.0", Configuration: map[string]interface{}{"required_boolean": true},
	}
	createdProvider, err := control.CreateModuleProviderWithResponse(t.Context(), org, providerBody)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, createdProvider.StatusCode(), string(createdProvider.Body))
	createdModule, err := createManagedModuleWithResponse(t, control, org, cp.ModuleCreateBody{
		Id: module, ResourceType: resourceType.Id, ModuleSource: "inline", ModuleSourceCode: ref.Ref(removalModuleSource),
		ProviderMapping: map[string]string{"faulty": "faulty.cleanup"},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, createdModule.StatusCode(), string(createdModule.Body))
	rule, err := control.CreateModuleRuleInOrgWithResponse(t.Context(), org, cp.RuleCreateBody{ModuleId: module})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, rule.StatusCode(), string(rule.Body))
	_, recipient := GetAgeIdentityAndRecipient(t)
	manifest := dp.DeploymentManifest{Workloads: map[string]dp.DeploymentManifestWorkload{}, Shared: map[string]dp.DeploymentManifestResource{"value": {Type: resourceType.Id}}}
	body := dp.DeploymentCreateBody{ProjectId: project.Id, EnvId: environment.Id, Mode: dp.DeploymentCreateBodyModeDeploy, Manifest: &manifest, EncryptedOutputsRecipient: ref.Ref(recipient)}
	deploy := func(request dp.DeploymentCreateBody, status string) *dp.Deployment {
		t.Helper()
		response, err := data.CreateDeploymentWithResponse(t.Context(), org, &dp.CreateDeploymentParams{}, request)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode(), string(response.Body))
		result := MustWaitForDeploymentComplete(t, data, org, response.JSON201.Id)
		require.Equal(t, status, result.Status, result.StatusMessage)
		return result
	}
	base := deploy(body, "succeeded")

	retire := func() {
		t.Helper()
		response, err := control.DeleteModuleProviderWithResponse(t.Context(), org, providerType, providerID)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, response.StatusCode(), string(response.Body))
	}
	expectedConfiguration := providerBody.Configuration
	readPending := func() time.Time {
		t.Helper()
		response, err := control.GetModuleProviderWithResponse(t.Context(), org, providerType, providerID)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode(), string(response.Body))
		var provider struct {
			DeletionRequestedAt *time.Time             `json:"deletion_requested_at"`
			Configuration       map[string]interface{} `json:"configuration"`
		}
		require.NoError(t, json.Unmarshal(response.Body, &provider))
		require.NotNil(t, provider.DeletionRequestedAt)
		require.Equal(t, expectedConfiguration, provider.Configuration)
		return *provider.DeletionRequestedAt
	}
	retire()
	marked := readPending()
	retire()
	require.Equal(t, marked, readPending())
	recreated, err := control.CreateModuleProviderWithResponse(t.Context(), org, providerBody)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, recreated.StatusCode(), string(recreated.Body))

	// A repeated desired-state use is rejected even though its Module remains
	// immutable and its Provider configuration is still physically present.
	blocked := body
	blocked.Mode, blocked.Manifest, blocked.RollbackToDeploymentId = dp.DeploymentCreateBodyModeRollback, nil, &base.Id
	response, err := data.CreateDeploymentWithResponse(t.Context(), org, &dp.CreateDeploymentParams{}, blocked)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, response.StatusCode(), string(response.Body))

	updated, err := updateManagedModuleWithResponse(t, control, org, module, cp.ModuleUpdateBody{
		ModuleSource: ref.Ref("inline"), ModuleSourceCode: ref.Ref(`output "platform_orchestrator_metadata" { value = {} }`),
		ProviderMapping: ref.Ref(map[string]string{}),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, updated.StatusCode(), string(updated.Body))
	state := func() []byte {
		t.Helper()
		secret, err := MustCreateK8sClient(t).CoreV1().Secrets("platform-orchestrator-runner").Get(t.Context(), "tfstate-default-"+environment.Uuid.String(), metav1.GetOptions{})
		require.NoError(t, err)
		return bytes.Clone(secret.Data["tfstate"])
	}
	before := state()
	planned := body
	planned.PlanOnly = ref.Ref(true)
	deploy(planned, "succeeded")
	require.Equal(t, before, state(), "plan-only is not cleanup evidence")
	readPending()
	// An invalid cleanup configuration must leave the Provider and its state
	// available for repair. A successful retry is the only release evidence.
	setConfiguration := func(config map[string]interface{}) {
		t.Helper()
		response, err := control.UpdateModuleProviderWithResponse(t.Context(), org, providerType, providerID, cp.ModuleProviderUpdateBody{Configuration: &config})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode(), string(response.Body))
		expectedConfiguration = config
	}
	setConfiguration(map[string]interface{}{})
	failed, err := data.CreateDeploymentWithResponse(t.Context(), org, &dp.CreateDeploymentParams{}, body)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, failed.StatusCode(), string(failed.Body))
	require.Contains(t, string(failed.Body), "no longer matches")
	require.Equal(t, before, state(), "rejected cleanup cannot change actual state")
	readPending()
	setConfiguration(providerBody.Configuration)
	cleaned := deploy(body, "succeeded")
	require.NotEqual(t, base.Id, cleaned.Id)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		result, err := control.GetModuleProviderWithResponse(t.Context(), org, providerType, providerID)
		require.NoError(c, err)
		require.Equal(c, http.StatusNotFound, result.StatusCode())
	}, time.Minute, time.Second)

	recreated, err = control.CreateModuleProviderWithResponse(t.Context(), org, providerBody)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, recreated.StatusCode(), string(recreated.Body))
	deleted, err := control.DeleteEnvironmentWithResponse(t.Context(), org, project.Id, environment.Id, &cp.DeleteEnvironmentParams{})
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, deleted.StatusCode(), string(deleted.Body))
}
