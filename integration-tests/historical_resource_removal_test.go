//go:build admissionintegration

package integrationtests

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	dp "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/genclient"
)

const removalModuleSource = `terraform {
 required_providers { faulty = { source = "astromechza/faulty" } }
}
resource "faulty_example" "value" { required_boolean = true }
output "platform_orchestrator_metadata" { value = {} }
`

func TestHistoricalResourceRemoval(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		name := "managed"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			control := MustControlPlaneClient(t)
			org := MustCreateOrgId(t, MustInternalControlPlaneClient(t))
			project := MustCreateProject(t, control, org, "historical-removal")
			envType := MustCreateEnvType(t, control, org, "development")
			MustCreateRunnerWithRule(t, control, org, "removal-runner", project.Id, envType.Id, nil)
			environment := MustCreateEnv(t, control, org, envType.Id, project.Id, "existing")
			actor := mustPinScopePrincipal(t, org, pinScopeGrant{Scope: "env:" + environment.Uuid.String(), Permissions: []string{"deployment_write"}})
			data := MustDataPlaneClientWithUserId(t, actor.String())
			debug := MustDataPlaneClient(t)
			resourceType := MustCreateResourceType(t, control, org, "removal-value")
			const module = "historical-removal"
			provider, err := control.CreateModuleProviderWithResponse(t.Context(), org, cp.ModuleProviderCreateBody{
				ProviderType: "faulty", Id: "cleanup", Source: "astromechza/faulty", VersionConstraint: "= 0.1.0", Configuration: map[string]interface{}{"required_boolean": true},
			})
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, provider.StatusCode(), string(provider.Body))
			if legacy {
				seedLegacyRemovalModule(t, org, module, resourceType.Id)
			} else {
				created, err := createManagedModuleWithResponse(t, control, org, cp.ModuleCreateBody{Id: module, ResourceType: resourceType.Id, ModuleSource: "inline", ModuleSourceCode: ref.Ref(removalModuleSource), ProviderMapping: map[string]string{"faulty": "faulty.cleanup"}})
				require.NoError(t, err)
				require.Equal(t, http.StatusCreated, created.StatusCode(), string(created.Body))
			}
			rule, err := control.CreateModuleRuleInOrgWithResponse(t.Context(), org, cp.RuleCreateBody{ModuleId: module})
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, rule.StatusCode(), string(rule.Body))
			_, recipient := GetAgeIdentityAndRecipient(t)
			manifest := dp.DeploymentManifest{Workloads: map[string]dp.DeploymentManifestWorkload{"application": {Resources: map[string]dp.DeploymentManifestResource{"value": {Type: resourceType.Id}}}}}
			empty := dp.DeploymentManifest{Workloads: map[string]dp.DeploymentManifestWorkload{"application": {}}}
			deploy := func(mode dp.DeploymentCreateBodyMode, plan bool, body *dp.DeploymentManifest, rollback *uuid.UUID, status string) *dp.Deployment {
				t.Helper()
				response, err := data.CreateDeploymentWithResponse(t.Context(), org, &dp.CreateDeploymentParams{}, dp.DeploymentCreateBody{
					ProjectId: project.Id, EnvId: environment.Id, Mode: mode, PlanOnly: ref.Ref(plan),
					Manifest: body, RollbackToDeploymentId: rollback, EncryptedOutputsRecipient: ref.Ref(recipient),
				})
				require.NoError(t, err)
				require.Equal(t, http.StatusCreated, response.StatusCode(), string(response.Body))
				completed := MustWaitForDeploymentComplete(t, data, org, response.JSON201.Id)
				require.Equal(t, status, completed.Status, completed.StatusMessage)
				return completed
			}
			base := deploy(dp.DeploymentCreateBodyModeDeploy, false, &manifest, nil, "succeeded")
			database := MustDatabaseConn(t)
			var originalGraph []byte
			require.NoError(t, database.QueryRowContext(t.Context(), `SELECT graph FROM deployments WHERE id=$1`, base.Id).Scan(&originalGraph))
			graph, err := graphs.FromJson(bytes.NewReader(originalGraph))
			require.NoError(t, err)
			var originalHash string
			for _, node := range graph.Nodes {
				if node.ModuleConfiguration != nil {
					originalHash = node.ModuleConfiguration.ProviderFullIdToHashVariantMapping["faulty.cleanup"]
				}
			}
			require.NotEmpty(t, originalHash)
			alias := graphs.LocalProviderName("faulty", "cleanup", originalHash)
			updated, err := updateManagedModuleWithResponse(t, control, org, module, cp.ModuleUpdateBody{ModuleSource: ref.Ref("inline"), ModuleSourceCode: ref.Ref(`output "platform_orchestrator_metadata" { value = {} }`), ProviderMapping: ref.Ref(map[string]string{})})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, updated.StatusCode(), string(updated.Body))
			catalogue, err := MustInternalControlPlaneClient(t).GenerateInternalModuleCatalogueWithResponse(t.Context(), org, project.Id, environment.Id, nil, cp.GenerateInternalModuleCatalogueJSONRequestBody{})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, catalogue.StatusCode(), string(catalogue.Body))
			require.Empty(t, catalogue.JSON200.Providers, "current selection must omit the old cleanup provider")
			for _, node := range graph.Nodes {
				if node.ModuleConfiguration != nil {
					for _, selected := range catalogue.JSON200.Modules {
						require.NotEqual(t, node.ModuleConfiguration.VersionId, selected.VersionId)
					}
				}
			}
			secretName := "tfstate-default-" + environment.Uuid.String()
			state := func() *corev1.Secret {
				t.Helper()
				secret, err := MustCreateK8sClient(t).CoreV1().Secrets("platform-orchestrator-runner").Get(t.Context(), secretName, metav1.GetOptions{})
				require.NoError(t, err)
				return secret
			}
			beforePlan := state()
			planned := deploy(dp.DeploymentCreateBodyModeDeploy, true, &empty, nil, "succeeded")
			afterPlan := state()
			require.Equal(t, beforePlan.UID, afterPlan.UID)
			require.Equal(t, beforePlan.Data, afterPlan.Data, "plan-only must preserve actual state bytes")
			require.Contains(t, MustGetDeploymentTofu(t, debug, org, planned.Id), alias)
			setProvider := func(config map[string]interface{}) {
				t.Helper()
				r, err := control.UpdateModuleProviderWithResponse(t.Context(), org, "faulty", "cleanup", cp.ModuleProviderUpdateBody{Configuration: &config})
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, r.StatusCode(), string(r.Body))
			}
			setProvider(map[string]interface{}{})
			failed := deploy(dp.DeploymentCreateBodyModeDeploy, false, &empty, nil, "failed")
			require.Contains(t, failed.StatusMessage, "required_boolean")
			setProvider(map[string]interface{}{"required_boolean": true})
			removed := deploy(dp.DeploymentCreateBodyModeDeploy, false, &empty, nil, "succeeded")
			require.Contains(t, MustGetDeploymentTofu(t, debug, org, removed.Id), alias)
			reader, err := gzip.NewReader(bytes.NewReader(state().Data["tfstate"]))
			require.NoError(t, err)
			raw, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			var remaining struct{ Resources []json.RawMessage }
			require.NoError(t, json.Unmarshal(raw, &remaining))
			require.Empty(t, remaining.Resources)
			redeployed := deploy(dp.DeploymentCreateBodyModeDeploy, false, &empty, nil, "succeeded")
			require.NotContains(t, MustGetDeploymentTofu(t, debug, org, redeployed.Id), alias, "completed tombstones must not persist")
			deploy(dp.DeploymentCreateBodyModeRollback, false, nil, &base.Id, "succeeded")
			var retainedGraph []byte
			require.NoError(t, database.QueryRowContext(t.Context(), `SELECT graph FROM deployments WHERE id=$1`, base.Id).Scan(&retainedGraph))
			require.Equal(t, originalGraph, retainedGraph, "historical deployment must remain immutable")
			deleted, err := control.DeleteEnvironmentWithResponse(t.Context(), org, project.Id, environment.Id, &cp.DeleteEnvironmentParams{})
			require.NoError(t, err)
			require.Equal(t, http.StatusAccepted, deleted.StatusCode(), string(deleted.Body))
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				r, err := control.GetEnvironmentWithResponse(t.Context(), org, project.Id, environment.Id)
				require.NoError(c, err)
				require.Equal(c, http.StatusNotFound, r.StatusCode(), string(r.Body))
			}, time.Minute, time.Second)
		})
	}
}

func seedLegacyRemovalModule(t *testing.T, org, module, resourceType string) {
	t.Helper()
	database, err := sql.Open("postgres", os.Getenv("CP_DB_CONNECTION_STRING"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	tx, err := database.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	moduleID, versionID := uuid.New(), uuid.New()
	_, err = tx.ExecContext(t.Context(), `INSERT INTO definitions (org_id,id,created_at,resource_type,latest_version_id,uuid,current_default_version_uuid) VALUES ($1,$2,now(),$3,'opaque-original',$4,$5)`, org, module, resourceType, moduleID, versionID)
	require.NoError(t, err)
	_, err = tx.ExecContext(
		t.Context(),
		`INSERT INTO definition_versions (org_id,definition_id,version_id,created_at,module_source,module_source_code,module_inputs,dependencies,coprovisioned,provider_mapping,provider_values,uuid,module_uuid,semantic_status,migration_generation) VALUES ($1,$2,'opaque-original',now(),'inline',$3,'{}','{}','[]','{"faulty":"faulty.cleanup"}','{}',$4,$5,'default','v0')`,
		org,
		module,
		removalModuleSource,
		versionID,
		moduleID,
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}
