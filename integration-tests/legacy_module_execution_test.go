package integrationtests

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genclient"
)

// This is the runtime half of the upgrade contract. The CP migration suite
// separately proves migration from a populated previous schema. Here the exact
// migrated shape is exercised through real CP, DP, Kubernetes and Runner APIs.
func TestLegacyModuleExecutionAndHistoryRollback(t *testing.T) {
	cpClient := MustControlPlaneClient(t)
	dpClient := MustDataPlaneClient(t)
	orgID := MustCreateOrgId(t, MustInternalControlPlaneClient(t))
	project := MustCreateProject(t, cpClient, orgID, "legacy-upgrade")
	envType := MustCreateEnvType(t, cpClient, orgID, "development")
	MustCreateRunnerWithRule(t, cpClient, orgID, "upgrade-runner", "", "", nil)
	environment := MustCreateEnv(t, cpClient, orgID, envType.Id, project.Id, "legacy")
	resourceType, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgID, platformorchestratorcp.ResourceTypeCreateBody{
		Id: "legacy-value", OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"c": map[string]interface{}{"type": "string"}}},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resourceType.StatusCode(), string(resourceType.Body))
	const moduleID, legacyVersion, source = "legacy-module", "original-opaque-identity", "/mnt/modules/terraform-module-success"
	seedMigratedLegacyModule(t, orgID, moduleID, legacyVersion, resourceType.JSON201.Id, source)
	rule, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgID, platformorchestratorcp.RuleCreateBody{ModuleId: moduleID})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, rule.StatusCode(), string(rule.Body))
	database := MustDatabaseConn(t)
	identity, recipient := GetAgeIdentityAndRecipient(t)
	manifest := serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{
		"application": {Resources: map[string]serverclient.DeploymentManifestResource{"value": {Type: resourceType.JSON201.Id}}, Outputs: map[string]string{"VALUE": "${resources.value.outputs.c}"}},
	}}
	deploy := func(mode serverclient.DeploymentCreateBodyMode, target *uuid.UUID, expectedValue, expectedVersion, generation string) uuid.UUID {
		body := serverclient.DeploymentCreateBody{ProjectId: project.Id, EnvId: environment.Id, Mode: mode,
			EncryptedOutputsRecipient: ref.Ref(recipient), RollbackToDeploymentId: target}
		if mode == serverclient.DeploymentCreateBodyModeDeploy {
			body.Manifest = &manifest
		}
		created, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgID, &serverclient.CreateDeploymentParams{}, body)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, created.StatusCode(), string(created.Body))
		completed := MustWaitForDeploymentComplete(t, dpClient, orgID, created.JSON201.Id)
		require.Equal(t, "succeeded", completed.Status, completed.StatusMessage)
		outputs, err := dpClient.GetDeploymentEncryptedOutputsWithResponse(t.Context(), orgID, completed.Id)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, outputs.StatusCode(), string(outputs.Body))
		decoded := decryptOutputs(t, identity, []byte(outputs.JSON200.Raw))
		assert.JSONEq(t, `{"application":{"VALUE":"`+expectedValue+`"}}`, decoded)
		var encoded []byte
		require.NoError(t, database.QueryRowContext(t.Context(), `SELECT module_artifacts FROM deployments WHERE id=$1`, completed.Id).Scan(&encoded))
		var artifacts []model.ModuleArtifactRequirement
		require.NoError(t, json.Unmarshal(encoded, &artifacts))
		require.Len(t, artifacts, 1)
		assert.Equal(t, expectedVersion, artifacts[0].Version)
		assert.Equal(t, generation, artifacts[0].MigrationGeneration)
		if generation == "v0" {
			assert.Empty(t, artifacts[0].SemanticVersion)
			assert.Empty(t, artifacts[0].ArtifactDigest)
		} else {
			assert.Equal(t, expectedVersion, artifacts[0].SemanticVersion)
			assert.Empty(t, artifacts[0].ArtifactDigest, "managed external artifacts may omit the digest without inventing a claim")
		}
		return completed.Id
	}
	baseID := deploy(serverclient.DeploymentCreateBodyModeDeploy, nil, "legacy-value", legacyVersion, "v0")
	// Old persisted Deployments have no artifact manifest column content. Their
	// retained graph identity, not a fabricated historical digest, drives rollback.
	_, err = database.ExecContext(t.Context(), `UPDATE deployments SET module_artifacts='[]'::jsonb WHERE id=$1`, baseID)
	require.NoError(t, err)
	deploy(serverclient.DeploymentCreateBodyModeDeploy, nil, "legacy-value", legacyVersion, "v0")
	updated, err := updateManagedModuleWithResponse(t, cpClient, orgID, moduleID, platformorchestratorcp.ModuleUpdateBody{
		ModuleSource: ref.Ref(source), ModuleInputs: ref.Ref(map[string]interface{}{"a": "managed-", "b": "value"}),
	}, "c")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, updated.StatusCode(), string(updated.Body))
	deploy(serverclient.DeploymentCreateBodyModeDeploy, nil, "managed-value", "1.0.1", "v1")
	rollbackID := deploy(serverclient.DeploymentCreateBodyModeRollback, &baseID, "legacy-value", legacyVersion, "v0")
	assert.NotEqual(t, baseID, rollbackID)
	var originalGraph, restoredGraph []byte
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT graph FROM deployments WHERE id=$1`, baseID).Scan(&originalGraph))
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT graph FROM deployments WHERE id=$1`, rollbackID).Scan(&restoredGraph))
	assert.JSONEq(t, string(originalGraph), string(restoredGraph))
}

func seedMigratedLegacyModule(t *testing.T, orgID, moduleID, versionID, resourceType, source string) {
	t.Helper()
	connection := os.Getenv("CP_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "CP_DB_CONNECTION_STRING must identify the isolated integration Control Plane database")
	database, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	tx, err := database.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	moduleUUID, versionUUID := uuid.New(), uuid.New()
	_, err = tx.ExecContext(t.Context(), `INSERT INTO definitions (org_id,id,created_at,resource_type,latest_version_id,uuid,current_default_version_uuid)
		VALUES ($1,$2,now(),$3,$4,$5,$6)`, orgID, moduleID, resourceType, versionID, moduleUUID, versionUUID)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), `INSERT INTO definition_versions
		(org_id,definition_id,version_id,created_at,module_source,module_inputs,dependencies,coprovisioned,provider_mapping,provider_values,uuid,module_uuid,semantic_status,migration_generation)
		VALUES ($1,$2,$3,now(),$4,'{"a":"legacy-","b":"value"}','{}','[]','{}','{}',$5,$6,'default','v0')`, orgID, moduleID, versionID, source, versionUUID, moduleUUID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}
