package api

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestExternalDeploymentRecoveryFieldsPreserveLegacySignedBytes(t *testing.T) {
	id := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	body := ExternalDeploymentAdmissionBody{OperationRef: id, Fence: 1, ProjectId: "project", EnvId: "env", ProjectUuid: id, EnvironmentUuid: id,
		Mode: ExternalDeploymentAdmissionBodyModeRollback, Rollback: &ExternalDeploymentAdmissionRollback{ForwardDeploymentId: id}}
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	expected := `{"env_id":"env","environment_uuid":"11111111-1111-4111-8111-111111111111",` +
		`"fence":1,"mode":"rollback","operation_ref":"11111111-1111-4111-8111-111111111111",` +
		`"project_id":"project","project_uuid":"11111111-1111-4111-8111-111111111111",` +
		`"rollback":{"forward_deployment_id":"11111111-1111-4111-8111-111111111111"}}`
	require.Equal(t, expected, string(encoded))
	zero := uuid.Nil
	body.FirstForwardDeploymentId = &zero
	_, _, err = validateExternalDeploymentBody(body)
	require.ErrorContains(t, err, "first_forward_deployment_id")
	body.FirstForwardDeploymentId = nil
	body.Rollback.RetryOfRollbackDeploymentId = &zero
	_, _, err = validateExternalDeploymentBody(body)
	require.ErrorContains(t, err, "retry_of_rollback_deployment_id")
}

func TestValidateExternalDeploymentBodyDerivesServiceActionFromMode(t *testing.T) {
	projectUUID, environmentUUID := uuid.New(), uuid.New()
	moduleUUID, versionUUID := uuid.New(), uuid.New()
	deployMode, deployAction, err := validateExternalDeploymentBody(ExternalDeploymentAdmissionBody{
		Mode: ExternalDeploymentAdmissionBodyModeDeploy, ProjectId: "project", EnvId: "env",
		ProjectUuid: projectUUID, EnvironmentUuid: environmentUUID,
		Plan: &ExternalDeploymentAdmissionPlan{
			SnapshotId: uuid.New(), Fingerprint: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ExpectedModuleVersions: map[string]uuid.UUID{moduleUUID.String(): versionUUID},
		},
	})
	require.NoError(t, err)
	require.Equal(t, ExternalDeploymentAction, deployAction)
	require.Equal(t, "deploy", string(deployMode))

	rollbackMode, rollbackAction, err := validateExternalDeploymentBody(ExternalDeploymentAdmissionBody{
		Mode: ExternalDeploymentAdmissionBodyModeRollback, ProjectId: "project", EnvId: "env",
		ProjectUuid: projectUUID, EnvironmentUuid: environmentUUID,
		Rollback: &ExternalDeploymentAdmissionRollback{ForwardDeploymentId: uuid.New()},
	})
	require.NoError(t, err)
	require.Equal(t, ExternalDeploymentRollbackAction, rollbackAction)
	require.Equal(t, "rollback", string(rollbackMode))
}

func TestValidateExternalDeploymentBodyRejectsUnfrozenDeploy(t *testing.T) {
	_, _, err := validateExternalDeploymentBody(ExternalDeploymentAdmissionBody{
		Mode: ExternalDeploymentAdmissionBodyModeDeploy, ProjectId: "project", EnvId: "env",
		ProjectUuid: uuid.New(), EnvironmentUuid: uuid.New(),
	})
	require.EqualError(t, err, "deploy requires one valid immutable plan and no rollback reference")
}
