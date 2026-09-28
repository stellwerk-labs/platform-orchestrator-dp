package api

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

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
