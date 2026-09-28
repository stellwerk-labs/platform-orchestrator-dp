package api

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPrepareExternalDeploymentDerivesServiceActionFromMode(t *testing.T) {
	deployMode, deployAction, _, err := prepareExternalDeployment(DeploymentCreateBody{
		Mode: DeploymentCreateBodyModeDeploy, ProjectId: "project", EnvId: "env",
		Manifest: &DeploymentManifest{Workloads: map[string]DeploymentManifestWorkload{}},
	})
	require.NoError(t, err)
	require.Equal(t, ExternalDeploymentAction, deployAction)
	require.Equal(t, "deploy", string(deployMode))

	rollbackID := uuid.New()
	rollbackMode, rollbackAction, _, err := prepareExternalDeployment(DeploymentCreateBody{
		Mode: DeploymentCreateBodyModeRollback, ProjectId: "project", EnvId: "env", RollbackToDeploymentId: &rollbackID,
	})
	require.NoError(t, err)
	require.Equal(t, ExternalDeploymentRollbackAction, rollbackAction)
	require.Equal(t, "rollback", string(rollbackMode))
}

func TestPrepareExternalDeploymentRejectsPlanningOnMutationRoute(t *testing.T) {
	planOnly := true
	_, _, _, err := prepareExternalDeployment(DeploymentCreateBody{
		Mode: DeploymentCreateBodyModeDeploy, ProjectId: "project", EnvId: "env", PlanOnly: &planOnly,
		Manifest: &DeploymentManifest{Workloads: map[string]DeploymentManifestWorkload{}},
	})
	require.EqualError(t, err, "dry-run and plan-only requests use the separate planning contract")
}
