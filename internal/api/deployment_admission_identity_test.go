package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func TestDeploymentAdmissionOptionalKeyHasIndependentStableAttemptIdentity(t *testing.T) {
	service := &OrdinaryDeploymentAdmissionService{}
	project, environment, actor := uuid.New(), uuid.New(), uuid.New()
	first := service.NewAttempt("org", project, environment, actor, model.DeploymentModeDeploy, "")
	second := service.NewAttempt("org", project, environment, actor, model.DeploymentModeDeploy, "")
	require.NotEqual(t, first.AdmissionID, second.AdmissionID, "separate unkeyed requests are not deduplicated")
	require.Equal(t, uuid.Version(4), first.AdmissionID.Version())
	require.Equal(t, uuid.Version(4), second.AdmissionID.Version())
	keyed := service.NewAttempt("org", project, environment, actor, model.DeploymentModeDeploy, first.AdmissionID.String())
	require.NotEqual(t, first.AdmissionID, keyed.AdmissionID, "a caller cannot enter the server-generated namespace")
	require.Equal(t, uuid.Version(5), keyed.AdmissionID.Version())
	identity, err := json.Marshal([]string{"ordinary-deployment-admission", "org", project.String(), environment.String(), first.AdmissionID.String()})
	require.NoError(t, err)
	require.Equal(t, uuid.NewHash(sha256.New(), deploymentAdmissionNamespace, identity, deploymentAdmissionUUIDVersion), keyed.AdmissionID, "existing keyed identity bytes remain unchanged")
	replay := service.NewAttempt("org", project, environment, actor, model.DeploymentModeDeploy, first.AdmissionID.String())
	require.Equal(t, keyed.AdmissionID, replay.AdmissionID)
	var disabled *OrdinaryDeploymentAdmissionService
	require.Nil(t, disabled.NewAttempt("org", project, environment, actor, model.DeploymentModeDeploy, ""))
}

func TestNativeAdmissionCannotUseNilAttemptOrService(t *testing.T) {
	var attempt *OrdinaryDeploymentAdmissionAttempt
	require.NoError(t, attempt.Authorize(context.Background(), nil, nil, ordinaryDeploymentCompiledIntent{}))
	compiled := ordinaryDeploymentCompiledIntent{NativeTarget: &cp.NativeExecutionTargetReference{}}
	require.ErrorIs(t, attempt.Authorize(context.Background(), nil, nil, compiled), ErrDeploymentAdmissionUnavailable)
	attempt = &OrdinaryDeploymentAdmissionAttempt{}
	require.ErrorIs(t, attempt.Authorize(context.Background(), nil, nil, compiled), ErrDeploymentAdmissionUnavailable)
}
