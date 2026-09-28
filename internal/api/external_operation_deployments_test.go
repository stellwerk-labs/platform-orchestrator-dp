package api

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/plugincontext"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func TestValidateExternalDeploymentAdmissionRequiresDPAudienceAndAction(t *testing.T) {
	actorID := uuid.New()
	projectUUID := uuid.New()
	envUUID := uuid.New()
	valid := ExternalDeploymentAdmissionRequest{
		Claims: plugincontext.Claims{
			Audience:    ExternalDeploymentAudience,
			OperationID: ExternalDeploymentAction,
			OrgID:       "org",
			PluginID:    "dev.stellwerk.progressive-rollouts",
			ActorID:     actorID.String(),
			RequestID:   "request",
			BodySHA256:  strings.Repeat("a", 64),
		},
		OperationRef:  uuid.NewString(),
		Fence:         7,
		ServiceAction: ExternalDeploymentAction,
		ProjectUUID:   projectUUID,
		EnvUUID:       envUUID,
		ProjectID:     "project",
		EnvID:         "env",
		Create: func(context.Context, model.Tx, uuid.UUID) (*model.DeploymentSummary, []*hstandardoutbox.PendingEventMessage, error) {
			panic("validation must not execute the deployment creator")
		},
	}
	require.NoError(t, validateExternalDeploymentAdmission(valid))

	for name, mutate := range map[string]func(*ExternalDeploymentAdmissionRequest){
		"audience": func(request *ExternalDeploymentAdmissionRequest) { request.Claims.Audience = "plugin:other" },
		"action": func(request *ExternalDeploymentAdmissionRequest) {
			request.Claims.OperationID = "external-deployment.read"
		},
		"organization":   func(request *ExternalDeploymentAdmissionRequest) { request.Claims.OrgID = "" },
		"plugin":         func(request *ExternalDeploymentAdmissionRequest) { request.Claims.PluginID = "" },
		"request":        func(request *ExternalDeploymentAdmissionRequest) { request.Claims.RequestID = "" },
		"operation":      func(request *ExternalDeploymentAdmissionRequest) { request.OperationRef = "" },
		"fence":          func(request *ExternalDeploymentAdmissionRequest) { request.Fence = 0 },
		"service action": func(request *ExternalDeploymentAdmissionRequest) { request.ServiceAction = "" },
		"creator":        func(request *ExternalDeploymentAdmissionRequest) { request.Create = nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := valid
			mutate(&changed)
			_, badRequest := model.IsErrBadRequest(validateExternalDeploymentAdmission(changed))
			require.True(t, badRequest)
		})
	}
}

func TestExternalDeploymentReceiptMatchBindsAuthorizedResourceAndActor(t *testing.T) {
	actorID := uuid.New()
	request := ExternalDeploymentAdmissionRequest{
		Claims: plugincontext.Claims{
			OperationID: ExternalDeploymentAction,
			ActorID:     actorID.String(),
			BodySHA256:  strings.Repeat("b", 64),
		},
		ServiceAction: ExternalDeploymentAction,
		ProjectUUID:   uuid.New(),
		EnvUUID:       uuid.New(),
		ProjectID:     "project",
		EnvID:         "env",
	}
	authorized := AuthorizedExternalDeployment{OperationRef: uuid.NewString(), Fence: 7}
	receipt := &model.ExternalOperationDeploymentReceipt{
		OperationRef:      authorized.OperationRef,
		Fence:             authorized.Fence,
		AssertionAction:   request.Claims.OperationID,
		ServiceAction:     request.ServiceAction,
		RequestBodySHA256: request.Claims.BodySHA256,
		ActorID:           actorID.String(),
		ProjectID:         request.ProjectID,
		EnvID:             request.EnvID,
	}
	require.True(t, externalDeploymentReceiptMatches(receipt, request, actorID, authorized))

	receipt.OperationRef = "other-operation"
	require.False(t, externalDeploymentReceiptMatches(receipt, request, actorID, authorized))
}
