package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	platformorchestratorcpv3 "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/plugincontext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoreExternalDeploymentGrantClientUsesBearerAndExactIntent(t *testing.T) {
	operationID := uuid.New()
	projectUUID := uuid.New()
	environmentUUID := uuid.New()
	actorID := uuid.NewString()
	claims := plugincontext.Claims{OrgID: "acme", PluginID: "dev.example.rollouts", ActorID: actorID}
	grant := ExternalDeploymentGrantRequest{
		OperationRef: operationID.String(), ServiceAction: ExternalDeploymentAction, Fence: 7,
		DispatchID: "dispatch-1", RequestDigest: stringOf('a', 64), ActorID: actorID,
		ProjectUUID: projectUUID, EnvUUID: environmentUUID,
	}
	testBearer := strings.Join([]string{"scoped", "dp", "token"}, "-")

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		assert.Equal(t, http.MethodPost, request.Method)
		assert.Equal(t, "/orgs/acme/plugins/dev.example.rollouts/operations/"+operationID.String()+"/check", request.URL.Path)
		assert.Equal(t, "Bearer "+testBearer, request.Header.Get("Authorization"))
		var body platformorchestratorcpv3.PluginDispatchCheck
		if assert.NoError(t, json.NewDecoder(request.Body).Decode(&body)) {
			assert.Equal(t, grant.Fence, body.Fence)
			assert.Equal(t, grant.ServiceAction, string(body.Action))
			assert.Equal(t, grant.DispatchID, body.DispatchId)
			assert.Equal(t, grant.RequestDigest, body.RequestDigest)
		}
		response.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(response).Encode(platformorchestratorcpv3.PluginDispatchAuthorization{
			OperationId: operationID, Fence: grant.Fence, Action: grant.ServiceAction,
			DispatchId: grant.DispatchID, PluginId: claims.PluginID, OrgId: claims.OrgID,
			ProjectUuid: projectUUID, EnvironmentUuid: environmentUUID,
			RequestDigest: grant.RequestDigest, ActorId: uuid.MustParse(actorID),
		}))
	}))
	t.Cleanup(server.Close)
	baseURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	client := CoreExternalDeploymentGrantClient{BaseURL: baseURL, HTTPClient: server.Client(), BearerToken: testBearer}
	authorized, err := client.AuthorizeExternalDeployment(context.Background(), claims, grant)
	require.NoError(t, err)
	require.Equal(t, operationID.String(), authorized.OperationRef)
	require.Equal(t, actorID, authorized.ActorID)
}

func TestCoreExternalDeploymentGrantClientFailsClosedOnResponseMismatch(t *testing.T) {
	operationID := uuid.New()
	projectUUID := uuid.New()
	environmentUUID := uuid.New()
	claims := plugincontext.Claims{OrgID: "acme", PluginID: "dev.example.rollouts", ActorID: uuid.NewString()}
	grant := ExternalDeploymentGrantRequest{
		OperationRef: operationID.String(), ServiceAction: ExternalDeploymentAction, Fence: 7,
		DispatchID: "dispatch-1", RequestDigest: stringOf('b', 64), ActorID: claims.ActorID,
		ProjectUUID: projectUUID, EnvUUID: environmentUUID,
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(platformorchestratorcpv3.PluginDispatchAuthorization{
			OperationId: operationID, Fence: grant.Fence, Action: ExternalDeploymentRollbackAction,
			DispatchId: grant.DispatchID, PluginId: claims.PluginID, OrgId: claims.OrgID,
			ProjectUuid: projectUUID, EnvironmentUuid: environmentUUID,
			RequestDigest: grant.RequestDigest, ActorId: uuid.MustParse(claims.ActorID),
		})
	}))
	t.Cleanup(server.Close)
	baseURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	client := CoreExternalDeploymentGrantClient{BaseURL: baseURL, HTTPClient: server.Client(), BearerToken: "token"}
	_, err = client.AuthorizeExternalDeployment(context.Background(), claims, grant)
	require.ErrorIs(t, err, ErrExternalDeploymentUnauthorized)
}

func stringOf(value byte, count int) string {
	data := make([]byte, count)
	for index := range data {
		data[index] = value
	}
	return string(data)
}
