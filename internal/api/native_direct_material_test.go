package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestDirectBundleAuthenticatesBeforeStorage(t *testing.T) {
	server := &Server{RunnerTokenSalt: uuid.NewString()}
	for _, token := range []string{"", "invalid"} {
		response, err := server.GetDeploymentBundle(context.Background(), GetDeploymentBundleRequestObject{
			OrgId: "owned", DeploymentId: uuid.New(), Params: GetDeploymentBundleParams{XDeploymentToken: token},
		})
		require.NoError(t, err)
		require.IsType(t, GetDeploymentBundle400JSONResponse{}, response)
	}
}

func TestRetainedKeyAuthenticatesBeforeStorage(t *testing.T) {
	service := &RetainedPlanService{Server: &Server{RunnerTokenSalt: uuid.NewString()}}
	for _, token := range []string{"", "invalid"} {
		request := httptest.NewRequest(http.MethodGet, "/key", nil)
		request.Header.Set("X-Deployment-Token", token)
		ctx := echo.New().NewContext(request, httptest.NewRecorder())
		ctx.SetParamNames("orgId", "runnerId", "deploymentId")
		ctx.SetParamValues("owned", "runner", uuid.NewString())
		err := service.HandleKey(ctx)
		var failure *echo.HTTPError
		require.ErrorAs(t, err, &failure)
		require.Equal(t, http.StatusUnauthorized, failure.Code)
	}
}
