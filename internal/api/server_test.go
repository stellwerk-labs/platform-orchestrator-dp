package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSkipGeneratedOpenAPIValidationIsExact(t *testing.T) {
	t.Parallel()

	server := echo.New()
	context := server.NewContext(httptest.NewRequest("GET", "/", nil), httptest.NewRecorder())
	for _, path := range []string{
		LegacyDeploymentAccessPath,
		"/alive", "/health", "/internal/actions/flush-pending-messages",
		ExternalDeploymentAdmissionPath,
		ExternalDeploymentReadinessPath,
		ExternalDeploymentPlanningPath,
		ExternalDeploymentReadPath,
		ExternalDeploymentDispatchReadPath,
		ExternalDeploymentLogsPath,
		ExternalDeploymentOutputsPath,
	} {
		context.SetPath(path)
		require.True(t, SkipGeneratedOpenAPIValidation(context), path)
	}

	for _, path := range []string{
		LegacyDeploymentAccessPath + "/extra",
		"/v1/orgs/:orgId/runners/:runnerId/deployments/:deploymentId/legacy-access-extra",
		CanonicalPipelinePath, PipelineCataloguePath, PipelineApplyPlanPath, CanonicalPipelineReplanPath, PipelineControlsPath, PipelineControlsPath + "/:controlId",
		"/orgs/:orgId/deployments",
		"/orgs/:orgId/deployment-pipelines",
		"/orgs/:orgId/deployment-pipelines/:pipelineId/replan",
		"/orgs/:orgId/deployment-pipeline-tools",
		ExternalDeploymentReadinessPath + "/extra",
		"/orgs/:orgId/plugins/:pluginId/external-deployment",
		"/internal/orgs/:orgId/plugins/:pluginId/external-deployment-readiness",
	} {
		context.SetPath(path)
		require.False(t, SkipGeneratedOpenAPIValidation(context), path)
	}
}

func TestFilteredManualExternalRouteReachesItsHandler(t *testing.T) {
	t.Parallel()

	server, err := hecho.DefaultEchoServerWithValidation(&hecho.ValidatedServerConfig{
		AppName: "external-route-test", Logger: zap.NewNop(), OpenAPIRawSchema: MustDecodeOpenApiSpec(),
		OpenAPISkipperFn: SkipGeneratedOpenAPIValidation,
	})
	require.NoError(t, err)
	server.POST(ExternalDeploymentReadinessPath, func(c echo.Context) error {
		return c.NoContent(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodPost,
		"/orgs/acme/plugins/dev.example.rollouts/external-deployment-readiness", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code)
}
