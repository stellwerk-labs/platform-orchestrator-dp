package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/herrors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceRefusalIsRetryableAcrossDeploymentAdapters(t *testing.T) {
	_, err := createDeploymentErrorResponse(model.ErrSystemMaintenance)
	var ordinary *herrors.PlatformOrchestratorError
	require.ErrorAs(t, err, &ordinary)
	require.Equal(t, http.StatusServiceUnavailable, ordinary.StatusCode)
	require.Equal(t, systemMaintenanceCode, ordinary.Code)
	adapter := &ExternalDeploymentHTTPAdapter{}
	for name, write := range map[string]func(echo.Context, error) error{"external": adapter.writeAdmissionError, "observation": writeObservationError} {
		t.Run(name, func(t *testing.T) {
			response := httptest.NewRecorder()
			ctx := echo.New().NewContext(httptest.NewRequest("POST", "/", nil), response)
			require.NoError(t, write(ctx, model.ErrSystemMaintenance))
			require.Equal(t, http.StatusServiceUnavailable, response.Code)
			require.Contains(t, response.Body.String(), `"error":"system_maintenance"`)
			require.NotEmpty(t, response.Header().Get("Retry-After"))
		})
	}
}
