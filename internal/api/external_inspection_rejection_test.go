package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestExternalInspectionRejectionPropagatesResponseWriteFailure(t *testing.T) {
	var adapter *ExternalDeploymentHTTPAdapter
	for name, handler := range map[string]echo.HandlerFunc{"readiness": adapter.HandleReadiness, "plan": adapter.HandlePlan} {
		t.Run(name, func(t *testing.T) {
			failure := errors.New("response transport failed")
			writer := &inspectionFailedWriter{ResponseRecorder: httptest.NewRecorder(), failure: failure}
			c := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/", nil), writer)
			require.ErrorIs(t, handler(c), failure)
			require.Equal(t, http.StatusServiceUnavailable, writer.Code)
			require.Equal(t, 1, writer.writes)
		})
	}
}

func TestExternalInspectionUnrelatedHTTPErrorRemainsRedacted(t *testing.T) {
	const private = "private-downstream-material"
	for name, message := range map[string]any{"string": private, "object": map[string]any{"secret": private}} {
		t.Run(name, func(t *testing.T) {
			response := httptest.NewRecorder()
			c := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/", nil), response)
			adapter := &ExternalDeploymentHTTPAdapter{}
			err := fmt.Errorf("downstream failed: %w", echo.NewHTTPError(http.StatusBadRequest, message))
			require.NoError(t, adapter.writeInspectionError(c, err))
			require.Equal(t, http.StatusInternalServerError, response.Code)
			require.NotContains(t, response.Body.String(), private)
			var rejected externalDeploymentErrorResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &rejected))
			require.Equal(t, externalDeploymentErrorResponse{Error: externalDeploymentFailedCode, Message: "external deployment inspection failed"}, rejected)
		})
	}
}

type inspectionFailedWriter struct {
	*httptest.ResponseRecorder
	failure error
	writes  int
}

func (w *inspectionFailedWriter) Write([]byte) (int, error) {
	w.writes++
	return 0, w.failure
}
