package api

import (
	"net/http"
	"testing"

	"github.com/stellwerk-labs/golib/herrors"
	"github.com/stretchr/testify/require"
)

func TestDeploymentAdmissionDeniedResponsePreservesDenialClass(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
	}{
		{"invalid request", http.StatusBadRequest},
		{"lost permission", http.StatusForbidden},
		{"scoped absence", http.StatusNotFound},
		{"Pin conflict", http.StatusConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			response, err := deploymentAdmissionDeniedResponse(test.status)
			switch test.status {
			case http.StatusBadRequest:
				require.NoError(t, err)
				_, ok := response.(CreateDeployment400JSONResponse)
				require.True(t, ok)
			case http.StatusForbidden:
				require.Nil(t, response)
				var forbidden *herrors.PlatformOrchestratorError
				require.ErrorAs(t, err, &forbidden)
				require.Equal(t, http.StatusForbidden, forbidden.StatusCode)
			case http.StatusNotFound, http.StatusConflict:
				require.NoError(t, err)
				_, ok := response.(CreateDeployment409JSONResponse)
				require.True(t, ok)
			}
		})
	}
}
