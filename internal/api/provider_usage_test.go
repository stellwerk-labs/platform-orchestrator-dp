package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/require"
)

func TestProviderUsageRequiresInternalIdentityAndProtectedWriters(t *testing.T) {
	server := &Server{}
	request := InternalCheckProviderUsageRequestObject{OrgId: "fixture"}
	for _, scenario := range []struct {
		name, actor string
		status      int
	}{
		{"unauthenticated", "", http.StatusUnauthorized},
		{"ordinary user", userid.NewHumanUserId().String(), http.StatusForbidden},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, scenario.actor)
			_, err := server.InternalCheckProviderUsage(ctx, request)
			var response *echo.HTTPError
			require.ErrorAs(t, err, &response)
			require.Equal(t, scenario.status, response.Code)
		})
	}
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	result, err := server.InternalCheckProviderUsage(ctx, request)
	require.NoError(t, err)
	require.IsType(t, InternalCheckProviderUsage503Response{}, result, "missing admission protection is not an empty usage proof")
}
