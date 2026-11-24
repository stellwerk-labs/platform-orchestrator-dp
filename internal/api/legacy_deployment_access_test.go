package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

func legacyAccessHTTP(t *testing.T, server *Server) *echo.Echo {
	t.Helper()
	e, err := hecho.DefaultEchoServerWithValidation(&hecho.ValidatedServerConfig{
		AppName: "legacy-access-test", Logger: zap.NewNop(), OpenAPIRawSchema: MustDecodeOpenApiSpec(), OpenAPISkipperFn: SkipGeneratedOpenAPIValidation,
	})
	require.NoError(t, err)
	server.MapRoutes(e)
	return e
}

func TestLegacyDeploymentAccessAuthenticatesBeforeStorage(t *testing.T) {
	server := &Server{RunnerTokenSalt: uuid.NewString()}
	e := legacyAccessHTTP(t, server)
	id := uuid.NewString()
	token := util.GenerateHashedRunnerToken(server.RunnerTokenSalt, "O", id)
	for _, tokens := range [][]string{nil, {""}, {"invalid"}, {token, token}, {"", token}, {token + ", " + token}} {
		r := httptest.NewRequest(http.MethodGet, "/v1/orgs/O/runners/R/deployments/"+id+"/legacy-access", nil)
		for _, value := range tokens {
			r.Header.Add("X-Deployment-Token", value)
		}
		w := httptest.NewRecorder()
		e.ServeHTTP(w, r)
		require.Equal(t, http.StatusUnauthorized, w.Code, "missing storage must never be reached")
	}
}

func TestLegacyDeploymentAccessRejectsInvalidPathBeforeStorage(t *testing.T) {
	e := legacyAccessHTTP(t, &Server{})
	id := "abcdefab-cdef-4abc-8def-abcdefabcdef"
	for _, path := range []string{
		"/v1/orgs/org/runners/runner/deployments/not-a-uuid/legacy-access",
		"/v1/orgs/org/runners/runner/deployments/" + uuid.Nil.String() + "/legacy-access",
		"/v1/orgs/org/runners/runner/deployments/" + strings.ToUpper(id) + "/legacy-access",
		"/v1/orgs/org.bad/runners/runner/deployments/" + id + "/legacy-access",
		"/v1/orgs/org/runners/runner.bad/deployments/" + id + "/legacy-access",
		"/v1/orgs/org/runners/0-" + id + "/deployments/" + id + "/legacy-access",
		"/v1/orgs/org/runners/" + strings.Repeat("r", 3073) + "/deployments/" + id + "/legacy-access",
	} {
		w := httptest.NewRecorder()
		e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusBadRequest, w.Code, path)
	}
}
