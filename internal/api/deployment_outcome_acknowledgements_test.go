package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
)

// The server has no database: every case must be rejected before any state is
// read, so a regression that reaches storage fails with a nil dereference.
func TestOutcomeAcknowledgementRejectsUnattributableOrInvalidRequestsBeforeState(t *testing.T) {
	server, err := hecho.DefaultEchoServerWithValidation(&hecho.ValidatedServerConfig{
		AppName: "outcome-acknowledgement-test", Logger: zap.NewNop(), OpenAPIRawSchema: MustDecodeOpenApiSpec(),
		OpenAPISkipperFn: SkipGeneratedOpenAPIValidation,
	})
	require.NoError(t, err)
	server.Use(middleware.EchoCtxMiddleware)
	(&Server{Logger: zap.NewNop()}).MapRoutes(server)
	path := "/internal/orgs/acme/deployments/" + uuid.NewString() + "/actions/acknowledge-outcome"
	human := userid.NewHumanUserId().String()

	for name, test := range map[string]struct {
		from, orgToken, body string
		status               int
	}{
		"unauthenticated":       {body: `{"reason":"job deleted"}`, status: http.StatusUnauthorized},
		"system user":           {from: userid.InternalSystemUuid.String(), body: `{"reason":"job deleted"}`, status: http.StatusForbidden},
		"organization token":    {from: human, orgToken: "acme", body: `{"reason":"job deleted"}`, status: http.StatusForbidden},
		"missing reason":        {from: human, body: `{}`, status: http.StatusBadRequest},
		"empty reason":          {from: human, body: `{"reason":""}`, status: http.StatusBadRequest},
		"blank reason":          {from: human, body: `{"reason":"   "}`, status: http.StatusBadRequest},
		"oversized reason":      {from: human, body: `{"reason":"` + strings.Repeat("x", 1001) + `"}`, status: http.StatusBadRequest},
		"unknown field":         {from: human, body: `{"reason":"job deleted","force":true}`, status: http.StatusBadRequest},
		"multi-byte overlength": {from: human, body: `{"reason":"` + strings.Repeat("ü", 1001) + `"}`, status: http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			if test.from != "" {
				request.Header.Set("From", test.from)
			}
			if test.orgToken != "" {
				request.Header.Set("Po-Org-Id", test.orgToken)
			}
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			require.Equal(t, test.status, response.Code, response.Body.String())
		})
	}
}
