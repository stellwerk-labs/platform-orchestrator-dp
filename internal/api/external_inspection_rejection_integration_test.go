//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hmessaging"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/plugincontext"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func TestExternalInspectionRejectionsAreSingleJSONWithoutDownstreamEffects(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	database := externalDeploymentTestDatabase(t, ctx, connection)
	tracked := &inspectionTransactionCounter{Databaser: database}
	var coreCalls atomic.Int32
	coreServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		coreCalls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(coreServer.Close)
	client, err := cp.NewClientWithResponses(coreServer.URL)
	require.NoError(t, err)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Now().UTC()
	publisher := &hmessaging.RecordingPublisher{}
	ref := externalEnvironmentRef{ProjectID: "checkout", EnvID: "production", ProjectUUID: uuid.New(), EnvironmentUUID: uuid.New()}
	readiness, err := json.Marshal(externalDeploymentReadinessBody{Environments: []externalEnvironmentRef{ref}})
	require.NoError(t, err)
	plan, err := json.Marshal(externalDeploymentPlanBody{ProjectID: ref.ProjectID, EnvID: ref.EnvID, ProjectUUID: ref.ProjectUUID, EnvironmentUUID: ref.EnvironmentUUID,
		ModuleVersions: map[string]externalModuleSelection{"database": {ModuleUUID: uuid.New(), Version: "target", VersionUUID: uuid.New()}}})
	require.NoError(t, err)
	base := ExternalDeploymentHTTPAdapter{Issuer: "platform-orchestrator-cp", VerificationKey: public,
		Admission:          ExternalDeploymentAdmissionService{Database: tracked, Authorizer: CoreExternalDeploymentGrantClient{}, Logger: zap.NewNop()},
		ControlPlaneClient: client, EventPublisher: publisher, Now: func() time.Time { return now }}
	tests := []struct {
		name    string
		status  int
		code    string
		change  func(*ExternalDeploymentHTTPAdapter, *http.Request, *plugincontext.Claims)
		missing bool
	}{
		{name: "absent configuration", status: 503, code: externalDeploymentUnavailableCode, missing: true},
		{name: "missing database", status: 503, code: externalDeploymentUnavailableCode, change: func(a *ExternalDeploymentHTTPAdapter, _ *http.Request, _ *plugincontext.Claims) {
			a.Admission.Database = nil
		}},
		{name: "missing authorizer", status: 503, code: externalDeploymentUnavailableCode, change: func(a *ExternalDeploymentHTTPAdapter, _ *http.Request, _ *plugincontext.Claims) {
			a.Admission.Authorizer = nil
		}},
		{name: "invalid verification key", status: 503, code: externalDeploymentUnavailableCode, change: func(a *ExternalDeploymentHTTPAdapter, _ *http.Request, _ *plugincontext.Claims) {
			a.VerificationKey = nil
		}},
		{name: "missing issuer", status: 503, code: externalDeploymentUnavailableCode, change: func(a *ExternalDeploymentHTTPAdapter, _ *http.Request, _ *plugincontext.Claims) { a.Issuer = "" }},
		{name: "missing Core client", status: 503, code: externalDeploymentUnavailableCode, change: func(a *ExternalDeploymentHTTPAdapter, _ *http.Request, _ *plugincontext.Claims) {
			a.ControlPlaneClient = nil
		}},
		{name: "missing publisher", status: 503, code: externalDeploymentUnavailableCode, change: func(a *ExternalDeploymentHTTPAdapter, _ *http.Request, _ *plugincontext.Claims) {
			a.EventPublisher = nil
		}},
		{name: "query rejected", status: 400, code: externalDeploymentInvalidRequestCode, change: func(_ *ExternalDeploymentHTTPAdapter, r *http.Request, _ *plugincontext.Claims) {
			r.URL.RawQuery = "scope=foreign"
		}},
		{name: "invalid path identity", status: 400, code: externalDeploymentInvalidRequestCode, change: func(_ *ExternalDeploymentHTTPAdapter, r *http.Request, _ *plugincontext.Claims) {
			r.URL.Path = strings.Replace(r.URL.Path, "dev.example.rollouts", "dev.example!", 1)
		}},
		{name: "wrong content type", status: 415, code: "invalid_content_type", change: func(_ *ExternalDeploymentHTTPAdapter, r *http.Request, _ *plugincontext.Claims) {
			r.Header.Set("Content-Type", "text/plain")
		}},
		{name: "missing request ID", status: 400, code: "invalid_idempotency_key", change: func(_ *ExternalDeploymentHTTPAdapter, r *http.Request, _ *plugincontext.Claims) {
			r.Header.Del("Idempotency-Key")
		}},
		{name: "duplicate request ID", status: 400, code: "invalid_idempotency_key", change: func(_ *ExternalDeploymentHTTPAdapter, r *http.Request, _ *plugincontext.Claims) {
			r.Header.Add("Idempotency-Key", "other")
		}},
		{name: "missing delegation", status: 401, code: externalInvalidPluginContextCode, change: func(_ *ExternalDeploymentHTTPAdapter, r *http.Request, _ *plugincontext.Claims) {
			r.Header.Del(plugincontext.Header)
		}},
		{name: "invalid signature", status: 401, code: externalInvalidPluginContextCode, change: func(_ *ExternalDeploymentHTTPAdapter, r *http.Request, _ *plugincontext.Claims) {
			r.Header.Set(plugincontext.Header, "invalid")
		}},
		{name: "foreign audience", status: 401, code: externalInvalidPluginContextCode, change: func(_ *ExternalDeploymentHTTPAdapter, _ *http.Request, c *plugincontext.Claims) {
			c.Audience = "foreign"
		}},
		{name: "foreign organization", status: 401, code: externalInvalidPluginContextCode, change: func(_ *ExternalDeploymentHTTPAdapter, _ *http.Request, c *plugincontext.Claims) { c.OrgID = "foreign" }},
		{name: "foreign action", status: 401, code: externalInvalidPluginContextCode, change: func(_ *ExternalDeploymentHTTPAdapter, _ *http.Request, c *plugincontext.Claims) {
			c.OperationID = "external-deployment.admit"
		}},
		{name: "invalid actor", status: 401, code: externalInvalidPluginContextCode, change: func(_ *ExternalDeploymentHTTPAdapter, _ *http.Request, c *plugincontext.Claims) {
			c.ActorID = "not-a-uuid"
		}},
		{name: "expired delegation", status: 401, code: externalInvalidPluginContextCode, change: func(a *ExternalDeploymentHTTPAdapter, _ *http.Request, _ *plugincontext.Claims) {
			a.Now = func() time.Time { return now.Add(2 * plugincontext.MaxLifetime) }
		}},
		{name: "body digest mismatch", status: 401, code: externalInvalidPluginContextCode, change: func(_ *ExternalDeploymentHTTPAdapter, r *http.Request, _ *plugincontext.Claims) {
			r.Body = io.NopCloser(strings.NewReader(`{"tampered":true}`))
		}},
		{name: "oversized body", status: 400, code: externalDeploymentInvalidRequestCode, change: func(_ *ExternalDeploymentHTTPAdapter, r *http.Request, _ *plugincontext.Claims) {
			r.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", maxExternalInspectionBodyBytes+1)))
		}},
	}
	messages := map[string]string{
		externalDeploymentUnavailableCode: "external deployment inspection is unavailable",
		externalInvalidPluginContextCode:  externalInvalidPluginContextMessage,
		"invalid_content_type":            "Content-Type must be application/json",
		"invalid_idempotency_key":         "one request identifier of at most 128 characters is required",
	}
	for _, platformErrors := range []bool{false, true} {
		for _, endpoint := range []struct {
			name, route, action string
			body                []byte
		}{
			{"readiness", ExternalDeploymentReadinessPath, ExternalDeploymentReadinessAction, readiness},
			{"plan", ExternalDeploymentPlanningPath, ExternalDeploymentPlanningAction, plan},
		} {
			for _, test := range tests {
				t.Run(endpoint.name+"/"+test.name+map[bool]string{false: "/echo", true: "/platform"}[platformErrors], func(t *testing.T) {
					adapter := base
					server := echo.New()
					if platformErrors {
						server.HTTPErrorHandler = hecho.CustomHTTPErrorHandler
					}
					selected := &adapter
					if test.missing {
						selected = nil
					}
					server.POST(ExternalDeploymentReadinessPath, selected.HandleReadiness)
					server.POST(ExternalDeploymentPlanningPath, selected.HandlePlan)
					path := strings.ReplaceAll(strings.ReplaceAll(endpoint.route, ":orgId", "acme"), ":pluginId", "dev.example.rollouts")
					request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(endpoint.body))
					request.Header.Set("Content-Type", "application/json")
					request.Header.Set("Idempotency-Key", "inspection-rejection")
					claims := plugincontext.Claims{Version: 1, Issuer: base.Issuer, Audience: ExternalDeploymentAudience, PluginID: "dev.example.rollouts", OrgID: "acme", ActorID: uuid.NewString(), OperationID: endpoint.action,
						Method: http.MethodPost, Path: path, BodySHA256: plugincontext.Digest(endpoint.body), RequestID: "inspection-rejection", IssuedAt: now.Unix(), ExpiresAt: now.Add(plugincontext.MaxLifetime).Unix()}
					request.Header.Set(plugincontext.Header, "to-be-signed")
					if test.change != nil {
						test.change(&adapter, request, &claims)
					}
					if request.Header.Get(plugincontext.Header) == "to-be-signed" {
						signed, signErr := plugincontext.Sign(private, claims, now)
						require.NoError(t, signErr)
						request.Header.Set(plugincontext.Header, signed)
					}
					response := httptest.NewRecorder()
					server.ServeHTTP(response, request)
					require.Equal(t, test.status, response.Code)
					require.Contains(t, response.Header().Get("Content-Type"), "application/json")
					decoder := json.NewDecoder(response.Body)
					decoder.DisallowUnknownFields()
					var rejected externalDeploymentErrorResponse
					require.NoError(t, decoder.Decode(&rejected))
					require.Equal(t, test.code, rejected.Error)
					message := messages[test.code]
					switch test.name {
					case "query rejected":
						message = externalQueryUnsupportedMessage
					case "invalid path identity":
						message = externalInvalidPathIdentityMessage
					case "invalid actor":
						message = externalInvalidActorMessage
					case "oversized body":
						message = "request body is invalid or too large"
					}
					require.NotEmpty(t, message)
					require.Equal(t, message, rejected.Message)
					require.ErrorIs(t, decoder.Decode(new(any)), io.EOF, "exactly one machine-readable response")
					require.Zero(t, tracked.begins.Load())
					require.Zero(t, coreCalls.Load())
					require.Empty(t, publisher.Messages())
				})
			}
		}
	}
	require.Zero(t, tracked.begins.Load())
	require.Zero(t, coreCalls.Load())
	require.Empty(t, publisher.Messages())
	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var rows int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM deployments) + (SELECT count(*) FROM external_deployment_plan_snapshots) + (SELECT count(*) FROM external_operation_deployments)`).Scan(&rows))
	require.Zero(t, rows)
}

type inspectionTransactionCounter struct {
	model.Databaser
	begins atomic.Int32
}

func (d *inspectionTransactionCounter) BeginTx(ctx context.Context, opts *sql.TxOptions) (model.TxWithCommit, error) {
	d.begins.Add(1)
	return d.Databaser.BeginTx(ctx, opts)
}
