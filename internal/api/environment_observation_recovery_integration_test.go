//go:build integration

package api

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/plugincontext"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type observationRecoveryFixture struct {
	*queueFixture
	router  *echo.Echo
	private ed25519.PrivateKey
	runID   uuid.UUID
	body    observation.ObservationRedeployRequest
}

func newObservationRecoveryFixture(t *testing.T) observationRecoveryFixture {
	t.Helper()
	f := newQueueFixture(t, DeploymentQueueConfig{})
	ordinary := f.mustDeploy(queueBody(nil))
	f.finish(ordinary.Id, model.DeploymentStatusSucceeded)
	now := time.Now().UTC()
	request := observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: queueProject, EnvId: queueEnv, ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, RequestId: uuid.New(), Occurrence: now, AdmissionDeadline: now.Add(time.Minute)}
	receipt, _, err := model.TryStartObservation(f.ctx, f.store, queueOrg, "dev.example.plugin", f.actor, request, queueRunner, 2, true)
	require.NoError(t, err)
	result := ObservationResult{SchemaVersion: "1", Complete: true, Tool: "opentofu", ToolVersion: "1.10.0", ObservedChanges: []ObservationChange{}, ProposedChanges: []ObservationChange{{Id: "r1", ResourceType: "test_resource", Actions: []ObservationChangeActions{"update"}}}}
	require.NoError(t, f.server.ApplyRunnerDeploymentResult(f.ctx, queueOrg, queueRunner, *receipt.RunId, receipt.RunId.String()+":deployment-result", DeploymentResultsUpdateBody{Status: Success, ObservationResult: &result}))
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	adapter := &ExternalDeploymentHTTPAdapter{Issuer: "core", VerificationKey: public, Admission: ExternalDeploymentAdmissionService{Database: f.store}, OrdinaryAdmissions: f.server.DeploymentAdmissions, ControlPlaneClient: f.coreClient, EventPublisher: f.server.EventPublisher, Logger: zap.NewNop()}
	router := echo.New()
	router.POST(ObservationRedeployPath, adapter.HandleObservationRedeploy)
	router.POST(ObservationRedeployResolvePath, adapter.HandleObservationRedeployResolve)
	router.POST(ObservationRequestResolvePath, adapter.HandleObservationRequestResolve)
	router.POST(ObservationStartPath, adapter.HandleObservationStart)
	return observationRecoveryFixture{
		queueFixture: f,
		router:       router,
		private:      private,
		runID:        *receipt.RunId,
		body:         observation.ObservationRedeployRequest{RequestId: uuid.New(), ProjectId: queueProject, EnvId: queueEnv, ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, BaselineId: ordinary.Id, BaselineFingerprint: *receipt.BaselineFingerprint},
	}
}

func (f observationRecoveryFixture) request(t *testing.T, path, action string, body any, actor uuid.UUID) *http.Request {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	var requestID uuid.UUID
	switch value := body.(type) {
	case observation.ObservationRequest:
		requestID = value.RequestId
	case observation.ObservationRedeployRequest:
		requestID = value.RequestId
	case observation.ObservationRedeployResolveRequest:
		requestID = value.Request.RequestId
	default:
		t.Fatal("unsupported recovery test request")
	}
	now := time.Now()
	assertion, err := plugincontext.Sign(
		f.private,
		plugincontext.Claims{
			Version:     1,
			Issuer:      "core",
			Audience:    ExternalDeploymentAudience,
			PluginID:    "dev.example.plugin",
			OrgID:       queueOrg,
			ActorID:     actor.String(),
			OperationID: action,
			Method:      http.MethodPost,
			Path:        path,
			BodySHA256:  plugincontext.Digest(raw),
			RequestID:   requestID.String(),
			IssuedAt:    now.Unix(),
			ExpiresAt:   now.Add(time.Minute).Unix(),
		},
		now,
	)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", requestID.String())
	request.Header.Set(plugincontext.Header, assertion)
	return request
}
func (f observationRecoveryFixture) serve(request *http.Request) *httptest.ResponseRecorder {
	out := httptest.NewRecorder()
	f.router.ServeHTTP(out, request)
	return out
}
func (f observationRecoveryFixture) forward(t *testing.T, body observation.ObservationRedeployRequest) *httptest.ResponseRecorder {
	t.Helper()
	path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/runs/" + f.runID.String() + "/redeploy"
	return f.serve(f.request(t, path, ObservationRedeployAction, body, f.actor))
}
func (f observationRecoveryFixture) resolve(t *testing.T, body observation.ObservationRedeployRequest, actor uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()
	path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/redeployments/" + body.RequestId.String() + "/resolve"
	return f.serve(f.request(t, path, ObservationRedeployResolveAction, observation.ObservationRedeployResolveRequest{RunId: f.runID, Request: body}, actor))
}

func TestObservationRedeployResolutionSealsAndAuthenticatesExactRequest(t *testing.T) {
	f := newObservationRecoveryFixture(t)
	admissions := f.core.admissions()
	sealed := f.resolve(t, f.body, f.actor)
	require.Equal(t, http.StatusOK, sealed.Code, sealed.Body.String())
	var resolution observation.ObservationRedeployResolution
	require.NoError(t, json.Unmarshal(sealed.Body.Bytes(), &resolution))
	require.Equal(t, observation.ObservationRedeployResolutionDispositionNotStarted, resolution.Disposition)
	require.Nil(t, resolution.Receipt)
	repeated := f.resolve(t, f.body, f.actor)
	require.Equal(t, sealed.Body.String(), repeated.Body.String())
	late := f.forward(t, f.body)
	require.Equal(t, http.StatusConflict, late.Code, late.Body.String())
	require.Contains(t, late.Body.String(), "observation_request_not_started")
	require.Equal(t, admissions, f.core.admissions(), "sealing and late forwards cannot ask Core to authorize a Deployment")
	changed := f.body
	changed.EnvironmentUuid = uuid.New()
	require.Equal(t, http.StatusConflict, f.resolve(t, changed, f.actor).Code)
	require.Equal(t, http.StatusConflict, f.resolve(t, f.body, uuid.New()).Code)
	path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/redeployments/" + f.body.RequestId.String() + "/resolve"
	envelope := observation.ObservationRedeployResolveRequest{RunId: f.runID, Request: f.body}
	wrongAction := f.serve(f.request(t, path, ObservationRedeployAction, envelope, f.actor))
	require.Equal(t, http.StatusUnauthorized, wrongAction.Code)
	crossScope := f.request(t, path, ObservationRedeployResolveAction, envelope, f.actor)
	crossScope.URL.Path = strings.Replace(path, "/orgs/"+queueOrg+"/", "/orgs/foreign-org/", 1)
	require.Equal(t, http.StatusUnauthorized, f.serve(crossScope).Code)
	changedRun := envelope
	changedRun.RunId = uuid.New()
	require.Equal(t, http.StatusConflict, f.serve(f.request(t, path, ObservationRedeployResolveAction, changedRun, f.actor)).Code)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var count int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployments WHERE mode='deploy'`).Scan(&count))
	require.Equal(t, 1, count, "only the actual test baseline exists; a negative seal creates no synthetic Deployment")
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`).Scan(&count))
	require.Zero(t, count)
}

func TestObservationRedeployResolutionAcceptedPreservesRunnerOwnership(t *testing.T) {
	f := newObservationRecoveryFixture(t)
	accepted := f.forward(t, f.body)
	require.Equal(t, http.StatusOK, accepted.Code, accepted.Body.String())
	var receipt observation.ObservationRedeployReceipt
	require.NoError(t, json.Unmarshal(accepted.Body.Bytes(), &receipt))
	dep, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, receipt.DeploymentId, model.GetModeDefault)
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	require.NoError(t, model.ReserveRunnerJob(f.ctx, tx, dep, 2))
	require.NoError(t, tx.Commit())
	_, err = f.store.BeginDeploymentDispatch(f.ctx, dep)
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, dep.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusFailed})
	require.NoError(t, err)
	// A retained safety resolution must not re-authorize the former actor or
	// interpret a watchdog failure as evidence that its accepted Runner stopped.
	admissions := f.core.admissions()
	f.core.set(func(core *queueCore) { core.deny[f.actor] = "actor_forbidden" })
	for range 2 {
		out := f.resolve(t, f.body, f.actor)
		require.Equal(t, http.StatusOK, out.Code, out.Body.String())
		var resolution observation.ObservationRedeployResolution
		require.NoError(t, json.Unmarshal(out.Body.Bytes(), &resolution))
		require.Equal(t, observation.ObservationRedeployResolutionDispositionAccepted, resolution.Disposition)
		require.Equal(t, dep.Id, resolution.Receipt.DeploymentId)
		require.Equal(t, observation.ObservationRedeployReceiptStatusFailed, resolution.Receipt.Status)
	}
	require.Equal(t, admissions, f.core.admissions())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var count int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM runner_job_reservations WHERE deployment_id=$1 AND released_at IS NULL`, dep.Id).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM environment_observation_request_seals`).Scan(&count))
	require.Zero(t, count)
}

func TestObservationRedeployResolutionRacesRealHTTPAdmission(t *testing.T) {
	for index := range 6 {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			f := newObservationRecoveryFixture(t)
			// Build signed requests before starting both independently handled HTTP
			// operations; the real PostgreSQL request lock arbitrates the winner.
			forwardPath := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/runs/" + f.runID.String() + "/redeploy"
			resolvePath := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/redeployments/" + f.body.RequestId.String() + "/resolve"
			forwardRequest := f.request(t, forwardPath, ObservationRedeployAction, f.body, f.actor)
			resolveRequest := f.request(t, resolvePath, ObservationRedeployResolveAction, observation.ObservationRedeployResolveRequest{RunId: f.runID, Request: f.body}, f.actor)
			start := make(chan struct{})
			var accepted, sealed *httptest.ResponseRecorder
			var group sync.WaitGroup
			group.Add(2)
			go func() { defer group.Done(); <-start; accepted = f.serve(forwardRequest) }()
			go func() { defer group.Done(); <-start; sealed = f.serve(resolveRequest) }()
			close(start)
			group.Wait()
			require.Equal(t, http.StatusOK, sealed.Code, sealed.Body.String())
			var resolution observation.ObservationRedeployResolution
			require.NoError(t, json.Unmarshal(sealed.Body.Bytes(), &resolution))
			if resolution.Disposition == observation.ObservationRedeployResolutionDispositionAccepted {
				require.Equal(t, http.StatusOK, accepted.Code, accepted.Body.String())
				var receipt observation.ObservationRedeployReceipt
				require.NoError(t, json.Unmarshal(accepted.Body.Bytes(), &receipt))
				require.Equal(t, receipt, *resolution.Receipt)
			} else {
				require.Equal(t, http.StatusConflict, accepted.Code, accepted.Body.String())
				require.Nil(t, resolution.Receipt)
				require.Contains(t, accepted.Body.String(), "observation_request_not_started")
			}
			replay := f.resolve(t, f.body, f.actor)
			require.Equal(t, sealed.Body.String(), replay.Body.String(), "lost resolution acknowledgement must replay without a new effect")
		})
	}
}

func TestObservationRequestResolutionRejectsLateSignedForwardAndScopeTampering(t *testing.T) {
	f := newObservationRecoveryFixture(t)
	now := time.Now().UTC()
	body := observation.ObservationRequest{RequestId: uuid.New(), GrantId: uuid.New(), GrantRevision: 1, ProjectId: queueProject, EnvId: queueEnv, ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, Occurrence: now, AdmissionDeadline: now.Add(time.Minute)}
	path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/requests/" + body.RequestId.String() + "/resolve"
	result := f.serve(f.request(t, path, ObservationRequestResolveAction, body, f.actor))
	require.Equal(t, http.StatusOK, result.Code, result.Body.String())
	require.JSONEq(t, `{"request_id":"`+body.RequestId.String()+`","disposition":"not_started"}`, result.Body.String())
	latePath := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/runs"
	late := f.serve(f.request(t, latePath, ObservationStartAction, body, f.actor))
	require.Equal(t, http.StatusConflict, late.Code, late.Body.String())
	require.Contains(t, late.Body.String(), "observation_request_not_started")
	require.Equal(t, result.Body.String(), f.serve(f.request(t, path, ObservationRequestResolveAction, body, f.actor)).Body.String())
	changed := body
	changed.ProjectUuid = uuid.New()
	require.Equal(t, http.StatusConflict, f.serve(f.request(t, path, ObservationRequestResolveAction, changed, f.actor)).Code)
	require.Equal(t, http.StatusConflict, f.serve(f.request(t, path, ObservationRequestResolveAction, body, uuid.New())).Code)
	crossScope := f.request(t, path, ObservationRequestResolveAction, body, f.actor)
	crossScope.URL.Path = strings.Replace(path, "dev.example.plugin", "dev.example.foreign", 1)
	require.Equal(t, http.StatusUnauthorized, f.serve(crossScope).Code)
	require.Equal(t, http.StatusUnauthorized, f.serve(f.request(t, path, ObservationStartAction, body, f.actor)).Code)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var count int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM environment_observation_receipts WHERE request_id=$1`, body.RequestId).Scan(&count))
	require.Zero(t, count, "a no-start marker is not a fabricated run or skipped execution receipt")
}
