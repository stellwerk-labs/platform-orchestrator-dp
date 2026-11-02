//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/plugincontext"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	platformgraph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestObservationHTTPBoundaryAndSafeRunnerCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	db := externalDeploymentTestDatabase(t, ctx, connection)
	envID, projectID, actor := uuid.New(), uuid.New(), uuid.New()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	base, err := db.CreateDeployment(
		ctx,
		tx,
		"acme",
		"p",
		"e",
		model.CreateDeploymentParams{
			CreatedBy:         actor,
			DeploymentEnvUuid: envID,
			Mode:              model.DeploymentModeDeploy,
			RunnerId:          "runner",
			RunnerLogLevel:    "info",
			Manifest:          model.EncodedDeploymentManifest(`{"workloads":{}}`),
			Graph:             model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`),
			Tofu:              model.RawTofu("terraform {}"),
		},
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = db.UpdateDeploymentStatusAndOutputs(ctx, nil, base.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/orgs/acme/projects/p/envs/e":
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": "e", "project_id": "p", "project_uuid": projectID, "uuid": envID, "runner_id": "runner", "status": "active"}))
		case "/internal/orgs/acme/runners/runner":
			_, _ = w.Write([]byte(`{"id":"runner","runner_configuration":{"type":"kubernetes-agent","max_parallel_jobs":2,"supported_protocols":["environment-observation/v1"]}}`))
		default:
			t.Errorf("unexpected Core route %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer core.Close()
	client, err := cp.NewClientWithResponses(core.URL)
	require.NoError(t, err)
	publisher := &hmessaging.RecordingPublisher{}
	adapter := &ExternalDeploymentHTTPAdapter{Issuer: "core", VerificationKey: public, Admission: ExternalDeploymentAdmissionService{Database: db}, ControlPlaneClient: client, EventPublisher: publisher, Logger: zap.NewNop()}
	router := echo.New()
	router.POST(ObservationStartPath, adapter.HandleObservationStart)
	router.GET(ObservationRunPath, adapter.HandleObservationRun)
	router.GET(ObservationRequestPath, adapter.HandleObservationRequest)
	router.GET(ObservationCapabilitiesPath, adapter.HandleObservationCapabilities)
	now := time.Now().UTC()
	body := observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: "p", EnvId: "e", ProjectUuid: projectID, EnvironmentUuid: envID, RequestId: uuid.New(), Occurrence: now, AdmissionDeadline: now.Add(time.Minute)}
	startPath := "/orgs/acme/plugins/dev.example.plugin/observations/v1/runs"
	signed := func(method, path, action, key string, raw []byte) *http.Request {
		claims := plugincontext.Claims{
			Version:     1,
			Issuer:      "core",
			Audience:    ExternalDeploymentAudience,
			PluginID:    "dev.example.plugin",
			OrgID:       "acme",
			ActorID:     actor.String(),
			OperationID: action,
			Method:      method,
			Path:        path,
			BodySHA256:  plugincontext.Digest(raw),
			RequestID:   key,
			IssuedAt:    time.Now().Unix(),
			ExpiresAt:   time.Now().Add(time.Minute).Unix(),
		}
		if key == "" {
			claims.RequestID = uuid.NewString()
		}
		assertion, err := plugincontext.Sign(private, claims, time.Now())
		require.NoError(t, err)
		r := httptest.NewRequest(method, path, bytes.NewReader(raw))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set(plugincontext.Header, assertion)
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		return r
	}
	serve := func(request *http.Request) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		return response
	}
	capabilityPath := "/orgs/acme/plugins/dev.example.plugin/observations/v1/capabilities"
	absentCapability := serve(signed(http.MethodGet, capabilityPath, ObservationCapabilitiesAction, "", nil))
	require.Equal(t, http.StatusOK, absentCapability.Code)
	require.JSONEq(t, `{"protocols":[]}`, absentCapability.Body.String())
	adapter.OrdinaryAdmissions = &OrdinaryDeploymentAdmissionService{Database: db, Core: client, BearerToken: "configured-boundary"}
	presentCapability := serve(signed(http.MethodGet, capabilityPath, ObservationCapabilitiesAction, "", nil))
	require.Equal(t, http.StatusOK, presentCapability.Code)
	require.JSONEq(t, `{"protocols":["environment-observation/v1","environment-observation-recovery/v1"]}`, presentCapability.Body.String())
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	missing := httptest.NewRequest(http.MethodPost, startPath, bytes.NewReader(raw))
	missing.Header.Set("Content-Type", "application/json")
	missing.Header.Set("Idempotency-Key", body.RequestId.String())
	require.Equal(t, http.StatusUnauthorized, serve(missing).Code)
	altered := signed(http.MethodPost, startPath, ObservationStartAction, body.RequestId.String(), raw)
	altered.Body = io.NopCloser(bytes.NewReader(append(raw, ' ')))
	require.Equal(t, http.StatusUnauthorized, serve(altered).Code)
	response := serve(signed(http.MethodPost, startPath, ObservationStartAction, body.RequestId.String(), raw))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var receipt observation.ObservationReceipt
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
	replay := serve(signed(http.MethodPost, startPath, ObservationStartAction, body.RequestId.String(), raw))
	require.Equal(t, response.Body.String(), replay.Body.String())
	resultServer := Server{Database: db, Logger: zap.NewNop(), EventPublisher: publisher}
	result := ObservationResult{SchemaVersion: "1", Complete: true, Tool: "terraform", ToolVersion: "1.9.0", ObservedChanges: []ObservationChange{}, ProposedChanges: []ObservationChange{{Id: "r1", ResourceType: "random_id", Actions: []ObservationChangeActions{"update"}}}}
	require.NoError(t, resultServer.ApplyRunnerDeploymentResult(ctx, "acme", "runner", *receipt.RunId, receipt.RunId.String()+":deployment-result", DeploymentResultsUpdateBody{Status: Success, ObservationResult: &result}))
	path := startPath + "/" + receipt.RunId.String()
	read := serve(signed(http.MethodGet, path, ObservationGetAction, "", nil))
	require.Equal(t, http.StatusOK, read.Code, read.Body.String())
	require.Contains(t, read.Body.String(), `"status":"succeeded"`)
	require.NotContains(t, read.Body.String(), "terraform {}")
	require.NotContains(t, read.Body.String(), "workloads")
	foreign := signed(http.MethodGet, path, ObservationGetAction, "", nil)
	foreign.URL.Path = strings.Replace(path, "/acme/", "/other/", 1)
	require.Equal(t, http.StatusUnauthorized, serve(foreign).Code)
	state, _, _, _, err := db.GetLastDeployment(ctx, nil, "acme", "p", "e", model.GetLastDeploymentParams{StateChangeOnly: true})
	require.NoError(t, err)
	require.Equal(t, base.Id, state.Id)
	requestPath := strings.Replace(startPath, "/runs", "/requests/"+body.RequestId.String(), 1)
	lookup := serve(signed(http.MethodGet, requestPath, ObservationRequestGetAction, "", nil))
	require.Equal(t, http.StatusOK, lookup.Code)
	require.Equal(t, response.Body.String(), lookup.Body.String())
	// New authenticated failure after a late/watchdog status releases ownership
	// while keeping the failed outcome; it cannot become a clean finding.
	body.RequestId = uuid.New()
	raw, err = json.Marshal(body)
	require.NoError(t, err)
	response = serve(signed(http.MethodPost, startPath, ObservationStartAction, body.RequestId.String(), raw))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
	dep, _, _, _, err := db.GetDeployment(ctx, nil, "acme", *receipt.RunId, model.GetModeDefault)
	require.NoError(t, err)
	_, err = db.BeginDeploymentDispatch(ctx, dep)
	require.NoError(t, err)
	_, err = db.UpdateDeploymentStatusAndOutputs(ctx, nil, dep.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusFailed})
	require.NoError(t, err)
	require.NoError(t, resultServer.ApplyRunnerDeploymentResult(ctx, "acme", "runner", dep.Id, dep.Id.String()+":deployment-result", DeploymentResultsUpdateBody{Status: Success, ObservationResult: &result}))
	stopped, _, _, _, err := db.GetDeployment(ctx, nil, "acme", dep.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, model.DeploymentStatusFailed, stopped.Status)
	// A legacy plan-only Runner can finish successfully without understanding
	// the marker, but its absent structured result is explicitly unavailable.
	body.RequestId = uuid.New()
	raw, err = json.Marshal(body)
	require.NoError(t, err)
	response = serve(signed(http.MethodPost, startPath, ObservationStartAction, body.RequestId.String(), raw))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
	require.NoError(t, resultServer.ApplyRunnerDeploymentResult(ctx, "acme", "runner", *receipt.RunId, receipt.RunId.String()+":deployment-result", DeploymentResultsUpdateBody{Status: Success}))
	unavailable := serve(signed(http.MethodGet, startPath+"/"+receipt.RunId.String(), ObservationGetAction, "", nil))
	require.Equal(t, http.StatusOK, unavailable.Code, unavailable.Body.String())
	require.Contains(t, unavailable.Body.String(), `"status":"unavailable"`)
	require.NotContains(t, unavailable.Body.String(), `"result":`)
}

func TestObservationRedeployReusesExactSnapshotThroughOrdinaryAdmission(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	ordinary := f.mustDeploy(queueBody(nil))
	f.finish(ordinary.Id, model.DeploymentStatusSucceeded)
	base, manifest, tofu, graph, err := f.store.GetDeployment(f.ctx, nil, queueOrg, ordinary.Id, model.GetModeDefault)
	require.NoError(t, err)
	request := observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: queueProject, EnvId: queueEnv, ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, RequestId: uuid.New(), Occurrence: time.Now().UTC(), AdmissionDeadline: time.Now().UTC().Add(time.Minute)}
	receipt, _, err := model.TryStartObservation(f.ctx, f.store, queueOrg, "dev.example.plugin", f.actor, request, queueRunner, 2, true)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
	result := ObservationResult{SchemaVersion: "1", Complete: true, Tool: "opentofu", ToolVersion: "1.9.0", ObservedChanges: []ObservationChange{}, ProposedChanges: []ObservationChange{{Id: "r1", ResourceType: "test_resource", Actions: []ObservationChangeActions{"update"}}}}
	require.NoError(t, f.server.ApplyRunnerDeploymentResult(f.ctx, queueOrg, queueRunner, *receipt.RunId, receipt.RunId.String()+":deployment-result", DeploymentResultsUpdateBody{Status: Success, ObservationResult: &result}))
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	adapter := &ExternalDeploymentHTTPAdapter{Issuer: "core", VerificationKey: public, Admission: ExternalDeploymentAdmissionService{Database: f.store}, OrdinaryAdmissions: f.server.DeploymentAdmissions, ControlPlaneClient: f.coreClient, EventPublisher: f.server.EventPublisher, Logger: zap.NewNop()}
	router := echo.New()
	router.POST(ObservationRedeployPath, adapter.HandleObservationRedeploy)
	router.GET(ObservationRedeployReadPath, adapter.HandleObservationRedeployRead)
	path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/runs/" + receipt.RunId.String() + "/redeploy"
	body := observation.ObservationRedeployRequest{RequestId: uuid.New(), ProjectId: queueProject, EnvId: queueEnv, ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, BaselineId: base.Id, BaselineFingerprint: *receipt.BaselineFingerprint}
	call := func(body observation.ObservationRedeployRequest, actor uuid.UUID) *httptest.ResponseRecorder {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		now := time.Now()
		assertion, err := plugincontext.Sign(
			private,
			plugincontext.Claims{
				Version:     1,
				Issuer:      "core",
				Audience:    ExternalDeploymentAudience,
				PluginID:    "dev.example.plugin",
				OrgID:       queueOrg,
				ActorID:     actor.String(),
				OperationID: ObservationRedeployAction,
				Method:      http.MethodPost,
				Path:        path,
				BodySHA256:  plugincontext.Digest(raw),
				RequestID:   body.RequestId.String(),
				IssuedAt:    now.Unix(),
				ExpiresAt:   now.Add(time.Minute).Unix(),
			},
			now,
		)
		require.NoError(t, err)
		r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", body.RequestId.String())
		r.Header.Set(plugincontext.Header, assertion)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w
	}
	// Current ordinary authorization remains mandatory even after the observation
	// grant was authorized earlier.
	deniedActor := uuid.New()
	f.core.set(func(c *queueCore) { c.deny[deniedActor] = "actor_forbidden" })
	deniedBody := body
	deniedBody.RequestId = uuid.New()
	denied := call(deniedBody, deniedActor)
	require.Equal(t, http.StatusConflict, denied.Code, denied.Body.String())
	response := call(body, f.actor)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var replay observation.ObservationRedeployReceipt
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &replay))
	require.NotEqual(t, base.Id, replay.DeploymentId)
	dep, actualManifest, actualTofu, actualGraph, err := f.store.GetDeployment(f.ctx, nil, queueOrg, replay.DeploymentId, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, model.DeploymentModeDeploy, dep.Mode)
	require.Equal(t, manifest, actualManifest)
	require.Equal(t, tofu, actualTofu)
	require.Equal(t, graph, actualGraph)
	require.Equal(t, base.ModuleArtifacts, dep.ModuleArtifacts)
	repeat := call(body, f.actor)
	require.Equal(t, http.StatusOK, repeat.Code, repeat.Body.String())
	require.Equal(t, response.Body.String(), repeat.Body.String())
	changed := body
	changed.BaselineFingerprint = "sha256:" + strings.Repeat("a", 64)
	conflict := call(changed, f.actor)
	require.Equal(t, http.StatusConflict, conflict.Code)
	require.NotContains(t, response.Body.String(), string(tofu))
	require.Equal(t, 3, f.core.admissions(), "base, denied fresh admission, and accepted redeploy, exact replay adds none")
	readPath := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/redeployments/" + body.RequestId.String()
	read := func() *httptest.ResponseRecorder {
		now := time.Now()
		assertion, err := plugincontext.Sign(
			private,
			plugincontext.Claims{
				Version:     1,
				Issuer:      "core",
				Audience:    ExternalDeploymentAudience,
				PluginID:    "dev.example.plugin",
				OrgID:       queueOrg,
				ActorID:     f.actor.String(),
				OperationID: ObservationRedeployReadAction,
				Method:      http.MethodGet,
				Path:        readPath,
				BodySHA256:  plugincontext.Digest(nil),
				RequestID:   uuid.NewString(),
				IssuedAt:    now.Unix(),
				ExpiresAt:   now.Add(time.Minute).Unix(),
			},
			now,
		)
		require.NoError(t, err)
		request := httptest.NewRequest(http.MethodGet, readPath, nil)
		request.Header.Set(plugincontext.Header, assertion)
		out := httptest.NewRecorder()
		router.ServeHTTP(out, request)
		return out
	}
	lookedUp := read()
	require.Equal(t, http.StatusOK, lookedUp.Code, lookedUp.Body.String())
	require.Equal(t, response.Body.String(), lookedUp.Body.String())
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	require.NoError(t, f.store.DeleteDeploymentsForEnv(f.ctx, tx, queueOrg, queueProject, queueEnv, true))
	require.NoError(t, tx.Commit())
	gone := read()
	require.Equal(t, http.StatusOK, gone.Code, gone.Body.String())
	require.Contains(t, gone.Body.String(), `"status":"unavailable"`)

}

// This component test uses real PostgreSQL and the existing Core bridge fixture.
// It proves retained inline replay and ordinary-gate invocation, not live IAM
// authority or Runner execution. The actual process qualification is separate.
func TestObservationInlineNoDigestSnapshotAdmitsAndReplaysThroughOrdinaryGate(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	coordinate := platformgraph.ResourceCoordinate{Type: "database", Class: "default", Id: "shared.database"}
	graph, err := graphs.ToJson(
		&platformgraph.Graph[*graphs.GraphNodeModuleConfig]{
			Nodes: map[platformgraph.ResourceCoordinate]platformgraph.ResourceNode[*graphs.GraphNodeModuleConfig]{coordinate: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "example", VersionId: "1.0.0", HasInlineSource: true}}},
			Edges: map[platformgraph.ResourceCoordinate]map[string]platformgraph.ResourceCoordinate{},
		},
	)
	require.NoError(t, err)
	artifact := model.ModuleArtifactRequirement{ModuleID: "example", Version: "1.0.0", ModuleUUID: uuid.New(), VersionUUID: uuid.New(), Source: "inline"}
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	base, err := f.store.CreateDeployment(
		f.ctx,
		tx,
		queueOrg,
		queueProject,
		queueEnv,
		model.CreateDeploymentParams{
			CreatedBy:         f.actor,
			DeploymentEnvUuid: f.core.envUUID,
			Mode:              model.DeploymentModeDeploy,
			RunnerId:          queueRunner,
			RunnerLogLevel:    "info",
			Manifest:          model.EncodedDeploymentManifest(`{"workloads":{}}`),
			Graph:             graph,
			Tofu:              model.RawTofu("terraform {}"),
			ModuleArtifacts:   []model.ModuleArtifactRequirement{artifact},
		},
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, base.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	now := time.Now().UTC()
	request := observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: queueProject, EnvId: queueEnv, ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, RequestId: uuid.New(), Occurrence: now, AdmissionDeadline: now.Add(time.Minute)}
	receipt, _, err := model.TryStartObservation(f.ctx, f.store, queueOrg, "dev.example.plugin", f.actor, request, queueRunner, 2, true)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
	result := ObservationResult{SchemaVersion: "1", Complete: true, Tool: "opentofu", ToolVersion: "1.10.0", ObservedChanges: []ObservationChange{}, ProposedChanges: []ObservationChange{{Id: "r1", ResourceType: "test_resource", Actions: []ObservationChangeActions{"update"}}}}
	require.NoError(t, f.server.ApplyRunnerDeploymentResult(f.ctx, queueOrg, queueRunner, *receipt.RunId, receipt.RunId.String()+":deployment-result", DeploymentResultsUpdateBody{Status: Success, ObservationResult: &result}))
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	adapter := &ExternalDeploymentHTTPAdapter{Issuer: "core", VerificationKey: public, Admission: ExternalDeploymentAdmissionService{Database: f.store}, OrdinaryAdmissions: f.server.DeploymentAdmissions, ControlPlaneClient: f.coreClient, EventPublisher: f.server.EventPublisher, Logger: zap.NewNop()}
	router := echo.New()
	router.POST(ObservationRedeployPath, adapter.HandleObservationRedeploy)
	fixture := observationRecoveryFixture{
		queueFixture: f,
		router:       router,
		private:      private,
		runID:        *receipt.RunId,
		body:         observation.ObservationRedeployRequest{RequestId: uuid.New(), ProjectId: queueProject, EnvId: queueEnv, ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, BaselineId: base.Id, BaselineFingerprint: *receipt.BaselineFingerprint},
	}
	deniedActor := uuid.New()
	f.core.set(func(core *queueCore) { core.deny[deniedActor] = "actor_forbidden" })
	deniedBody := fixture.body
	deniedBody.RequestId = uuid.New()
	path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/runs/" + receipt.RunId.String() + "/redeploy"
	denied := fixture.serve(fixture.request(t, path, ObservationRedeployAction, deniedBody, deniedActor))
	require.Equal(t, http.StatusConflict, denied.Code, denied.Body.String())
	accepted := fixture.forward(t, fixture.body)
	require.Equal(t, http.StatusOK, accepted.Code, accepted.Body.String())
	var replay observation.ObservationRedeployReceipt
	require.NoError(t, json.Unmarshal(accepted.Body.Bytes(), &replay))
	dep, manifest, tofu, actualGraph, err := f.store.GetDeployment(f.ctx, nil, queueOrg, replay.DeploymentId, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, model.DeploymentModeDeploy, dep.Mode)
	require.Equal(t, []model.ModuleArtifactRequirement{artifact}, dep.ModuleArtifacts)
	require.Equal(t, model.EncodedDeploymentManifest(`{"workloads":{}}`), manifest)
	require.Equal(t, model.RawTofu("terraform {}"), tofu)
	require.Equal(t, model.EncodedDeploymentGraph(graph), actualGraph)
	require.Equal(t, 2, f.core.admissions(), "denied and accepted replay each use the ordinary Core gate")
	require.Equal(t, accepted.Body.String(), fixture.forward(t, fixture.body).Body.String())
}
