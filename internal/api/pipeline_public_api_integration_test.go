//go:build integration

package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipelinePublicAPIUsesStrictRoutesWithoutProxy(t *testing.T) {
	f := nativePlanFixture(t)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	path := "/orgs/" + queueOrg + "/deployments/" + plan.Id.String() + "/pipeline"
	response := f.call(http.MethodGet, path, nil, f.actor, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	requirePipelineSchema(t, "DeploymentPipelineExecution", response.Body.Bytes())
	var view DeploymentPipelineExecution
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &view))
	require.Equal(t, plan.Id, view.Progress.Id)
	require.Equal(t, f.core.envUUID, view.Progress.EnvironmentId)
	require.Equal(t, DeploymentPipelinePlanStatusStateReady, view.Plan.State)
	for _, secret := range []string{"plan_capture", "material_sha256", "command", "frozen source", "tofu", "signing", "deployment_token", "bundle", "outputs"} {
		require.NotContains(t, response.Body.String(), secret)
	}
	catalogue := f.call(http.MethodGet, "/orgs/"+queueOrg+"/projects/"+queueProject+"/envs/"+queueEnv+"/deployment-pipelines", nil, f.actor, "")
	require.Equal(t, http.StatusOK, catalogue.Code, catalogue.Body.String())
	requirePipelineSchema(t, "DeploymentPipelineCatalogue", catalogue.Body.Bytes())
	var choices DeploymentPipelineCatalogue
	require.NoError(t, json.Unmarshal(catalogue.Body.Bytes(), &choices))
	require.Equal(t, "default", choices.DefaultPipelineId)
	require.NotEmpty(t, choices.Items)
	require.True(t, choices.Items[0].Compatible)
}

func requirePipelineSchema(t *testing.T, name string, raw []byte) {
	t.Helper()
	var body any
	require.NoError(t, json.Unmarshal(raw, &body))
	spec, err := GetSwagger()
	require.NoError(t, err)
	require.NoError(t, spec.Components.Schemas[name].Value.VisitJSON(body))
}

func TestPipelinePublicAPIRefusesBypassIdentities(t *testing.T) {
	f := nativePlanFixture(t)
	id := uuid.NewString()
	base := "/orgs/" + queueOrg + "/deployments/" + id + "/pipeline"
	routes := []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, base, nil},
		{http.MethodGet, "/orgs/" + queueOrg + "/projects/" + queueProject + "/envs/" + queueEnv + "/deployment-pipelines", nil},
		{http.MethodPost, base + "/actions/apply-plan", map[string]any{"subject": "exact"}},
		{http.MethodPost, base + "/actions/replan", map[string]any{}},
	}
	for _, route := range routes {
		for _, actor := range []uuid.UUID{uuid.Nil, userid.InternalSystemUuid} {
			response := f.call(route.method, route.path, route.body, actor, "identity-trial")
			require.Equal(t, http.StatusUnauthorized, response.Code, response.Body.String())
		}
		request := f.request(route.method, route.path, route.body, f.actor, "identity-trial")
		request.Header.Set("Po-Org-Id", queueOrg)
		response := f.serve(request)
		require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	}
	require.Zero(t, f.count(`SELECT count(*) FROM deployments`))
}

func TestPipelinePublicAPIReadAndWritePermissions(t *testing.T) {
	f := nativePlanFixture(t)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	path := "/orgs/" + queueOrg + "/deployments/" + plan.Id.String() + "/pipeline"
	reader := uuid.New()
	f.iam.onlyDeploymentRead(reader)
	service := userid.NewServiceUserTokenId()
	for _, actor := range []uuid.UUID{reader, service} {
		require.Equal(t, http.StatusOK, f.call(http.MethodGet, path, nil, actor, "").Code)
	}
	for _, action := range []string{"apply-plan", "replan"} {
		body := map[string]any{}
		if action == "apply-plan" {
			body["subject"] = "exact-source-subject"
		}
		response := f.call(http.MethodPost, path+"/actions/"+action, body, reader, "reader-"+action)
		require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	}
	denied := uuid.New()
	f.iam.deny(denied, true)
	require.Equal(t, http.StatusForbidden, f.call(http.MethodGet, path, nil, denied, "").Code)
	catalogue := "/orgs/" + queueOrg + "/projects/" + queueProject + "/envs/" + queueEnv + "/deployment-pipelines"
	require.Equal(t, http.StatusForbidden, f.call(http.MethodGet, catalogue, nil, denied, "").Code)
	applied := f.call(http.MethodPost, path+"/actions/apply-plan", map[string]any{"subject": "exact-source-subject"}, service, "service-apply")
	require.Equal(t, http.StatusCreated, applied.Code, applied.Body.String())
	var child DeploymentPipelineActionResponse
	require.NoError(t, json.Unmarshal(applied.Body.Bytes(), &child))
	saved, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, child.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, service, saved.CreatedBy)
}

func TestPipelinePublicAPIKeepsScopedAbsenceAndStorageFailureDistinct(t *testing.T) {
	f := nativePlanFixture(t)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	path := "/orgs/" + queueOrg + "/deployments/" + plan.Id.String() + "/pipeline"
	require.Equal(t, http.StatusNotFound, f.call(http.MethodGet, strings.Replace(path, queueOrg, "foreign-org", 1), nil, f.actor, "").Code)
	require.Equal(t, http.StatusNotFound, f.call(http.MethodGet, "/orgs/"+queueOrg+"/deployments/"+uuid.NewString()+"/pipeline", nil, f.actor, "").Code)
	f.exec(`ALTER TABLE deployments RENAME TO deployments_unavailable`)
	response := f.call(http.MethodGet, path, nil, f.actor, "")
	require.Equal(t, http.StatusInternalServerError, response.Code, response.Body.String())
	f.exec(`ALTER TABLE deployments_unavailable RENAME TO deployments`)
}

func TestPipelinePublicAPIActionsValidateBodiesAndKeysBeforeAdmission(t *testing.T) {
	f := nativePlanFixture(t)
	base := "/orgs/" + queueOrg + "/deployments/" + uuid.NewString() + "/pipeline/actions/"
	for _, action := range []string{"apply-plan", "replan"} {
		body := map[string]any{}
		if action == "apply-plan" {
			body["subject"] = "exact"
		}
		for _, key := range []string{"", "unsafe\rkey", "unsafe\nkey", strings.Repeat("k", 129), strings.Repeat("é", 65)} {
			response := f.call(http.MethodPost, base+action, body, f.actor, key)
			require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		}
		accepted := f.call(http.MethodPost, base+action, body, f.actor, strings.Repeat("k", 128))
		require.Equal(t, http.StatusConflict, accepted.Code, accepted.Body.String())
		body["undeclared"] = true
		response := f.call(http.MethodPost, base+action, body, f.actor, "valid-key")
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	}
	for _, subject := range []string{"", strings.Repeat("s", 129)} {
		response := f.call(http.MethodPost, base+"apply-plan", map[string]any{"subject": subject}, f.actor, "valid-key")
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	}
	require.Zero(t, f.count(`SELECT count(*) FROM deployments`))
}

func TestPipelinePublicAPICatalogueUsesPathAndSelectedRunner(t *testing.T) {
	f := nativePlanFixture(t)
	environment := cp.Environment{Id: "edge-env", ProjectId: "edge-project", Uuid: uuid.New()}
	runnerID := "different-site"
	environment.RunnerId = &runnerID
	requests := []string{}
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/orgs/edge-org/projects/edge-project/envs/edge-env":
			assert.NoError(t, json.NewEncoder(w).Encode(environment))
		case "/orgs/edge-org/runners/" + runnerID:
			_, err := w.Write([]byte(`{"id":"different-site","runner_configuration":{"type":"kubernetes-agent","supported_protocols":["stellwerk.pipeline/v1"]}}`))
			assert.NoError(t, err)
		default:
			t.Errorf("unexpected Core path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer core.Close()
	client, err := cp.NewClientWithResponses(core.URL)
	require.NoError(t, err)
	f.server.ControlPlaneClient = client
	response := f.call(http.MethodGet, "/orgs/edge-org/projects/edge-project/envs/edge-env/deployment-pipelines", nil, f.actor, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var catalogue DeploymentPipelineCatalogue
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &catalogue))
	require.True(t, catalogue.Items[0].Compatible)
	require.Equal(t, []string{"/orgs/edge-org/projects/edge-project/envs/edge-env", "/orgs/edge-org/runners/different-site"}, requests)
}

func TestPipelinePublicAPICatalogueRefusesMissingOrForeignEnvironment(t *testing.T) {
	f := nativePlanFixture(t)
	for _, trial := range []struct {
		name   string
		status int
		body   string
		want   int
	}{
		{"missing", 404, `{}`, 404},
		{"Core unavailable", 503, `{}`, 502},
		{"wrong project", 200, `{"id":"environment","project_id":"other","uuid":"11111111-1111-4111-8111-111111111111"}`, 502},
		{"wrong environment", 200, `{"id":"other","project_id":"project","uuid":"11111111-1111-4111-8111-111111111111"}`, 502},
		{"zero UUID", 200, `{"id":"environment","project_id":"project","uuid":"00000000-0000-0000-0000-000000000000"}`, 502},
	} {
		t.Run(trial.name, func(t *testing.T) {
			core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/orgs/"+queueOrg+"/projects/"+queueProject+"/envs/"+queueEnv, r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(trial.status)
				_, err := w.Write([]byte(trial.body))
				assert.NoError(t, err)
			}))
			defer core.Close()
			client, err := cp.NewClientWithResponses(core.URL)
			require.NoError(t, err)
			f.server.ControlPlaneClient = client
			response := f.call(http.MethodGet, "/orgs/"+queueOrg+"/projects/"+queueProject+"/envs/"+queueEnv+"/deployment-pipelines", nil, f.actor, "")
			require.Equal(t, trial.want, response.Code, response.Body.String())
		})
	}
}

func TestPipelinePublicAPIRichOutcomesKeepPublicShape(t *testing.T) {
	f := nativePlanFixture(t)
	dep := f.mustDeploy(queueBody(nil))
	progress := model.PipelineProgress{ID: dep.Id.String(), EnvironmentID: f.core.envUUID.String(), Phase: "post_failed", CreatedAt: time.Now().UTC(), Revision: 4, InfrastructureApplied: true,
		PlanChanges: &model.PipelineChanges{Add: 1}, ApplyChanges: &model.PipelineChanges{Add: 1},
		Steps:            []model.PipelineStepResult{{ID: "policy", Phase: "plan_check", Status: "completed", Outcome: &model.PipelineStepOutcome{Verdict: "allow", Meaning: "No destructive changes", ToolID: "policy", ToolName: "Resource policy", ToolVersion: "1"}}},
		ApprovalDecision: &model.PipelineApprovalDecision{Source: "human", Decision: "approve", ActorID: f.actor.String(), DecidedAt: time.Now().UTC(), Subject: "reviewed", EvidenceDigest: "digest"},
		Cost:             &model.PipelineCostComparison{Version: "v1", Estimator: "infracost", Build: "test", Currency: "USD", Status: "complete", BeforeMonthly: "10", AfterMonthly: "12", DeltaMonthly: "2", GeneratedAt: time.Now().UTC()},
	}
	raw, err := json.Marshal(progress)
	require.NoError(t, err)
	f.exec(`UPDATE deployment_pipeline_runs SET progress=$2 WHERE deployment_id=$1`, dep.Id, raw)
	response := f.call(http.MethodGet, "/orgs/"+queueOrg+"/deployments/"+dep.Id.String()+"/pipeline", nil, f.actor, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	requirePipelineSchema(t, "DeploymentPipelineExecution", response.Body.Bytes())
	var public struct {
		Progress model.PipelineProgress `json:"progress"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &public))
	require.Equal(t, progress, public.Progress)
}

func TestPipelinePublicAPIActionsRetainSameOriginGuard(t *testing.T) {
	f := nativePlanFixture(t)
	f.server.UiHostUrl = "https://console.example/path"
	base := "/orgs/" + queueOrg + "/deployments/" + uuid.NewString() + "/pipeline/actions/"
	for _, action := range []string{"apply-plan", "replan"} {
		body := map[string]any{}
		if action == "apply-plan" {
			body["subject"] = "exact"
		}
		for _, origin := range []string{"https://attacker.example", "https://console.example"} {
			request := f.request(http.MethodPost, base+action, body, f.actor, "same-origin")
			request.Header.Set("Origin", origin)
			response := f.serve(request)
			expected := http.StatusForbidden
			if origin == "https://console.example" {
				expected = http.StatusConflict
			}
			require.Equal(t, expected, response.Code, response.Body.String())
		}
	}
	require.Zero(t, f.count(`SELECT count(*) FROM deployments`))
}

func TestPipelinePublicAPIActionsBoundBodyBeforeValidation(t *testing.T) {
	f := nativePlanFixture(t)
	server := httptest.NewServer(f.echo)
	defer server.Close()
	// A closed store makes an accidental handler call observable, not authoritative.
	require.NoError(t, f.store.Close())
	for _, action := range []string{"apply-plan", "replan"} {
		for _, unknownLength := range []bool{false, true} {
			body := strings.Repeat(" ", consoleJSONLimit) + `{}`
			request, err := http.NewRequest(http.MethodPost, server.URL+"/orgs/"+queueOrg+"/deployments/"+uuid.NewString()+"/pipeline/actions/"+action, strings.NewReader(body))
			require.NoError(t, err)
			request.Header.Set("From", f.actor.String())
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", "bounded-action")
			if unknownLength {
				request.ContentLength = -1
				request.TransferEncoding = []string{"chunked"}
			}
			response, err := server.Client().Do(request)
			require.NoError(t, err)
			raw, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusRequestEntityTooLarge, response.StatusCode, "action=%s unknownLength=%t body=%s", action, unknownLength, raw)
			require.NoError(t, response.Body.Close())
		}
	}
	f.iam.mu.Lock()
	defer f.iam.mu.Unlock()
	require.Zero(t, f.iam.calls)
}
