//go:build integration

package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hmessaging"
	coreapi "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	admissionapi "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/genclient"
	iamapi "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

const (
	queueOrg     = "queue-org"
	queueProject = "project"
	queueEnv     = "environment"
	queueRunner  = "runner"
)

// queueCore is an in-process Core with the behaviour the queue depends on:
// one unsettled Deployment admission guard per Environment, refused with the
// stable code environment_busy, plus scoped admission denials by actor.
type queueCore struct {
	t           *testing.T
	projectUUID uuid.UUID
	envUUID     uuid.UUID

	mu          sync.Mutex
	status      coreapi.EnvironmentStatus
	gone        bool
	guards      map[uuid.UUID]*queueCoreGuard
	deny        map[uuid.UUID]string
	onAuthorize func(admissionID uuid.UUID)
	epoch       int64
}

type queueCoreGuard struct {
	request admissionapi.DeploymentAdmissionRequest
	epoch   int64
	settled bool
}

func (c *queueCore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	admissions := "/orgs/" + queueOrg + "/deployment-admissions/"
	if strings.HasPrefix(r.URL.Path, admissions) {
		c.admission(w, r, strings.Split(strings.TrimPrefix(r.URL.Path, admissions), "/"))
		return
	}
	c.mu.Lock()
	status, gone := c.status, c.gone
	c.mu.Unlock()
	runner := queueRunner
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/orgs/"+queueOrg+"/projects/"+queueProject+"/envs/"+queueEnv:
		if gone {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"HTTP-404","message":"environment not found"}`))
			return
		}
		assert.NoError(c.t, json.NewEncoder(w).Encode(coreapi.Environment{Id: queueEnv, ProjectId: queueProject, ProjectUuid: &c.projectUUID, Uuid: c.envUUID, RunnerId: &runner, Status: status}))
	case r.Method == http.MethodGet && r.URL.Path == "/orgs/"+queueOrg+"/runners/"+queueRunner:
		var storage coreapi.StateStorageConfiguration
		assert.NoError(c.t, storage.FromK8sStorageConfiguration(coreapi.K8sStorageConfiguration{Namespace: "test"}))
		assert.NoError(c.t, json.NewEncoder(w).Encode(coreapi.Runner{Id: queueRunner, StateStorageConfiguration: storage}))
	case r.Method == http.MethodPost && r.URL.Path == "/internal/orgs/"+queueOrg+"/projects/"+queueProject+"/envs/"+queueEnv+"/module-catalogue":
		assert.NoError(c.t, json.NewEncoder(w).Encode(coreapi.InternalModuleCatalogue{Modules: []coreapi.InternalModuleCatalogueModule{}, Providers: []coreapi.ModuleProvider{}}))
	default:
		c.t.Errorf("unexpected Core request: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

func (c *queueCore) admission(w http.ResponseWriter, r *http.Request, parts []string) {
	if !assert.Equal(c.t, "Bearer queue-dp-token", r.Header.Get("Authorization")) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	id, err := uuid.Parse(parts[0])
	if err != nil {
		c.t.Errorf("invalid admission id %q", parts[0])
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if len(parts) == 2 {
		var settlement struct {
			admissionapi.DeploymentAdmissionRequest
			Outcome string `json:"outcome"`
		}
		if !c.decode(w, r, &settlement) {
			return
		}
		outcome := settlement.Outcome
		if parts[1] == "seal-absence" {
			outcome = string(admissionapi.AdmissionOutcomeAbsent)
		}
		c.mu.Lock()
		guard := c.guards[id]
		if guard == nil {
			// Sealing a refused admission records its absence for good.
			c.epoch++
			guard = &queueCoreGuard{request: settlement.DeploymentAdmissionRequest, epoch: c.epoch}
			c.guards[id] = guard
		}
		guard.settled = true
		receipt := admissionapi.DeploymentAdmissionReceipt{AdmissionId: id, ProjectUuid: guard.request.ProjectUuid, EnvironmentUuid: guard.request.EnvironmentUuid,
			RequestDigest: guard.request.RequestDigest, Mode: string(guard.request.Mode), Epoch: guard.epoch, State: admissionapi.AdmissionStateSettled, Outcome: &outcome}
		c.mu.Unlock()
		assert.NoError(c.t, json.NewEncoder(w).Encode(receipt))
		return
	}
	var request admissionapi.DeploymentAdmissionRequest
	if !c.decode(w, r, &request) {
		return
	}
	c.mu.Lock()
	if code, denied := c.deny[request.ActorId]; denied {
		c.mu.Unlock()
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"` + code + `","message":"admission denied"}`))
		return
	}
	for other, guard := range c.guards {
		if other != id && !guard.settled && guard.request.EnvironmentUuid == request.EnvironmentUuid {
			c.mu.Unlock()
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"environment_busy","message":"Environment has an unresolved ordinary Deployment admission"}`))
			return
		}
	}
	c.epoch++
	c.guards[id] = &queueCoreGuard{request: request, epoch: c.epoch}
	receipt := admissionapi.DeploymentAdmissionReceipt{AdmissionId: id, ProjectUuid: request.ProjectUuid, EnvironmentUuid: request.EnvironmentUuid,
		RequestDigest: request.RequestDigest, Mode: string(request.Mode), Epoch: c.epoch, State: admissionapi.AdmissionStateActive}
	hook := c.onAuthorize
	c.mu.Unlock()
	if hook != nil {
		hook(id)
	}
	assert.NoError(c.t, json.NewEncoder(w).Encode(receipt))
}

func (c *queueCore) decode(w http.ResponseWriter, r *http.Request, value any) bool {
	if err := json.NewDecoder(r.Body).Decode(value); err != nil {
		c.t.Errorf("undecodable Core request: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return false
	}
	return true
}

func (c *queueCore) unsettledGuards() []uuid.UUID {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]uuid.UUID, 0)
	for id, guard := range c.guards {
		if !guard.settled {
			out = append(out, id)
		}
	}
	return out
}

func (c *queueCore) set(update func(*queueCore)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	update(c)
}

// hookAuthorize runs hook on the next admission only, while Core holds its new guard.
func (c *queueCore) hookAuthorize(t *testing.T, hook func(admissionID uuid.UUID)) {
	var once sync.Once
	c.set(func(c *queueCore) {
		c.onAuthorize = func(admissionID uuid.UUID) { once.Do(func() { hook(admissionID) }) }
	})
	t.Cleanup(func() { c.set(func(c *queueCore) { c.onAuthorize = nil }) })
}

// queueIAM grants every check except to denied users.
type queueIAM struct {
	t      *testing.T
	mu     sync.Mutex
	denied map[uuid.UUID]bool
}

func (i *queueIAM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body iamapi.InternalAuthorizeBody
	assert.NoError(i.t, json.NewDecoder(r.Body).Decode(&body))
	i.mu.Lock()
	denied := i.denied[body.UserId]
	i.mu.Unlock()
	if denied {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"HTTP-403","message":"not permitted","details":{}}`))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (i *queueIAM) deny(user uuid.UUID, denied bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.denied[user] = denied
}

type queueFixture struct {
	t          *testing.T
	ctx        context.Context
	connection string
	schema     string
	store      model.Databaser
	server     *Server
	echo       *echo.Echo
	core       *queueCore
	iam        *queueIAM
	coreURL    string
	coreClient *coreapi.ClientWithResponses
	iamClient  *iamapi.ClientWithResponses
	tokenFile  string
	config     DeploymentQueueConfig
	actor      uuid.UUID
}

func newQueueFixture(t *testing.T, config DeploymentQueueConfig) *queueFixture {
	t.Helper()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "MODEL_TEST_DB_CONNECTION_STRING must name an isolated local PostgreSQL test database")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	f := &queueFixture{t: t, ctx: ctx, connection: connection, config: config, actor: uuid.New(),
		schema: "queue_api_" + strings.ReplaceAll(uuid.NewString(), "-", "")}
	f.store = createAdmissionSchemaDatabase(t, ctx, connection, f.schema)
	f.core = &queueCore{t: t, projectUUID: uuid.New(), envUUID: uuid.New(), status: coreapi.EnvironmentStatusActive,
		guards: map[uuid.UUID]*queueCoreGuard{}, deny: map[uuid.UUID]string{}}
	core := httptest.NewServer(f.core)
	t.Cleanup(core.Close)
	f.iam = &queueIAM{t: t, denied: map[uuid.UUID]bool{}}
	iam := httptest.NewServer(f.iam)
	t.Cleanup(iam.Close)
	var err error
	f.coreURL = core.URL
	f.coreClient, err = coreapi.NewClientWithResponses(core.URL)
	require.NoError(t, err)
	f.iamClient, err = iamapi.NewClientWithResponses(iam.URL)
	require.NoError(t, err)
	f.tokenFile = t.TempDir() + "/token"
	require.NoError(t, os.WriteFile(f.tokenFile, []byte("queue-dp-token"), 0o600))
	f.server = f.newServer(f.store)
	f.echo, err = hecho.DefaultEchoServerWithValidation(&hecho.ValidatedServerConfig{AppName: "deployment-queue-test", Logger: zap.NewNop(), OpenAPIRawSchema: MustDecodeOpenApiSpec(), OpenAPISkipperFn: SkipGeneratedOpenAPIValidation})
	require.NoError(t, err)
	f.echo.Use(middleware.EchoCtxMiddleware)
	f.server.MapRoutes(f.echo)
	return f
}

// newServer is another Data Plane process on the same database.
func (f *queueFixture) newServer(store model.Databaser) *Server {
	admissions, err := NewOrdinaryDeploymentAdmissionService(store, f.coreURL, f.tokenFile, http.DefaultClient)
	require.NoError(f.t, err)
	server := &Server{Database: store, Logger: zap.NewNop(), EventPublisher: &hmessaging.RecordingPublisher{}, ControlPlaneClient: f.coreClient,
		IamClient: f.iamClient, DeploymentAdmissions: admissions, DeploymentQueue: f.config}
	// Background promotions must end before the schema is dropped.
	f.t.Cleanup(server.WaitDeploymentQueueKicks)
	return server
}

// openStore opens a second connection pool, as another process would.
func (f *queueFixture) openStore() model.Databaser {
	store, err := model.NewDatabaser(f.ctx, zap.NewNop(), f.connection+" search_path="+f.schema)
	require.NoError(f.t, err)
	f.t.Cleanup(func() { require.NoError(f.t, store.Close()) })
	return store
}

func queueBody(fields map[string]any) map[string]any {
	body := map[string]any{"project_id": queueProject, "env_id": queueEnv, "mode": "deploy", "is_dry_run": false, "manifest": map[string]any{"workloads": map[string]any{}}}
	for key, value := range fields {
		if value == nil {
			delete(body, key)
		} else {
			body[key] = value
		}
	}
	return body
}

func (f *queueFixture) request(method, path string, body any, actor uuid.UUID, idempotencyKey string) *http.Request {
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			// Errorf, not FailNow: concurrent callers run outside the test goroutine.
			f.t.Errorf("unencodable request body: %v", err)
		}
	}
	request := httptest.NewRequest(method, path, bytes.NewReader(encoded)).WithContext(f.ctx)
	request.Header.Set("From", actor.String())
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return request
}

// serve answers one request and lets the promotions it kicked finish.
func (f *queueFixture) serve(request *http.Request) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	f.echo.ServeHTTP(response, request)
	f.server.WaitDeploymentQueueKicks()
	return response
}

func (f *queueFixture) call(method, path string, body any, actor uuid.UUID, idempotencyKey string) *httptest.ResponseRecorder {
	return f.serve(f.request(method, path, body, actor, idempotencyKey))
}

func (f *queueFixture) deploy(body map[string]any) *httptest.ResponseRecorder {
	return f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", body, f.actor, "")
}

func (f *queueFixture) mustDeploy(body map[string]any) Deployment {
	f.t.Helper()
	response := f.deploy(body)
	require.Equal(f.t, http.StatusCreated, response.Code, response.Body.String())
	var deployment Deployment
	require.NoError(f.t, json.Unmarshal(response.Body.Bytes(), &deployment))
	return deployment
}

// dryRunPendingChanges answers a dry run, checks the answer against the
// published schema and returns its pending_changes, nil when absent.
func (f *queueFixture) dryRunPendingChanges(fields map[string]any) any {
	f.t.Helper()
	body := map[string]any{"is_dry_run": true}
	for key, value := range fields {
		body[key] = value
	}
	response := f.deploy(queueBody(body))
	require.Equal(f.t, http.StatusOK, response.Code, response.Body.String())
	var decoded map[string]any
	require.NoError(f.t, json.Unmarshal(response.Body.Bytes(), &decoded))
	spec, err := GetSwagger()
	require.NoError(f.t, err)
	require.NoError(f.t, spec.Components.Schemas["DeploymentDryRun"].Value.VisitJSON(decoded), "the dry run conforms to the published schema")
	require.Contains(f.t, decoded, "diff")
	return decoded["pending_changes"]
}

func (f *queueFixture) mustQueue(response *httptest.ResponseRecorder) DeploymentQueueItem {
	f.t.Helper()
	require.Equal(f.t, http.StatusAccepted, response.Code, response.Body.String())
	var item DeploymentQueueItem
	require.NoError(f.t, json.Unmarshal(response.Body.Bytes(), &item))
	return item
}

func (f *queueFixture) requireError(response *httptest.ResponseRecorder, status int, code string) {
	f.t.Helper()
	require.Equal(f.t, status, response.Code, response.Body.String())
	var body Error
	require.NoError(f.t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(f.t, code, body.Error, response.Body.String())
}

func (f *queueFixture) item(id uuid.UUID) *model.DeploymentQueueItem {
	f.t.Helper()
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(f.t, err)
	defer func() { _ = tx.Rollback() }()
	item, err := model.GetDeploymentQueueItem(f.ctx, tx, queueOrg, id)
	require.NoError(f.t, err)
	return item
}

func (f *queueFixture) queued() []model.DeploymentQueueItem {
	f.t.Helper()
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(f.t, err)
	defer func() { _ = tx.Rollback() }()
	items, _, err := model.ListDeploymentQueueItems(f.ctx, tx, queueOrg, "", 100, model.ListDeploymentQueueParams{State: opt.Of(model.DeploymentQueueQueued)})
	require.NoError(f.t, err)
	return items
}

// complete ends a Deployment as the Runner result would; Core still holds its admission.
func (f *queueFixture) complete(deploymentID uuid.UUID, status model.DeploymentStatus) {
	f.t.Helper()
	_, err := f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, deploymentID, model.UpdateDeploymentStatusAndOutputsParams{Status: status})
	require.NoError(f.t, err)
}

// settle is what the deployment.updated handler does: settle Core, then promote.
func (f *queueFixture) settle(server *Server, deploymentID uuid.UUID) {
	f.t.Helper()
	require.NoError(f.t, server.DeploymentAdmissions.SettleDeployment(f.ctx, deploymentID))
	f.promote(server)
}

func (f *queueFixture) finish(deploymentID uuid.UUID, status model.DeploymentStatus) {
	f.t.Helper()
	f.complete(deploymentID, status)
	f.settle(f.server, deploymentID)
}

func (f *queueFixture) promote(server *Server) {
	f.t.Helper()
	server.WaitDeploymentQueueKicks()
	require.NoError(f.t, server.PromoteDeploymentQueue(f.ctx, f.core.envUUID))
	server.WaitDeploymentQueueKicks()
}

// deployments lists the Environment's Deployments, oldest first.
func (f *queueFixture) deployments() []model.DeploymentSummary {
	f.t.Helper()
	items, _, err := f.store.ListDeployments(f.ctx, nil, queueOrg, "", 1000, model.ListDeploymentsParams{ProjectId: opt.Of(queueProject), EnvId: opt.Of(queueEnv)})
	require.NoError(f.t, err)
	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
	}
	return items
}

func (f *queueFixture) count(query string, args ...any) int {
	f.t.Helper()
	var count int
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(f.t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(f.t, tx.QueryRowContext(f.ctx, query, args...).Scan(&count))
	return count
}

func (f *queueFixture) exec(query string, args ...any) {
	f.t.Helper()
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(f.t, err)
	_, err = tx.ExecContext(f.ctx, query, args...)
	require.NoError(f.t, err)
	require.NoError(f.t, tx.Commit())
}

// terminateQueueHolder kills the database session that holds a claimed queue head, as a process crash would.
func (f *queueFixture) terminateQueueHolder() error {
	admin, err := sql.Open("postgres", f.connection)
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	var terminated bool
	if err := admin.QueryRowContext(f.ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks
		WHERE locktype = 'relation' AND mode = 'RowShareLock' AND granted AND pid <> pg_backend_pid()
		  AND relation = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		                  WHERE c.relname = 'environment_deployment_queue' AND n.nspname = $1)`, f.schema).Scan(&terminated); err != nil {
		return err
	}
	if !terminated {
		return errors.New("no session held the queue head")
	}
	return nil
}
