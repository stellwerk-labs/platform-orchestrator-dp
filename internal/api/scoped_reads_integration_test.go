//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"filippo.io/age"
	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	iam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scopedReadFixture struct {
	*queueFixture
	deployment Deployment
	queuedItem DeploymentQueueItem
	identity   *age.X25519Identity
}

func newScopedReadFixture(t *testing.T) *scopedReadFixture {
	t.Helper()
	f := newQueueFixture(t, DeploymentQueueConfig{})
	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	deployment := f.mustDeploy(queueBody(map[string]any{"encrypted_outputs_recipient": identity.Recipient().String()}))
	queued := f.mustQueue(f.deploy(queueBody(queueEverything)))
	for range 2 {
		f.mustQueue(f.deploy(queueBody(queueEverything)))
	}
	var encrypted bytes.Buffer
	writer, err := age.Encrypt(&encrypted, identity.Recipient())
	require.NoError(t, err)
	_, err = io.WriteString(writer, "private-output-and-log-canary")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	f.complete(deployment.Id, model.DeploymentStatusSucceeded)
	f.exec(`UPDATE deployments SET outputs=$2 WHERE id=$1`, deployment.Id, encrypted.Bytes())
	encoded := base64.StdEncoding.EncodeToString(encrypted.Bytes())
	f.server.RunnerLogsReader = func(_ context.Context, key string) (io.ReadCloser, error) {
		assert.Equal(t, f.core.envUUID.String()+"/"+deployment.Id.String(), key)
		return io.NopCloser(strings.NewReader(encoded)), nil
	}
	return &scopedReadFixture{f, deployment, queued, identity}
}

func scopedReadIAM(t *testing.T, actor uuid.UUID, resources map[string]int, logs bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/internal/authorize", r.URL.Path)
		var body iam.InternalAuthorizeBody
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&body)) || !assert.Len(t, body.Checks, 1) {
			w.WriteHeader(400)
			return
		}
		assert.Equal(t, actor, body.UserId)
		permission := body.Checks[0].Permission
		allowed := permission == PermissionDeploymentRead || permission == PermissionActiveResourceRead || permission == PermissionResourceGraphRead || (logs && permission == PermissionDeploymentLogsRead)
		status := resources[body.Checks[0].Resource]
		if !allowed || status == 0 {
			status = 403
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status == 403 {
			_, err := w.Write([]byte(`{"error":"HTTP-403","message":"denied","details":{}}`))
			assert.NoError(t, err)
		}
	}))
}

func (f *scopedReadFixture) authorize(actor uuid.UUID, resources map[string]int, logs bool) {
	f.t.Helper()
	peer := scopedReadIAM(f.t, actor, resources, logs)
	f.t.Cleanup(peer.Close)
	client, err := iam.NewClientWithResponses(peer.URL)
	require.NoError(f.t, err)
	f.server.IamClient = client
}

func TestScopedDeploymentReadsHumanAndServiceActors(t *testing.T) {
	f := newScopedReadFixture(t)
	base := "/orgs/" + queueOrg
	deployment := base + "/deployments/" + f.deployment.Id.String()
	filtered := "?project_id=" + queueProject + "&env_id=" + queueEnv
	for _, actor := range []uuid.UUID{uuid.New(), userid.NewServiceUserTokenId()} {
		for _, resource := range []string{"env:" + f.core.envUUID.String(), "organization:" + queueOrg} {
			f.authorize(actor, map[string]int{resource: 204}, false)
			for _, route := range []struct{ method, path string }{
				{http.MethodGet, deployment + "/encrypted-outputs"},
				{http.MethodGet, deployment + "/actions/get-logs"},
				{http.MethodGet, deployment + "/resources"},
				{http.MethodPost, deployment + "/actions/diff?fromDeploymentId=" + f.deployment.Id.String()},
				{http.MethodGet, base + "/active-resources" + filtered},
				{http.MethodGet, base + "/last-deployments" + filtered},
				{http.MethodGet, base + "/deployment-queue" + filtered + "&state=queued"},
				{http.MethodGet, base + "/deployment-queue/" + f.queuedItem.Id.String()},
			} {
				response := f.call(route.method, route.path, nil, actor, "")
				require.Equal(t, 200, response.Code, response.Body.String())
				require.NotContains(t, response.Body.String(), "private-output-and-log-canary")
			}
		}
	}
}

func TestScopedDeploymentListsDoNotPromoteEnvironmentAuthority(t *testing.T) {
	f := newScopedReadFixture(t)
	actor := uuid.New()
	base := "/orgs/" + queueOrg
	for _, path := range []string{"/deployments", "/last-deployments", "/deployment-queue"} {
		f.authorize(actor, map[string]int{"env:" + f.core.envUUID.String(): 204}, false)
		for _, query := range []string{"", "?project_id=" + queueProject} {
			response := f.call(http.MethodGet, base+path+query, nil, actor, "")
			require.Equal(t, 403, response.Code, response.Body.String())
			require.NotContains(t, response.Body.String(), f.deployment.Id.String())
			require.NotContains(t, response.Body.String(), f.queuedItem.Id.String())
		}
		response := f.call(http.MethodGet, base+path+"?env_id="+queueEnv, nil, actor, "")
		require.Equal(t, 400, response.Code, response.Body.String())
		f.authorize(actor, map[string]int{"project:" + f.core.projectUUID.String(): 204}, false)
		response = f.call(http.MethodGet, base+path+"?project_id="+queueProject, nil, actor, "")
		require.Equal(t, 200, response.Code, response.Body.String())
	}
}

func TestScopedDeploymentLogsRetainSeparatePlaintextPermission(t *testing.T) {
	f := newScopedReadFixture(t)
	actor := uuid.New()
	resource := "env:" + f.core.envUUID.String()
	path := "/orgs/" + queueOrg + "/deployments/" + f.deployment.Id.String() + "/actions/get-logs"
	f.authorize(actor, map[string]int{resource: 204}, false)
	request := f.request(http.MethodGet, path, nil, actor, "")
	request.Header.Set("DecryptKey", f.identity.String())
	response := f.serve(request)
	require.Equal(t, 403, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), "private-output-and-log-canary")
	f.authorize(actor, map[string]int{resource: 204}, true)
	request = f.request(http.MethodGet, path, nil, actor, "")
	request.Header.Set("DecryptKey", f.identity.String())
	response = f.serve(request)
	require.Equal(t, 200, response.Code, response.Body.String())
	require.Equal(t, "private-output-and-log-canary", response.Body.String())
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	debug := f.call(http.MethodGet, "/orgs/"+queueOrg+"/deployments/"+f.deployment.Id.String()+"/tf", nil, actor, "")
	require.Equal(t, 403, debug.Code, debug.Body.String())
}

func TestScopedDeploymentReadFailuresAndAbsence(t *testing.T) {
	f := newScopedReadFixture(t)
	actor := uuid.New()
	base := "/orgs/" + queueOrg
	for _, resource := range []map[string]int{{}, {"env:" + f.core.envUUID.String(): 503, "organization:" + queueOrg: 503}} {
		f.authorize(actor, resource, false)
		for _, path := range []string{"/deployments/" + f.deployment.Id.String() + "/encrypted-outputs", "/deployments/" + f.deployment.Id.String() + "/resources", "/deployment-queue/" + f.queuedItem.Id.String()} {
			response := f.call(http.MethodGet, base+path, nil, actor, "")
			want := 403
			if len(resource) > 0 {
				want = 500
			}
			require.Equal(t, want, response.Code, response.Body.String())
			require.NotContains(t, response.Body.String(), "private-output-and-log-canary")
		}
	}
	f.authorize(actor, map[string]int{"env:" + f.core.envUUID.String(): 204}, false)
	for _, prefix := range []string{base, "/orgs/foreign"} {
		id := uuid.NewString()
		if prefix != base {
			id = f.deployment.Id.String()
		}
		for _, suffix := range []string{"/encrypted-outputs", "/resources", "/actions/get-logs"} {
			response := f.call(http.MethodGet, prefix+"/deployments/"+id+suffix, nil, actor, "")
			require.Equal(t, 404, response.Code, response.Body.String())
		}
		response := f.call(http.MethodGet, prefix+"/deployment-queue/"+f.queuedItem.Id.String(), nil, actor, "")
		want := 200
		if prefix != base {
			want = 404
		}
		require.Equal(t, want, response.Code, response.Body.String())
	}
	require.NoError(t, f.store.Close())
	response := f.call(http.MethodGet, base+"/deployments/"+f.deployment.Id.String()+"/encrypted-outputs", nil, actor, "")
	require.Equal(t, 500, response.Code, response.Body.String())
}

func TestScopedDeploymentDiffRejectsForeignGraphBeforeParsing(t *testing.T) {
	f := newScopedReadFixture(t)
	actor := uuid.New()
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	foreign, err := f.store.CreateDeployment(f.ctx, tx, queueOrg, queueProject, "sibling", model.CreateDeploymentParams{
		DeploymentEnvUuid: uuid.New(), CreatedBy: f.actor, Mode: model.DeploymentModeDeploy, RunnerId: queueRunner, RunnerLogLevel: "info",
		Manifest: model.EncodedDeploymentManifest(`{}`), Graph: model.EncodedDeploymentGraph(`foreign-private-invalid-graph`), Tofu: model.RawTofu("terraform {}"),
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	f.authorize(actor, map[string]int{"env:" + f.core.envUUID.String(): 204}, false)
	base := "/orgs/" + queueOrg + "/deployments/" + f.deployment.Id.String()
	response := f.call(http.MethodPost, base+"/actions/diff?fromDeploymentId="+foreign.Id.String(), nil, actor, "")
	require.Equal(t, 400, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), "foreign-private-invalid-graph")
	require.NotContains(t, response.Body.String(), "unmarshal")
	for _, suffix := range []string{"/resources", "/encrypted-outputs", "/actions/get-logs"} {
		denied := f.call(http.MethodGet, "/orgs/"+queueOrg+"/deployments/"+foreign.Id.String()+suffix, nil, actor, "")
		require.Equal(t, 403, denied.Code, denied.Body.String())
		require.NotContains(t, denied.Body.String(), "foreign-private-invalid-graph")
	}
	f.exec(`UPDATE deployments SET graph=$2 WHERE id=$1`, f.deployment.Id, []byte(`private-invalid-target-graph`))
	f.authorize(actor, nil, false)
	denied := f.call(http.MethodPost, base+"/actions/diff", nil, actor, "")
	require.Equal(t, 403, denied.Code, denied.Body.String())
	require.NotContains(t, denied.Body.String(), "unmarshal")
}

func TestScopedDeploymentQueuePaginationKeepsFiltersAndAuthority(t *testing.T) {
	f := newScopedReadFixture(t)
	actor := uuid.New()
	base := "/orgs/" + queueOrg
	f.authorize(actor, map[string]int{"env:" + f.core.envUUID.String(): 204}, false)
	query := "?project_id=" + queueProject + "&env_id=" + queueEnv + "&state=queued&per_page=1"
	response := f.call(http.MethodGet, base+"/deployment-queue"+query, nil, actor, "")
	require.Equal(t, 200, response.Code, response.Body.String())
	var page DeploymentQueueItemPage
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
	require.Len(t, page.Items, 1)
	require.NotNil(t, page.NextPageToken)
	token := url.QueryEscape(*page.NextPageToken)
	next := f.call(http.MethodGet, base+"/deployment-queue"+query+"&page="+token, nil, actor, "")
	require.Equal(t, 200, next.Code, next.Body.String())
	var second DeploymentQueueItemPage
	require.NoError(t, json.Unmarshal(next.Body.Bytes(), &second))
	require.Len(t, second.Items, 1)
	require.NotEqual(t, page.Items[0].Id, second.Items[0].Id)
	require.Equal(t, queueEnv, second.Items[0].EnvId)
	denied := f.call(http.MethodGet, base+"/deployment-queue?page="+token, nil, actor, "")
	require.Equal(t, 403, denied.Code, denied.Body.String())
	for _, stateChange := range []string{"true", "false"} {
		last := f.call(http.MethodGet, base+"/last-deployments?project_id="+queueProject+"&env_id="+queueEnv+"&state_change_only="+stateChange, nil, actor, "")
		require.Equal(t, 200, last.Code, last.Body.String())
		require.Contains(t, last.Body.String(), f.deployment.Id.String())
	}
}

func TestScopedDeploymentFilteredListsRefuseUnavailableCore(t *testing.T) {
	f := newScopedReadFixture(t)
	actor := uuid.New()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(503)
		_, err := w.Write([]byte(`{}`))
		assert.NoError(t, err)
	}))
	defer peer.Close()
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	f.server.ControlPlaneClient = client
	f.authorize(actor, map[string]int{"env:" + f.core.envUUID.String(): 204}, false)
	for _, path := range []string{"/deployments", "/last-deployments", "/deployment-queue"} {
		response := f.call(http.MethodGet, "/orgs/"+queueOrg+path+"?project_id="+queueProject+"&env_id="+queueEnv, nil, actor, "")
		require.Equal(t, 502, response.Code, response.Body.String())
		require.NotContains(t, response.Body.String(), f.deployment.Id.String())
	}
	active := f.call(http.MethodGet, "/orgs/"+queueOrg+"/active-resources?project_id="+queueProject+"&env_id="+queueEnv, nil, actor, "")
	require.Equal(t, 200, active.Code, active.Body.String(), "retained exact Environment identity does not need live Core")
	missing := f.call(http.MethodGet, "/orgs/"+queueOrg+"/active-resources?project_id="+queueProject+"&env_id=never-deployed", nil, actor, "")
	require.Equal(t, 404, missing.Code, missing.Body.String())
}

func TestScopedDeploymentReadsMalformedIAMForbiddens(t *testing.T) {
	f := newScopedReadFixture(t)
	actor := uuid.New()
	for _, trial := range []struct {
		name, media, body string
		want              int
	}{
		{"empty", "text/plain", "", 403},
		{"proxy HTML", "text/html", "<html>private-IAM-canary</html>", 403},
		{"missing details", "application/json", `{"error":"private-IAM-canary","message":"private-IAM-canary"}`, 403},
		{"null", "application/json", `null`, 403},
		{"valid failed checks", "application/json", `{"error":"ignored","details":{"failed_checks":[{"permission":"deployment_read","resource":"env:denied"}]}}`, 403},
		{"invalid JSON", "application/json", `<private-IAM-canary>`, 500},
	} {
		t.Run(trial.name, func(t *testing.T) {
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body iam.InternalAuthorizeBody
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				assert.Equal(t, actor, body.UserId)
				w.Header().Set("Content-Type", trial.media)
				w.WriteHeader(403)
				_, err := w.Write([]byte(trial.body))
				assert.NoError(t, err)
			}))
			defer peer.Close()
			client, err := iam.NewClientWithResponses(peer.URL)
			require.NoError(t, err)
			f.server.IamClient = client
			response := f.call(http.MethodGet, "/orgs/"+queueOrg+"/deployments/"+f.deployment.Id.String()+"/encrypted-outputs", nil, actor, "")
			require.Equal(t, trial.want, response.Code, response.Body.String())
			require.NotContains(t, response.Body.String(), "private-IAM-canary")
			require.NotContains(t, response.Body.String(), "private-output-and-log-canary")
			if trial.name == "valid failed checks" {
				require.Contains(t, response.Body.String(), "failed_checks")
				require.Contains(t, response.Body.String(), "deployment_read")
			}
		})
	}
}
