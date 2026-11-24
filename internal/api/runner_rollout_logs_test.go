package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const reviewedLogSubject = "reviewed+plan&subject"

func rolloutLogHTTPTest(t *testing.T, permission string, logs http.HandlerFunc) (*echo.Echo, uuid.UUID, string) {
	t.Helper()
	id := uuid.NewString()
	var environment uuid.UUID
	e, actor, env := rolloutHTTPTest(t, true, permission, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/runner-rollouts/"+id {
			w.Header().Set("Content-Type", observationJSONContentType)
			assert.NoError(t, json.NewEncoder(w).Encode(runnerRolloutView{ID: id, EnvironmentID: environment.String(), Subject: reviewedLogSubject}))
			return
		}
		logs(w, r)
	})
	environment = env
	return e, actor, "/" + id + "/plan-logs?subject=" + url.QueryEscape(reviewedLogSubject)
}

func TestRunnerRolloutPlanLogsUseBothPermissionsAndTrustedActor(t *testing.T) {
	content := "{\"@level\":\"info\",\"@message\":\"Planning\"}\n  ~ value = \"before\" -> \"after\"\n<script>text</script>\n"
	var actor uuid.UUID
	e, caller, path := rolloutLogHTTPTest(t, "", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer adapter-secret", r.Header.Get("Authorization"))
		assert.Equal(t, actor.String(), r.Header.Get("X-Rollout-Actor"))
		assert.Equal(t, reviewedLogSubject, r.URL.Query().Get("subject"))
		w.Header().Set(runnerPlanSubjectHeader, reviewedLogSubject)
		_, _ = w.Write([]byte(content))
	})
	actor = caller
	response := callRolloutHTTP(e, actor, http.MethodGet, path, "")
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, content, response.Body.String())
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "text/plain; charset=utf-8", response.Header().Get("Content-Type"))
	require.Equal(t, reviewedLogSubject, response.Header().Get(runnerPlanSubjectHeader))
}

func TestRunnerRolloutPlanLogsRefusePartialAuthorityAndStaleSubject(t *testing.T) {
	for _, permission := range []string{PermissionDeploymentRead, PermissionDeploymentLogsRead} {
		t.Run(permission, func(t *testing.T) {
			var calls atomic.Int32
			e, actor, path := rolloutLogHTTPTest(t, permission, func(w http.ResponseWriter, _ *http.Request) { calls.Add(1) })
			require.Equal(t, http.StatusForbidden, callRolloutHTTP(e, actor, http.MethodGet, path, "").Code)
			require.Zero(t, calls.Load())
		})
	}
	var calls atomic.Int32
	e, actor, path := rolloutLogHTTPTest(t, "", func(w http.ResponseWriter, _ *http.Request) { calls.Add(1) })
	path = strings.ReplaceAll(path, url.QueryEscape(reviewedLogSubject), "stale")
	require.Equal(t, http.StatusConflict, callRolloutHTTP(e, actor, http.MethodGet, path, "").Code)
	require.Zero(t, calls.Load())
}

func TestRunnerRolloutPlanLogsRefuseAnotherEnvironmentBeforeReadingContent(t *testing.T) {
	id := uuid.NewString()
	var calls atomic.Int32
	e, actor, _ := rolloutHTTPTest(t, true, "", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/plan-logs") {
			calls.Add(1)
		}
		w.Header().Set("Content-Type", observationJSONContentType)
		assert.NoError(t, json.NewEncoder(w).Encode(runnerRolloutView{ID: id, EnvironmentID: uuid.NewString(), Subject: reviewedLogSubject}))
	})
	response := callRolloutHTTP(e, actor, http.MethodGet, "/"+id+"/plan-logs?subject="+url.QueryEscape(reviewedLogSubject), "")
	require.Equal(t, http.StatusNotFound, response.Code)
	require.Zero(t, calls.Load())
}

func TestRunnerRolloutPlanLogFailuresNeverReturnPartialOrPrivateContent(t *testing.T) {
	for _, test := range []struct {
		name    string
		status  int
		subject string
		content string
		length  string
		want    int
	}{
		{name: "not captured", status: 404, want: 404},
		{name: "site limit", status: 413, want: 413},
		{name: "site conflict", status: 409, want: 409},
		{name: "site unavailable", status: 500, want: 503},
		{name: "different plan", status: 200, subject: "different", want: 409},
		{name: "oversize", status: 200, subject: reviewedLogSubject, content: strings.Repeat("private-canary", runnerPlanLogLimit/14+1), want: 413},
		{name: "interrupted", status: 200, subject: reviewedLogSubject, length: "100", want: 503},
	} {
		t.Run(test.name, func(t *testing.T) {
			e, actor, path := rolloutLogHTTPTest(t, "", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(runnerPlanSubjectHeader, test.subject)
				if test.length != "" {
					w.Header().Set("Content-Length", test.length)
				}
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte("private-canary" + test.content))
			})
			response := callRolloutHTTP(e, actor, http.MethodGet, path, "")
			require.Equal(t, test.want, response.Code, response.Body.String())
			require.NotContains(t, response.Body.String(), "private-canary")
			require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
		})
	}
}

func TestRunnerRolloutPlanLogsAllowFailedPlanDiagnosticsWithoutApprovalSubject(t *testing.T) {
	id := uuid.NewString()
	var environment uuid.UUID
	e, actor, env := rolloutHTTPTest(t, true, "", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/plan-logs") {
			assert.Empty(t, r.URL.Query().Get("subject"))
			_, _ = w.Write([]byte("Error: invalid configuration\n"))
			return
		}
		w.Header().Set("Content-Type", observationJSONContentType)
		assert.NoError(t, json.NewEncoder(w).Encode(runnerRolloutView{ID: id, EnvironmentID: environment.String(), Phase: "failed"}))
	})
	environment = env
	response := callRolloutHTTP(e, actor, http.MethodGet, "/"+id+"/plan-logs?subject=", "")
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "Error: invalid configuration\n", response.Body.String())
}

func TestRunnerRolloutPlanLogsUnavailableSiteReturnsSafeError(t *testing.T) {
	e, actor, path := rolloutLogHTTPTest(t, "", func(w http.ResponseWriter, _ *http.Request) {
		connection, _, err := w.(http.Hijacker).Hijack()
		assert.NoError(t, err)
		if err == nil {
			_ = connection.Close()
		}
	})
	response := callRolloutHTTP(e, actor, http.MethodGet, path, "")
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Contains(t, response.Body.String(), "site is unavailable")
}
