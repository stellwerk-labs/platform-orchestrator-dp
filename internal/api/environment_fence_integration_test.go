//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stellwerk-labs/golib/hecho"
	coreapi "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	iamapi "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func TestLastDeploymentsShowTheEnvironmentFenceUntilTheOutcomeIsAcknowledged(t *testing.T) {
	f := newLateResultFixture(t)
	// Core knows the organization and IAM grants everything: this test is about
	// what the read model reports, not about who may read it.
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/internal/orgs/late-org" {
			t.Errorf("unexpected Core request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"late-org"}`))
	}))
	t.Cleanup(core.Close)
	iam := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	t.Cleanup(iam.Close)
	var err error
	f.server.ControlPlaneClient, err = coreapi.NewClientWithResponses(core.URL)
	require.NoError(t, err)
	f.server.IamClient, err = iamapi.NewClientWithResponses(iam.URL)
	require.NoError(t, err)
	e, err := hecho.DefaultEchoServerWithValidation(&hecho.ValidatedServerConfig{AppName: "environment-fence-test", Logger: zap.NewNop(), OpenAPIRawSchema: MustDecodeOpenApiSpec(), OpenAPISkipperFn: SkipGeneratedOpenAPIValidation})
	require.NoError(t, err)
	e.Use(middleware.EchoCtxMiddleware)
	f.server.MapRoutes(e)
	actor := userid.NewHumanUserId()

	current := func(stateChangeOnly bool) DeploymentSummary {
		t.Helper()
		path := "/orgs/late-org/last-deployments?project_id=p&env_id=e"
		if stateChangeOnly {
			path += "&state_change_only=true"
		}
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("From", actor.String())
		response := httptest.NewRecorder()
		e.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var page DeploymentPage
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
		require.Len(t, page.Items, 1)
		require.Equal(t, f.forward.Id, page.Items[0].Id)
		return page.Items[0]
	}
	acknowledge := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/internal/orgs/late-org/deployments/"+f.forward.Id.String()+"/actions/acknowledge-outcome",
			strings.NewReader(`{"reason":"runner job confirmed deleted"}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("From", actor.String())
		response := httptest.NewRecorder()
		e.ServeHTTP(response, request)
		return response
	}
	replacementError := func() error {
		tx, err := f.store.BeginTx(f.ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		_, err = f.store.CreateDeployment(f.ctx, tx, "late-org", "p", "e", f.params)
		return err
	}
	dispatchDeadline := func() time.Time {
		t.Helper()
		tx, err := f.store.BeginTx(f.ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		budget, err := model.ReadDeploymentDispatchBudget(f.ctx, tx, f.forward.Id)
		require.NoError(t, err)
		require.True(t, budget.Deadline.Valid)
		return budget.Deadline.Time
	}

	for _, stateChangeOnly := range []bool{false, true} {
		require.Nil(t, current(stateChangeOnly).EnvironmentFence, "an executing Deployment makes the Environment busy, not fenced")
	}

	failed := f.fail(t)
	window := dispatchDeadline()
	for _, stateChangeOnly := range []bool{false, true} {
		fence := current(stateChangeOnly).EnvironmentFence
		require.NotNil(t, fence, "state_change_only=%t", stateChangeOnly)
		require.Equal(t, OutcomeUnreconciled, fence.Code)
		require.Equal(t, f.forward.Id, fence.DeploymentId)
		require.True(t, failed.CompletedAt.Must().Equal(fence.Since), "since is when the Deployment failed: %s vs %s", failed.CompletedAt.Must(), fence.Since)
		require.Equal(t, AcknowledgeOutcome, fence.Remedy)
		require.NotNil(t, fence.RemedyAvailableAt)
		require.True(t, window.Equal(*fence.RemedyAvailableAt), "the remedy waits for the Runner dispatch window: %s vs %s", window, *fence.RemedyAvailableAt)
	}
	require.ErrorIs(t, replacementError(), model.ErrOutcomeUnreconciled, "the signal and the refusal agree")
	early := acknowledge()
	require.Equal(t, http.StatusConflict, early.Code, "the remedy is refused before remedy_available_at")
	require.Contains(t, early.Body.String(), "dispatch window is open")

	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now()-interval '2 hours', deadline=now()-interval '1 hour' WHERE deployment_id=$1`, f.forward.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	closed := current(false).EnvironmentFence
	require.NotNil(t, closed, "a closed dispatch window alone lifts nothing")
	require.True(t, dispatchDeadline().Equal(*closed.RemedyAvailableAt))

	acknowledged := acknowledge()
	require.Equal(t, http.StatusCreated, acknowledged.Code, acknowledged.Body.String())
	for _, stateChangeOnly := range []bool{false, true} {
		item := current(stateChangeOnly)
		require.Nil(t, item.EnvironmentFence, "the acknowledgement lifts the fence, state_change_only=%t", stateChangeOnly)
		require.Equal(t, string(model.DeploymentStatusFailed), item.Status, "and never rewrites the Deployment")
	}
	require.NoError(t, replacementError(), "the Environment accepts Deployments again exactly when the signal is gone")
}
