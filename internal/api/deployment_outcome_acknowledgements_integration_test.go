//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stellwerk-labs/golib/hecho"
	iamapi "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestOutcomeAcknowledgementRequiresPermissionAndReleasesCapturedBaseRecoveryOnce(t *testing.T) {
	f := newLateResultFixture(t)
	failed := f.fail(t)
	actor := userid.NewHumanUserId()
	var allowed atomic.Bool
	iam := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body iamapi.InternalAuthorizeBody
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, actor, body.UserId)
		if assert.Len(t, body.Checks, 1) {
			assert.Equal(t, PermissionDeploymentOutcomeAcknowledge, body.Checks[0].Permission)
			assert.Contains(t, []string{"env:" + f.params.DeploymentEnvUuid.String(), "organization:late-org"}, body.Checks[0].Resource)
		}
		if allowed.Load() {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"HTTP-403","message":"not permitted","details":{}}`))
	}))
	t.Cleanup(iam.Close)
	iamClient, err := iamapi.NewClientWithResponses(iam.URL)
	require.NoError(t, err)
	f.server.IamClient = iamClient
	e, err := hecho.DefaultEchoServerWithValidation(&hecho.ValidatedServerConfig{AppName: "outcome-acknowledgement-test", Logger: zap.NewNop(), OpenAPIRawSchema: MustDecodeOpenApiSpec(), OpenAPISkipperFn: SkipGeneratedOpenAPIValidation})
	require.NoError(t, err)
	e.Use(middleware.EchoCtxMiddleware)
	f.server.MapRoutes(e)
	acknowledge := func(from, reason string) *httptest.ResponseRecorder {
		body, err := json.Marshal(DeploymentOutcomeAcknowledgementBody{Reason: reason})
		require.NoError(t, err)
		request := httptest.NewRequest(http.MethodPost, "/internal/orgs/late-org/deployments/"+f.forward.Id.String()+"/actions/acknowledge-outcome", strings.NewReader(string(body)))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("From", from)
		response := httptest.NewRecorder()
		e.ServeHTTP(response, request)
		return response
	}
	const reason = "runner job confirmed deleted"

	require.Equal(t, http.StatusForbidden, acknowledge(actor.String(), reason).Code, "explicit permission is required")
	allowed.Store(true)
	open := acknowledge(actor.String(), reason)
	require.Equal(t, http.StatusConflict, open.Code)
	require.Contains(t, open.Body.String(), "dispatch window is open")
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now()-interval '2 hours', deadline=now()-interval '1 hour' WHERE deployment_id=$1`, f.forward.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	created := acknowledge(actor.String(), reason)
	require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
	var ack DeploymentOutcomeAcknowledgement
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &ack))
	require.False(t, ack.Replayed)
	require.Equal(t, actor, ack.ActorId)
	require.Equal(t, reason, ack.Reason)
	require.Equal(t, f.forward.Id, ack.DeploymentId)
	require.Equal(t, f.params.DeploymentEnvUuid, ack.EnvironmentId)
	require.Equal(t, "dev.example.rollouts", ack.PluginId)

	replayed := acknowledge(actor.String(), reason)
	require.Equal(t, http.StatusOK, replayed.Code)
	var replay DeploymentOutcomeAcknowledgement
	require.NoError(t, json.Unmarshal(replayed.Body.Bytes(), &replay))
	require.True(t, replay.Replayed)
	require.True(t, ack.AcknowledgedAt.Equal(replay.AcknowledgedAt))
	require.Equal(t, http.StatusConflict, acknowledge(actor.String(), "a different story").Code)
	require.Equal(t, http.StatusForbidden, acknowledge(userid.InternalSystemUuid.String(), reason).Code)

	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var audits int
	var auditedActor, auditedReason string
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*), min(actor_id::text), min(reason) FROM deployment_outcome_acknowledgements`).Scan(&audits, &auditedActor, &auditedReason))
	require.Equal(t, 1, audits)
	require.Equal(t, actor.String(), auditedActor)
	require.Equal(t, reason, auditedReason)
	got, _, _, _, err := f.store.GetDeployment(f.ctx, tx, "late-org", f.forward.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, failed.Status, got.Status)
	require.Equal(t, failed.Revision, got.Revision, "acknowledgement never rewrites the Deployment")

	adapter := ExternalDeploymentHTTPAdapter{Admission: ExternalDeploymentAdmissionService{Database: f.store}}
	receipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(f.ctx, tx, "late-org", "dev.example.rollouts", f.forward.Id)
	require.NoError(t, err)
	projected, err := adapter.projectExternalDeploymentStatus(f.ctx, tx, *got, *receipt, f.project, f.params.DeploymentEnvUuid)
	require.NoError(t, err, "the plugin can now observe the acknowledged failure")
	require.Equal(t, "failed", projected.Status)
	recovery := f.params
	recovery.Mode = model.DeploymentModeRollback
	recovery.RollbackToId = opt.Of(f.base.Id)
	recovery.ExpectedCurrentDeploymentID = opt.Of(f.forward.Id)
	rollback, err := f.store.CreateDeployment(f.ctx, tx, "late-org", "p", "e", recovery)
	require.NoError(t, err, "captured-base recovery accepts the audited acknowledgement as the stop attestation")
	require.Equal(t, f.base.Id, rollback.RollbackToId.Must())
}
