//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func retainCheckedNativeAdmission(t *testing.T, ctx context.Context, store model.Databaser, project, environment, actor uuid.UUID, epoch int64) *cp.NativeAdmissionWatermark {
	t.Helper()
	intent := model.DeploymentAdmissionReceipt{OrgID: "org", AdmissionID: uuid.New(), ProjectUUID: project, EnvironmentUUID: environment, ActorID: actor,
		RequestDigest: strings.Repeat("a", 64), Mode: model.DeploymentModeDeployPlan, Disposition: "pending", NativeTarget: nativeTargetFixture()}
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = model.PrepareDeploymentAdmissionReceipt(ctx, tx, intent)
	require.NoError(t, err)
	_, err = model.BindDeploymentAdmissionReceipt(ctx, tx, intent, uuid.New(), epoch)
	require.NoError(t, err)
	require.NoError(t, model.MarkDeploymentAdmissionCoreSettled(ctx, tx, intent.OrgID, intent.AdmissionID, "succeeded", epoch))
	require.NoError(t, tx.Commit())
	return &cp.NativeAdmissionWatermark{AdmissionId: intent.AdmissionID, Epoch: epoch}
}

// The typed HTTP peer proves DP wire/recovery behavior. It is not actual Core/IAM qualification.
func TestNativeAdmissionWatermarkWireKeepsPendingUnavailableAndReconstructsExactAbsence(t *testing.T) {
	for _, coreCommitted := range []bool{false, true} {
		t.Run(fmt.Sprintf("Core already committed=%t", coreCommitted), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			store := deploymentAdmissionTestDatabase(t, ctx, os.Getenv("MODEL_TEST_DB_CONNECTION_STRING"))
			project, environment, actor := uuid.New(), uuid.New(), uuid.New()
			predecessor := retainCheckedNativeAdmission(t, ctx, store, project, environment, actor, 7)
			requests := make(chan cp.DeploymentAdmissionRequest, 4)
			var authorizations atomic.Int32
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body cp.DeploymentAdmissionRequest
				if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&body)) {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				requests <- body
				if !strings.HasSuffix(r.URL.Path, "/seal-absence") {
					authorizations.Add(1)
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if coreCommitted {
					// Preserve the existing refusal when Core already committed the guard.
					w.WriteHeader(http.StatusConflict)
					return
				}
				parts := strings.Split(r.URL.Path, "/")
				id := uuid.MustParse(parts[len(parts)-2])
				w.Header().Set("Content-Type", "application/json")
				assert.NoError(t, json.NewEncoder(w).Encode(cp.DeploymentAdmissionReceipt{AdmissionId: id, ProjectUuid: body.ProjectUuid, EnvironmentUuid: body.EnvironmentUuid,
					RequestDigest: body.RequestDigest, Mode: string(body.Mode), Epoch: 9, State: cp.AdmissionStateSettled, Outcome: refString("absent")}))
			}))
			defer peer.Close()
			client, err := cp.NewClientWithResponses(peer.URL)
			require.NoError(t, err)
			service := &OrdinaryDeploymentAdmissionService{Database: store, Core: client, BearerToken: "owned-test-token"}
			compiled := ordinaryDeploymentCompiledIntent{NativeTarget: nativeTargetFixture(), ProjectID: "p", EnvironmentID: "e", RunnerID: "native_pool", Manifest: []byte(`{}`), Graph: []byte(`{}`), Tofu: []byte("terraform {}")}
			attempt := service.NewAttempt("org", project, environment, actor, model.DeploymentModeDeploy, "lost-native-response")
			tx, err := store.BeginTx(ctx, nil)
			require.NoError(t, err)
			require.ErrorIs(t, attempt.Authorize(ctx, tx, nil, compiled), ErrDeploymentAdmissionUnavailable)
			require.NoError(t, tx.Rollback())
			original := <-requests
			require.Equal(t, predecessor, original.NativeAdmissionWatermark)
			backdateAdmissionReceipt(t, ctx, store, "org", attempt.AdmissionID)
			retry := service.NewAttempt("org", project, environment, actor, model.DeploymentModeDeploy, "lost-native-response")
			tx, err = store.BeginTx(ctx, nil)
			require.NoError(t, err)
			require.ErrorIs(t, retry.Authorize(ctx, tx, nil, compiled), ErrDeploymentAdmissionUnavailable)
			require.NoError(t, tx.Rollback())
			require.Equal(t, int32(1), authorizations.Load(), "pending requests cannot actively reauthorize")
			env := &cp.Environment{ProjectUuid: &project, Uuid: environment, ProjectId: "p", Id: "e"}
			server := &Server{Database: store, DeploymentAdmissions: service}
			key := "lost-native-response"
			_, _, _, err = server.recoverRequestedPipeline(ctx, actor, "org", env, model.DeploymentModeDeploy, &key, nil)
			require.ErrorIs(t, err, ErrDeploymentAdmissionUnavailable, "public pending replay remains unavailable")
			if coreCommitted {
				require.ErrorIs(t, service.ReconcileUnsettled(ctx, 100), ErrDeploymentAdmissionUnavailable)
				require.Equal(t, original, <-requests, "refused absence uses the persisted predecessor and digest")
				tx, err = store.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer func() { _ = tx.Rollback() }()
				receipt, err := model.GetDeploymentAdmissionReceipt(ctx, tx, "org", attempt.AdmissionID)
				require.NoError(t, err)
				require.False(t, receipt.CoreSettledAt.Valid, "Core committed conflict must not become checked local absence")
				_, err = model.LatestNativeAdmissionWatermark(ctx, tx, "org", project, environment)
				require.Error(t, err)
				return
			}
			require.NoError(t, service.ReconcileUnsettled(ctx, 100))
			require.Equal(t, original, <-requests, "reconciliation never recompiles pending intent")
			assertSealedAdmission(t, ctx, store, "org", attempt.AdmissionID, 9)
			tx, err = store.BeginTx(ctx, nil)
			require.NoError(t, err)
			latest, err := model.LatestNativeAdmissionWatermark(ctx, tx, "org", project, environment)
			require.NoError(t, err)
			require.Equal(t, predecessor, latest, "checked absence is not a predecessor")
			require.NoError(t, tx.Commit())
		})
	}
}

func TestNativeAdmissionWatermarkWirePreservesCommittedReplayAfterHistoryAdvances(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := deploymentAdmissionTestDatabase(t, ctx, os.Getenv("MODEL_TEST_DB_CONNECTION_STRING"))
	project, environment, actor := uuid.New(), uuid.New(), uuid.New()
	predecessor := retainCheckedNativeAdmission(t, ctx, store, project, environment, actor, 7)
	requests := make(chan cp.DeploymentAdmissionRequest, 2)
	settlements := make(chan map[string]json.RawMessage, 1)
	var authorized atomic.Value
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		idIndex := len(parts) - 1
		settle := strings.HasSuffix(r.URL.Path, "/settle")
		var original cp.DeploymentAdmissionRequest
		if settle {
			original = authorized.Load().(cp.DeploymentAdmissionRequest)
			idIndex--
			var body map[string]json.RawMessage
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			settlements <- body
		} else {
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&original))
			requests <- original
			authorized.Store(original)
		}
		receipt := cp.DeploymentAdmissionReceipt{AdmissionId: uuid.MustParse(parts[idIndex]), ProjectUuid: project, EnvironmentUuid: environment,
			RequestDigest: original.RequestDigest, Mode: string(original.Mode), Epoch: 8, State: cp.AdmissionStateActive}
		if settle {
			receipt.State, receipt.Outcome = cp.AdmissionStateSettled, refString("succeeded")
		}
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(receipt))
	}))
	defer peer.Close()
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	service := &OrdinaryDeploymentAdmissionService{Database: store, Core: client, BearerToken: "owned-test-token"}
	key := "committed-native"
	attempt := service.NewAttempt("org", project, environment, actor, model.DeploymentModeDeploy, key)
	compiled := ordinaryDeploymentCompiledIntent{NativeTarget: nativeTargetFixture(), ProjectID: "p", EnvironmentID: "e", RunnerID: "native_pool", Manifest: []byte(`{"workloads":{}}`), Graph: []byte(`{}`), Tofu: []byte("terraform {}")}
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, attempt.Authorize(ctx, tx, nil, compiled))
	first := <-requests
	require.Equal(t, predecessor, first.NativeAdmissionWatermark)
	deployment, err := store.CreateDeployment(ctx, tx, "org", "p", "e", model.CreateDeploymentParams{
		NativeTarget:         compiled.NativeTarget,
		CreatedBy:            actor,
		DeploymentEnvUuid:    environment,
		Mode:                 model.DeploymentModeDeploy,
		RunnerId:             compiled.RunnerID,
		RunnerLogLevel:       "info",
		Manifest:             model.EncodedDeploymentManifest(compiled.Manifest),
		Graph:                model.EncodedDeploymentGraph(compiled.Graph),
		Tofu:                 compiled.Tofu,
		IdempotencyKeyDigest: opt.Of(digestIdempotencyKey(key)),
	})
	require.NoError(t, err)
	require.NoError(t, attempt.Bind(ctx, tx, deployment.Id))
	require.NoError(t, tx.Commit())
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, deployment.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	require.NoError(t, service.SettleDeployment(ctx, deployment.Id))
	settlement := <-settlements
	require.Len(t, settlement, 4, "settlement carries scope, original digest and outcome only")
	var digest string
	require.NoError(t, json.Unmarshal(settlement["request_digest"], &digest))
	require.Equal(t, first.RequestDigest, digest)
	retainCheckedNativeAdmission(t, ctx, store, project, environment, actor, 10)
	server := &Server{Database: store, DeploymentAdmissions: service}
	_, target, found, err := server.recoverRequestedPipeline(ctx, actor, "org", &cp.Environment{ProjectUuid: &project, Uuid: environment, ProjectId: "p", Id: "e"}, model.DeploymentModeDeploy, &key, nil)
	require.NoError(t, err)
	require.True(t, found, "accepted replay precedes fresh history derivation")
	require.Equal(t, compiled.NativeTarget, target)
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	receipt, err := model.GetDeploymentAdmissionReceiptByDeployment(ctx, tx, deployment.Id)
	require.NoError(t, err)
	require.Equal(t, first, deploymentAdmissionRequest(receipt), "retained accepted intent does not adopt the newer predecessor")
	require.NoError(t, tx.Commit())
	require.Empty(t, requests, "committed replay performs no fresh Core authorize")
}
