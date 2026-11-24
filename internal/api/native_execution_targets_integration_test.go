//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func TestNativeTargetPreparedReceiptReconstructsExactTypedCoreWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := deploymentAdmissionTestDatabase(t, ctx, os.Getenv("MODEL_TEST_DB_CONNECTION_STRING"))
	requests := make(chan cp.DeploymentAdmissionRequest, 2)
	// This owned HTTP peer proves DP encoding/reconstruction, not Core/IAM admission policy.
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body cp.DeploymentAdmissionRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		requests <- body
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer peer.Close()
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	service := &OrdinaryDeploymentAdmissionService{Database: store, Core: client, BearerToken: "owned-test-token"}
	attempt := service.NewAttempt("org", uuid.MustParse("22222222-2222-4222-8222-222222222222"), uuid.MustParse("33333333-3333-4333-8333-333333333333"), uuid.MustParse("44444444-4444-4444-8444-444444444444"), model.DeploymentModeDeploy, "native-wire")
	compiled := ordinaryDeploymentCompiledIntent{NativeTarget: nativeTargetFixture(), ProjectID: "p", EnvironmentID: "e", RunnerID: "native_pool", Manifest: []byte(`{}`), Graph: []byte(`{}`), Tofu: []byte("terraform {}")}
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.ErrorIs(t, attempt.Authorize(ctx, tx, nil, compiled), ErrDeploymentAdmissionUnavailable)
	require.NoError(t, tx.Rollback())
	first := <-requests
	require.Equal(t, compiled.NativeTarget, first.NativeTarget)
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	receipt, err := model.GetDeploymentAdmissionReceipt(ctx, tx, "org", attempt.AdmissionID)
	require.NoError(t, err)
	require.Equal(t, compiled.NativeTarget, receipt.NativeTarget)
	require.NoError(t, tx.Commit())
	request := deploymentAdmissionRequest(receipt)
	_, err = client.AuthorizeDeploymentAdmissionWithResponse(ctx, "org", receipt.AdmissionID, request)
	require.NoError(t, err)
	require.Equal(t, first, <-requests, "the durable receipt must reconstruct the same complete typed request")
	// A changed target cannot reuse a prepared pre-commit admission after response loss.
	changed := *compiled.NativeTarget
	changed.RunnerUuid = uuid.New()
	compiled.NativeTarget = &changed
	retry := service.NewAttempt("org", receipt.ProjectUUID, receipt.EnvironmentUUID, receipt.ActorID, model.DeploymentModeDeploy, "native-wire")
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.ErrorIs(t, retry.Authorize(ctx, tx, nil, compiled), ErrDeploymentAdmissionUnavailable)
	require.NoError(t, tx.Rollback())
}

func TestLegacyCompiledAdmissionDigestRetainsHistoricalBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := deploymentAdmissionTestDatabase(t, ctx, os.Getenv("MODEL_TEST_DB_CONNECTION_STRING"))
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer peer.Close()
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	service := &OrdinaryDeploymentAdmissionService{Database: store, Core: client, BearerToken: "owned-test-token"}
	attempt := service.NewAttempt("org", uuid.MustParse("22222222-2222-4222-8222-222222222222"), uuid.MustParse("33333333-3333-4333-8333-333333333333"), uuid.MustParse("44444444-4444-4444-8444-444444444444"), model.DeploymentModeDeploy, "legacy-wire")
	attempt.AdmissionID = uuid.MustParse("55555555-5555-4555-8555-555555555555")
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.ErrorIs(t, attempt.Authorize(ctx, tx, nil, ordinaryDeploymentCompiledIntent{ProjectID: "p", EnvironmentID: "e", RunnerID: "runner", Manifest: []byte(`{}`), Graph: []byte(`{}`), Tofu: []byte("terraform {}")}), ErrDeploymentAdmissionUnavailable)
	require.NoError(t, tx.Rollback())
	require.Equal(t, "c70e3097798d01757081148784341d5836586d4e9f5e03ff36570c9dda3eed4c", attempt.requestDigest)
	receipt := attempt.receipt("pending", uuid.Nil, "")
	raw, err := json.Marshal(deploymentAdmissionRequest(&receipt))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "native_target")
	require.NotContains(t, string(raw), "native_admission_watermark")
	expected := []byte(
		`{"actor_id":"44444444-4444-4444-8444-444444444444","environment_uuid":"33333333-3333-4333-8333-333333333333","mode":"deploy","module_versions":{},"project_uuid":"22222222-2222-4222-8222-222222222222","request_digest":"c70e3097798d01757081148784341d5836586d4e9f5e03ff36570c9dda3eed4c"}`,
	)
	// This golden binds wire bytes, including field order; semantic JSON equality is insufficient.
	require.True(t, bytes.Equal(expected, raw), "legacy request bytes changed")
}

func TestNativeTargetSavedPlanCaptureAndApplyChildUseSamePersistedTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := deploymentAdmissionTestDatabase(t, ctx, os.Getenv("MODEL_TEST_DB_CONNECTION_STRING"))
	target := nativeTargetFixture()
	env, actor := uuid.New(), uuid.New()
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	graph := model.EncodedDeploymentGraph(`{"nodes":{},"edges":{},"workloads":{},"shared_resources":{},"leaf_resources":{}}`)
	dep, err := store.CreateDeployment(
		ctx,
		tx,
		"org",
		"p",
		"e",
		model.CreateDeploymentParams{
			NativeTarget:      target,
			CreatedBy:         actor,
			DeploymentEnvUuid: env,
			Mode:              model.DeploymentModeDeployPlan,
			RunnerId:          target.RunnerId,
			RunnerLogLevel:    "info",
			Manifest:          model.EncodedDeploymentManifest(`{"workloads":{}}`),
			Graph:             graph,
			Tofu:              model.RawTofu("terraform {}"),
		},
	)
	require.NoError(t, err)
	capture := pipelinePlanCapture{NativeTarget: target, BaseDeploymentID: dep.Id, Request: deploymentRequest{NativeTarget: target}}
	raw, err := json.Marshal(capture)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO deployment_pipeline_runs(deployment_id,backend,definition,progress,plan_capture) VALUES($1,'native','{}','{}',$2)`, dep.Id, raw)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, dep.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	dep, _, _, _, err = store.GetDeployment(ctx, tx, "org", dep.Id, model.GetModeDefault)
	require.NoError(t, err)
	saved, backend, err := readPipelinePlanCapture(ctx, tx, dep.Id, true)
	require.NoError(t, err)
	require.Equal(t, target, saved.NativeTarget)
	require.Equal(t, target, saved.Request.NativeTarget)
	server := &Server{Database: store, Vault: managedLogVault(t)}
	source := &pipelinePlanSource{Deployment: dep, Run: &model.DeploymentPipeline{}, Capture: saved, Backend: backend, Manifest: model.EncodedDeploymentManifest(`{"workloads":{}}`), Graph: graph, Tofu: model.RawTofu("terraform {}")}
	child, _, err := server.createPipelineApply(ctx, tx, source, nil, actor, "source-subject", "apply-key")
	require.NoError(t, err)
	require.Equal(t, target, child.NativeTarget)
	require.NoError(t, tx.Commit())
	restored, _, _, _, err := store.GetDeployment(ctx, nil, "org", child.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, target, restored.NativeTarget)
}
