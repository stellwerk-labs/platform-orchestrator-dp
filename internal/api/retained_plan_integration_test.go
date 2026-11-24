//go:build integration

package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func TestRetainedPlanIngestAndApplyUsesFrozenSource(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	f.server.Vault = managedLogVault(t)
	service := &RetainedPlanService{Server: f.server}
	f.server.RetainedPlans = service
	envResponse, err := f.coreClient.GetEnvironmentWithResponse(f.ctx, queueOrg, queueProject, queueEnv)
	require.NoError(t, err)
	req := RetainedPlanRequest{OrgID: queueOrg, ProjectID: queueProject, EnvID: queueEnv, Environment: *envResponse.JSON200, Manifest: DeploymentManifest{}, Backend: "kubernetes", Tool: "opentofu"}
	operation := uuid.New()
	plan, err := service.CreatePlan(f.ctx, f.actor, req, operation)
	require.NoError(t, err)
	again, err := service.CreatePlan(f.ctx, f.actor, req, operation)
	require.NoError(t, err)
	require.Equal(t, plan.SourceDeploymentID, again.SourceDeploymentID)
	changed := req
	changed.Tool = "other"
	_, err = service.CreatePlan(f.ctx, f.actor, changed, operation)
	require.ErrorContains(t, err, "identity mismatch")
	review := []byte(`{"planned_values":{"root_module":{}}}`)
	snapshot := retainedSnapshot{Binding: plan.Binding, OS: "linux", Arch: "amd64", ToolSHA256: string(bytes.Repeat([]byte("a"), 64)), Archive: []byte("private archive"), Review: review}
	plain, err := json.Marshal(snapshot)
	require.NoError(t, err)
	recipient, err := age.ParseX25519Recipient(plan.Recipient)
	require.NoError(t, err)
	var encrypted bytes.Buffer
	writer, err := age.Encrypt(&encrypted, recipient)
	require.NoError(t, err)
	_, err = writer.Write(plain)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	source, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, plan.SourceDeploymentID, model.GetModeDefault)
	require.NoError(t, err)
	require.True(t, source.HasRetainedPlan)
	planLogMaterial := retainedTestLogMaterial(t, f, source)
	require.NotEqual(t, plan.Recipient, planLogMaterial.Recipient)
	f.server.RetainedPlans = nil
	_, err = f.server.retainedBundle(f.ctx, source)
	require.ErrorContains(t, err, "disabled")
	encoded := base64.StdEncoding.EncodeToString(encrypted.Bytes())
	err = f.server.ApplyRunnerDeploymentResult(f.ctx, queueOrg, queueRunner, plan.SourceDeploymentID, uuid.NewString(), DeploymentResultsUpdateBody{Status: Success, Outputs: &encoded})
	require.NoError(t, err)
	f.server.RetainedPlans = service
	plan, err = service.GetPlan(f.ctx, queueOrg, operation)
	require.NoError(t, err)
	require.Equal(t, encrypted.Bytes(), plan.Ciphertext)
	require.True(t, plan.Subject.Valid)
	gotReview, err := service.Review(f.ctx, queueOrg, operation)
	require.NoError(t, err)
	require.JSONEq(t, string(review), string(gotReview))
	_, err = f.store.DeleteDeploymentOutputsByCompletionDate(f.ctx, nil, time.Now().Add(time.Hour))
	require.NoError(t, err)
	gotReview, err = service.Review(f.ctx, queueOrg, operation)
	require.NoError(t, err)
	require.JSONEq(t, string(review), string(gotReview))
	apply, err := service.ApplyPlan(f.ctx, f.actor, queueOrg, operation, "approval-subject", "apply-key")
	require.NoError(t, err)
	savedCore := f.server.ControlPlaneClient
	f.server.ControlPlaneClient = nil // Existing accepted identity recovery needs no new Runner capability lookup.
	replay, err := service.ApplyPlan(f.ctx, f.actor, queueOrg, operation, "approval-subject", "apply-key")
	f.server.ControlPlaneClient = savedCore
	require.NoError(t, err)
	require.Equal(t, apply.Id, replay.Id)
	applyLogMaterial := retainedTestLogMaterial(t, f, apply)
	require.NotEqual(t, planLogMaterial.Recipient, applyLogMaterial.Recipient)
	require.NotEqual(t, plan.Recipient, applyLogMaterial.Recipient)
	require.Equal(t, apply.EncryptedLogsRecipient, replay.EncryptedLogsRecipient)
	_, err = service.ApplyPlan(f.ctx, f.actor, queueOrg, operation, "other-subject", "apply-key")
	require.ErrorContains(t, err, "identity mismatch")
	_, sourceManifest, sourceTofu, sourceGraph, err := f.store.GetDeployment(f.ctx, nil, queueOrg, plan.SourceDeploymentID, model.GetModeDefault)
	require.NoError(t, err)
	_, applyManifest, applyTofu, applyGraph, err := f.store.GetDeployment(f.ctx, nil, queueOrg, apply.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, sourceManifest, applyManifest)
	require.Equal(t, sourceTofu, applyTofu)
	require.Equal(t, sourceGraph, applyGraph)
	// The ordinary transfer output is cleaned independently of the retained
	// copy used by post tasks.
	var encryptedOutputs bytes.Buffer
	outputWriter, err := age.Encrypt(&encryptedOutputs, recipient)
	require.NoError(t, err)
	_, err = outputWriter.Write([]byte(`{"url":{"value":"private-output"}}`))
	require.NoError(t, err)
	require.NoError(t, outputWriter.Close())
	applyState, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, apply.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.True(t, applyState.HasRetainedPlan)
	f.server.RetainedPlans = nil
	_, err = f.server.retainedBundle(f.ctx, applyState)
	require.ErrorContains(t, err, "disabled")
	outputEncoded := base64.StdEncoding.EncodeToString(encryptedOutputs.Bytes())
	err = f.server.ApplyRunnerDeploymentResult(f.ctx, queueOrg, queueRunner, apply.Id, uuid.NewString(), DeploymentResultsUpdateBody{Status: Success, Outputs: &outputEncoded})
	require.NoError(t, err)
	f.server.RetainedPlans = service
	_, err = f.store.DeleteDeploymentOutputsByCompletionDate(f.ctx, nil, time.Now().Add(time.Hour))
	require.NoError(t, err)
	output, err := service.BoundOutputs(f.ctx, queueOrg, operation)
	require.NoError(t, err)
	require.JSONEq(t, `{"url":{"value":"private-output"}}`, string(output))
	_, err = f.server.retainedBundle(f.ctx, &model.DeploymentSummary{OrgId: queueOrg, Id: uuid.New(), HasRetainedPlan: true})
	require.ErrorContains(t, err, "binding unavailable")

	_, err = service.BoundOutputs(f.ctx, "other-org", operation)
	require.Error(t, err)

}

func retainedTestLogMaterial(t *testing.T, f *queueFixture, deployment *model.DeploymentSummary) *model.DeploymentLogMaterial {
	t.Helper()
	require.True(t, deployment.EncryptedLogsRecipient.IsSet())
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	material, err := model.GetDeploymentLogMaterial(f.ctx, tx, queueOrg, deployment.Id)
	require.NoError(t, err)
	require.Equal(t, deployment.EncryptedLogsRecipient.Must(), material.Recipient)
	identity, err := (DeploymentLogMaterialService{Vault: f.server.Vault}).Identity(f.ctx, *material)
	require.NoError(t, err)
	var encrypted bytes.Buffer
	recipient, err := age.ParseX25519Recipient(material.Recipient)
	require.NoError(t, err)
	writer, err := age.Encrypt(&encrypted, recipient)
	require.NoError(t, err)
	_, err = writer.Write([]byte("pipeline operational log"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	reader, err := age.Decrypt(&encrypted, identity)
	require.NoError(t, err)
	plain, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "pipeline operational log", string(plain))
	return material
}
