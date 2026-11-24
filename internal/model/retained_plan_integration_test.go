//go:build integration

package model

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRetainedApplyDispatchAndUnknownOutcomeFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	env, actor := uuid.New(), uuid.New()
	params := CreateDeploymentParams{DeploymentEnvUuid: env, Mode: DeploymentModeDeployPlan, CreatedBy: actor, RunnerId: "runner", RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}")}
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	source, err := store.CreateDeployment(ctx, tx, "unknown-org", "p", "e", params)
	require.NoError(t, err)
	p := RetainedPlan{ID: uuid.New(), OrgID: "unknown-org", ProjectID: "p", EnvID: "e", EnvironmentID: env, CreatorID: actor, SourceDeploymentID: source.Id, BaseDeploymentID: uuid.Nil, RunnerID: "runner", Binding: RetainedPlanBinding{Schema: "stellwerk.retained-plan/v1"}, Recipient: "recipient", EncryptedIdentity: "vault:v1:wrapped", Lifecycle: []byte(`{}`), RequestDigest: "digest"}
	require.NoError(t, InsertRetainedPlan(ctx, tx, p))
	require.NoError(t, tx.Commit())
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, source.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded})
	require.NoError(t, err)
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	params.Mode = DeploymentModeDeploy
	apply, err := store.CreateDeployment(ctx, tx, "unknown-org", "p", "e", params)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE retained_plans SET apply_deployment_id=$2,ciphertext=$3,ciphertext_sha256='digest' WHERE id=$1`, p.ID, apply.Id, []byte("ciphertext"))
	require.NoError(t, err)
	dispatch, err := LoadRetainedPlanDispatch(ctx, tx, "unknown-org", apply.Id)
	require.NoError(t, err)
	require.False(t, dispatch.Export)
	apply.RetainedPlan = dispatch
	require.NoError(t, tx.Commit())
	_, err = BeginDeploymentDispatch(ctx, store, apply)
	require.NoError(t, err)
	_, err = BeginDeploymentDispatch(ctx, store, apply)
	require.ErrorContains(t, err, "unresolved")
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, apply.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusFailed})
	require.NoError(t, err)
	require.ErrorContains(t, replaceHead(ctx, store, params), "outcome")
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, RecordDeploymentCompletion(ctx, tx, apply.Id))
	require.NoError(t, tx.Commit())
	require.NoError(t, replaceHead(ctx, store, params))
}
