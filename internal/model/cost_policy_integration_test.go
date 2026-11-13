//go:build integration

package model

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stretchr/testify/require"
)

func TestCostPolicyDefaultsAuditRevisionAndAdmissionIndependence(t *testing.T) {
	f := newQueueModelFixture(t)
	policy, err := ReadCostPolicy(f.ctx, f.store)
	require.NoError(t, err)
	require.False(t, policy.Enabled)
	require.False(t, policy.APIKeyConfigured)
	require.EqualValues(t, 1, policy.Revision)
	require.Nil(t, policy.UpdatedBy)
	actor := uuid.New()
	// Deployment admission holds maintenance's row; cost settings never wait for it.
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, requireDeploymentAcceptance(f.ctx, tx, uuid.Nil, "queue-org", "p", "e", f.env, actor, DeploymentModeDeploy))
	updated, err := UpdateCostPolicy(f.ctx, f.store, actor, policy.Revision, false, ref.Ref("vault:v1:fixture-ciphertext"))
	require.NoError(t, err)
	require.False(t, updated.Enabled)
	require.True(t, updated.APIKeyConfigured)
	require.EqualValues(t, 2, updated.Revision)
	require.Equal(t, &actor, updated.UpdatedBy)
	require.False(t, updated.UpdatedAt.IsZero())
	_, err = UpdateCostPolicy(f.ctx, f.store, actor, 1, true, nil)
	require.ErrorIs(t, err, ErrSettingsRevision)
	var count int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM installation_cost_policy_events WHERE revision=2 AND NOT enabled AND api_key_configured AND api_key_changed AND changed_by=$1`, actor).Scan(&count))
	require.Equal(t, 1, count)
}

func TestCostPolicySnapshotRetainsPolicyAcrossDispatchRetries(t *testing.T) {
	for _, mode := range []DeploymentMode{DeploymentModeDeployPlan, DeploymentModeObservation} {
		t.Run(string(mode), func(t *testing.T) {
			f := newQueueModelFixture(t)
			dep, err := f.create(t, mode, nil)
			require.NoError(t, err)
			_, err = UpdateCostPolicy(f.ctx, f.store, uuid.New(), 1, false, ref.Ref("vault:v1:fixture-ciphertext"))
			require.NoError(t, err)
			require.NoError(t, FreezeDeploymentCostPolicy(f.ctx, f.store, dep))
			want := *dep.CostPolicy
			require.Equal(t, DeploymentCostPolicy{Enabled: false, PolicyRevision: 2}, want)
			_, err = BeginDeploymentDispatch(f.ctx, f.store, dep)
			require.NoError(t, err)
			_, err = UpdateCostPolicy(f.ctx, f.store, uuid.New(), 2, true, nil)
			require.NoError(t, err)
			dep.CostPolicy = nil
			require.NoError(t, FreezeDeploymentCostPolicy(f.ctx, f.store, dep))
			require.Equal(t, want, *dep.CostPolicy)
		})
	}
}

func TestCostPolicyLeavesOldInFlightDeploymentUnchanged(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		f := newQueueModelFixture(t)
		dep, err := f.create(t, DeploymentModeDeployPlan, nil)
		require.NoError(t, err)
		f.inTx(t, func(tx TxWithCommit) {
			_, err := tx.ExecContext(f.ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now(),deadline=now()+interval '1 hour',legacy=$2 WHERE deployment_id=$1`, dep.Id, legacy)
			require.NoError(t, err)
		})
		require.NoError(t, FreezeDeploymentCostPolicy(f.ctx, f.store, dep))
		require.Nil(t, dep.CostPolicy)
	}
}

func TestCostPolicyKeyPreserveClearAndRequired(t *testing.T) {
	f := newQueueModelFixture(t)
	actor := uuid.New()
	_, err := UpdateCostPolicy(f.ctx, f.store, actor, 1, true, nil)
	require.ErrorIs(t, err, ErrCostAPIKeyRequired)
	configured, err := UpdateCostPolicy(f.ctx, f.store, actor, 1, true, ref.Ref("vault:v1:first"))
	require.NoError(t, err)
	require.True(t, configured.APIKeyConfigured)
	_, err = UpdateCostPolicy(f.ctx, f.store, actor, configured.Revision, true, nil)
	require.NoError(t, err)
	stored, err := ReadCostAPIKeyCiphertext(f.ctx, f.store)
	require.NoError(t, err)
	require.Equal(t, "vault:v1:first", stored)
	_, err = UpdateCostPolicy(f.ctx, f.store, actor, 3, true, ref.Ref(""))
	require.ErrorIs(t, err, ErrCostAPIKeyRequired)
	cleared, err := UpdateCostPolicy(f.ctx, f.store, actor, 3, false, ref.Ref(""))
	require.NoError(t, err)
	require.False(t, cleared.Enabled)
	require.False(t, cleared.APIKeyConfigured)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var events string
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT json_agg(e)::text FROM installation_cost_policy_events e`).Scan(&events))
	require.NotContains(t, events, "vault:v1:")
	require.NotContains(t, events, "ciphertext")
	require.Contains(t, events, `"api_key_changed":false`)
}
