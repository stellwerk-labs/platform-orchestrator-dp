//go:build integration

package model

import (
	"encoding/json"
	"io/fs"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/require"
)

// These tests exercise owned PostgreSQL storage, not Core/IAM policy.
func TestNativeAdmissionKnowledgeUsesCheckedReceiptsAcrossPlanAndRetention(t *testing.T) {
	f := newQueueModelFixture(t)
	intent := testDeploymentAdmissionIntent()
	intent.NativeTarget = testNativeTarget()
	f.inTx(t, func(tx TxWithCommit) {
		latest, err := LatestNativeAdmissionWatermark(f.ctx, tx, intent.OrgID, intent.ProjectUUID, intent.EnvironmentUUID)
		require.NoError(t, err)
		require.Nil(t, latest)
	})
	for i, mode := range []DeploymentMode{DeploymentModeDeploy, DeploymentModeDeployPlan, DeploymentModeRollbackPlan} {
		intent.AdmissionID = uuid.New()
		intent.Mode = mode
		f.inTx(t, func(tx TxWithCommit) {
			var err error
			intent.NativeAdmissionWatermark, err = LatestNativeAdmissionWatermark(f.ctx, tx, intent.OrgID, intent.ProjectUUID, intent.EnvironmentUUID)
			require.NoError(t, err)
			_, err = PrepareDeploymentAdmissionReceipt(f.ctx, tx, intent)
			require.NoError(t, err)
			// There is deliberately no Deployment FK. Retained receipts alone
			// must carry knowledge after normal Deployment retention.
			bound, err := BindDeploymentAdmissionReceipt(f.ctx, tx, intent, uuid.New(), int64(i+1))
			require.NoError(t, err)
			require.Equal(t, intent.NativeAdmissionWatermark, bound.NativeAdmissionWatermark)
			require.NoError(t, MarkDeploymentAdmissionCoreSettled(f.ctx, tx, intent.OrgID, intent.AdmissionID, "failed", int64(i+1)))
			latest, err := LatestNativeAdmissionWatermark(f.ctx, tx, intent.OrgID, intent.ProjectUUID, intent.EnvironmentUUID)
			require.NoError(t, err)
			require.Equal(t, &cp.NativeAdmissionWatermark{AdmissionId: intent.AdmissionID, Epoch: int64(i + 1)}, latest)
		})
	}
	f.inTx(t, func(tx TxWithCommit) {
		absent := intent
		absent.AdmissionID = uuid.New()
		_, err := PrepareDeploymentAdmissionReceipt(f.ctx, tx, absent)
		require.NoError(t, err)
		_, err = SealDeploymentAdmissionAbsent(f.ctx, tx, absent, 0)
		require.NoError(t, err)
		require.NoError(t, MarkDeploymentAdmissionCoreSettled(f.ctx, tx, absent.OrgID, absent.AdmissionID, "absent", 9))
		latest, err := LatestNativeAdmissionWatermark(f.ctx, tx, intent.OrgID, intent.ProjectUUID, intent.EnvironmentUUID)
		require.NoError(t, err)
		require.Equal(t, &cp.NativeAdmissionWatermark{AdmissionId: intent.AdmissionID, Epoch: 3}, latest, "checked absence/raw epoch gaps do not advance knowledge")
		retained, err := GetDeploymentAdmissionReceipt(f.ctx, tx, intent.OrgID, intent.AdmissionID)
		require.NoError(t, err)
		byDeployment, err := GetDeploymentAdmissionReceiptByDeployment(f.ctx, tx, retained.DeploymentID.UUID)
		require.NoError(t, err)
		require.Equal(t, intent.NativeAdmissionWatermark, byDeployment.NativeAdmissionWatermark)
	})
}

func TestNativeAdmissionKnowledgeRefusesUncertainOrContradictoryStorage(t *testing.T) {
	f := newQueueModelFixture(t)
	intent := testDeploymentAdmissionIntent()
	intent.NativeTarget = testNativeTarget()
	f.inTx(t, func(tx TxWithCommit) {
		_, err := PrepareDeploymentAdmissionReceipt(f.ctx, tx, intent)
		require.NoError(t, err)
		_, err = BindDeploymentAdmissionReceipt(f.ctx, tx, intent, uuid.New(), 7)
		require.NoError(t, err)
		require.NoError(t, MarkDeploymentAdmissionCoreSettled(f.ctx, tx, intent.OrgID, intent.AdmissionID, "succeeded", 7))
	})
	f.inTx(t, func(tx TxWithCommit) {
		latest, err := LatestNativeAdmissionWatermark(f.ctx, tx, intent.OrgID, intent.ProjectUUID, intent.EnvironmentUUID)
		require.NoError(t, err)
		require.Equal(t, &cp.NativeAdmissionWatermark{AdmissionId: intent.AdmissionID, Epoch: 7}, latest)
	})
	for _, mutation := range []struct{ name, sql string }{
		{"unchecked committed", `UPDATE deployment_admission_receipts SET core_settled_at=NULL`},
		{"legacy latest", `UPDATE deployment_admission_receipts SET native_target=NULL`},
		{"malformed latest", `UPDATE deployment_admission_receipts SET native_target='{}'`},
		{"zero actor", `UPDATE deployment_admission_receipts SET actor_id='00000000-0000-0000-0000-000000000000'`},
		{"invalid predecessor", `UPDATE deployment_admission_receipts SET native_admission_watermark='{"admission_id":"00000000-0000-0000-0000-000000000000","epoch":1}'`},
		{"unknown predecessor field", `UPDATE deployment_admission_receipts SET native_admission_watermark='{"admission_id":"11111111-1111-4111-8111-111111111111","epoch":1,"extra":true}'`},
		{"alias-only predecessor ID", `UPDATE deployment_admission_receipts SET native_admission_watermark='{"Admission_id":"11111111-1111-4111-8111-111111111111","epoch":1}'`},
		{"alias-only predecessor epoch", `UPDATE deployment_admission_receipts SET native_admission_watermark='{"admission_id":"11111111-1111-4111-8111-111111111111","Epoch":1}'`},
		{"conflicting predecessor ID aliases", `UPDATE deployment_admission_receipts SET native_admission_watermark='{"admission_id":"11111111-1111-4111-8111-111111111111","Admission_id":"22222222-2222-4222-8222-222222222222","epoch":1}'`},
		{"conflicting predecessor epoch aliases", `UPDATE deployment_admission_receipts SET native_admission_watermark='{"admission_id":"11111111-1111-4111-8111-111111111111","epoch":1,"Epoch":2}'`},
		{"unknown target field", `UPDATE deployment_admission_receipts SET native_target=native_target||'{"extra":true}'::jsonb`},
		{"alias-only target Runner ID", `UPDATE deployment_admission_receipts SET native_target=(native_target-'runner_id')||jsonb_build_object('Runner_id',native_target->'runner_id')`},
		{"alias-only target tools digest", `UPDATE deployment_admission_receipts SET native_target=(native_target-'tools_digest')||jsonb_build_object('Tools_digest',native_target->'tools_digest')`},
		{"conflicting target Runner ID aliases", `UPDATE deployment_admission_receipts SET native_target=native_target||'{"Runner_id":"other_native_pool"}'::jsonb`},
		{"conflicting target Runner UUID aliases", `UPDATE deployment_admission_receipts SET native_target=native_target||'{"Runner_uuid":"22222222-2222-4222-8222-222222222222"}'::jsonb`},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Rollback()) }()
			_, err = tx.ExecContext(f.ctx, mutation.sql)
			require.NoError(t, err)
			var before, after string
			require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT to_jsonb(r)::text FROM deployment_admission_receipts r WHERE admission_id=$1`, intent.AdmissionID).Scan(&before))
			_, err = LatestNativeAdmissionWatermark(f.ctx, tx, intent.OrgID, intent.ProjectUUID, intent.EnvironmentUUID)
			_, conflict := IsErrConflict(err)
			require.True(t, conflict, "%v", err)
			require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT to_jsonb(r)::text FROM deployment_admission_receipts r WHERE admission_id=$1`, intent.AdmissionID).Scan(&after))
			require.Equal(t, before, after, "a refused read must preserve the exact stored receipt")
		})
	}
	for _, disposition := range []string{"pending", "absent", "equal epoch", "foreign org", "foreign project", "foreign environment"} {
		t.Run(disposition, func(t *testing.T) {
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Rollback()) }()
			other := intent
			other.AdmissionID = uuid.New()
			switch disposition {
			case "foreign org":
				other.OrgID = "other-org"
			case "foreign project":
				other.ProjectUUID = uuid.New()
			case "foreign environment":
				other.EnvironmentUUID = uuid.New()
			}
			_, err = PrepareDeploymentAdmissionReceipt(f.ctx, tx, other)
			require.NoError(t, err)
			if disposition == "absent" {
				_, err = SealDeploymentAdmissionAbsent(f.ctx, tx, other, 0)
				require.NoError(t, err)
			}
			if disposition == "equal epoch" {
				_, err = BindDeploymentAdmissionReceipt(f.ctx, tx, other, uuid.New(), 7)
				require.NoError(t, err)
				require.NoError(t, MarkDeploymentAdmissionCoreSettled(f.ctx, tx, other.OrgID, other.AdmissionID, "failed", 7))
			}
			latest, err := LatestNativeAdmissionWatermark(f.ctx, tx, intent.OrgID, intent.ProjectUUID, intent.EnvironmentUUID)
			if strings.HasPrefix(disposition, "foreign ") {
				require.NoError(t, err)
				require.Equal(t, intent.AdmissionID, latest.AdmissionId)
			} else {
				_, conflict := IsErrConflict(err)
				require.True(t, conflict, "%v", err)
			}
			var count int
			require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployment_admission_receipts`).Scan(&count))
			require.Equal(t, 2, count, "a refused read must preserve all evidence")
		})
	}
}

func TestNativeAdmissionWatermarkMigrationPreservesOldWritersAndGuardsDowngrade(t *testing.T) {
	f := newQueueModelFixture(t)
	db := f.store.(*databaser).DB
	source, err := fs.Sub(embedMigrations, "migrations")
	require.NoError(t, err)
	provider, err := goose.NewProvider(goose.DialectPostgres, db, source, goose.WithDisableGlobalRegistry(true), goose.WithGoMigrations(goose.NewGoMigration(2, &goose.GoFunc{RunTx: hstandardoutbox.MigrateUp01}, &goose.GoFunc{RunTx: hstandardoutbox.MigrateDown01})))
	require.NoError(t, err)
	_, err = provider.DownTo(f.ctx, 54)
	require.NoError(t, err)
	id := uuid.New()
	_, err = db.ExecContext(f.ctx, `INSERT INTO deployment_admission_receipts
 (org_id,admission_id,project_uuid,environment_uuid,actor_id,request_digest,mode,module_versions,carry_forward_module_versions,confirmed_restricted_version_uuids,disposition)
 VALUES('old-writer',$1,$2,$3,$4,$5,'deploy','{}','{}','[]','pending')`, id, uuid.New(), uuid.New(), uuid.New(), strings.Repeat("a", 64))
	require.NoError(t, err)
	_, err = provider.Up(f.ctx)
	require.NoError(t, err)
	var absent bool
	require.NoError(t, db.QueryRowContext(f.ctx, `SELECT native_admission_watermark IS NULL FROM deployment_admission_receipts WHERE admission_id=$1`, id).Scan(&absent))
	require.True(t, absent)
	for _, mode := range []DeploymentMode{DeploymentModeDeploy, DeploymentModeDeployPlan, DeploymentModeRollbackPlan} {
		raw, err := json.Marshal(&cp.NativeAdmissionWatermark{AdmissionId: uuid.New(), Epoch: 1})
		require.NoError(t, err)
		_, err = db.ExecContext(f.ctx, `UPDATE deployment_admission_receipts SET mode=$2,native_admission_watermark=$3 WHERE admission_id=$1`, id, mode, raw)
		require.NoError(t, err)
		if mode != DeploymentModeDeploy {
			_, err = db.ExecContext(f.ctx, `UPDATE deployment_admission_receipts SET native_admission_watermark=NULL WHERE admission_id=$1`, id)
			require.NoError(t, err)
		}
		_, err = provider.DownTo(f.ctx, 54)
		require.ErrorContains(t, err, "cannot discard retained Native admission predecessor or Plan evidence")
	}
	_, err = db.ExecContext(f.ctx, `UPDATE deployment_admission_receipts SET mode='deploy',native_admission_watermark=NULL`)
	require.NoError(t, err)
	_, err = provider.DownTo(f.ctx, 54)
	require.NoError(t, err)
	_, err = provider.Up(f.ctx)
	require.NoError(t, err)
}
