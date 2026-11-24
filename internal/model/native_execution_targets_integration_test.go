//go:build integration

package model

import (
	"encoding/json"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/require"
)

func TestNativeTargetPersistsAcrossEveryDeploymentReaderAndReceipt(t *testing.T) {
	f := newQueueModelFixture(t)
	target := testNativeTarget()
	dep, err := f.create(t, DeploymentModeDeploy, func(params *CreateDeploymentParams) {
		params.NativeTarget = target
		params.RunnerId = target.RunnerId
		params.IdempotencyKeyDigest = opt.Of("native-key")
	})
	require.NoError(t, err)
	require.Equal(t, target, dep.NativeTarget)
	updated, err := f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, dep.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	require.Equal(t, target, updated.NativeTarget)
	got, _, _, _, err := f.store.GetDeployment(f.ctx, nil, "queue-org", dep.Id, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, target, got.NativeTarget)
	got, _, _, _, err = f.store.GetLastDeployment(f.ctx, nil, "queue-org", "p", "e", GetLastDeploymentParams{})
	require.NoError(t, err)
	require.Equal(t, target, got.NativeTarget)
	got, _, err = f.store.GetDeploymentByIdempotencyKeyDigest(f.ctx, nil, "queue-org", "p", "e", "native-key")
	require.NoError(t, err)
	require.Equal(t, target, got.NativeTarget)
	rows, _, err := f.store.ListDeployments(f.ctx, nil, "queue-org", "", 10, ListDeploymentsParams{})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, target, rows[0].NativeTarget)
	rows, _, err = f.store.ListLastDeployments(f.ctx, nil, "queue-org", "", 10, ListLastDeploymentsParams{})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, target, rows[0].NativeTarget)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `INSERT INTO resource_nodes(hash,env_uuid,resource_type,resource_class,resource_id,last_deployment_id,last_module_definition_id,last_module_definition_version) VALUES('node',$1,'db','default','main',$2,'postgres','v1')`, f.env, dep.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	rows, _, err = f.store.ListLastDeploymentsByNodeProperties(f.ctx, nil, "queue-org", "", 10, ListLastDeploymentsByNodePropertiesParams{})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, target, rows[0].NativeTarget)
	intent := testDeploymentAdmissionIntent()
	intent.OrgID = "queue-org"
	intent.NativeTarget = target
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	pending, err := PrepareDeploymentAdmissionReceipt(f.ctx, tx, intent)
	require.NoError(t, err)
	require.Equal(t, target, pending.NativeTarget)
	require.NoError(t, tx.Commit())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	restored, err := GetDeploymentAdmissionReceipt(f.ctx, tx, intent.OrgID, intent.AdmissionID)
	require.NoError(t, err)
	require.Equal(t, target, restored.NativeTarget)
	unsettled, err := ListUnsettledDeploymentAdmissionReceipts(f.ctx, tx, 10, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, unsettled, 1)
	require.Equal(t, target, unsettled[0].NativeTarget)
	bound, err := BindDeploymentAdmissionReceipt(f.ctx, tx, intent, dep.Id, 1)
	require.NoError(t, err)
	require.Equal(t, target, bound.NativeTarget)
	restored, err = GetDeploymentAdmissionReceiptByDeployment(f.ctx, tx, dep.Id)
	require.NoError(t, err)
	require.Equal(t, target, restored.NativeTarget)
	require.NoError(t, tx.Commit())
	changed := intent
	copyTarget := *target
	copyTarget.RunnerUuid = uuid.New()
	changed.NativeTarget = &copyTarget
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = PrepareDeploymentAdmissionReceipt(f.ctx, tx, changed)
	_, conflict := IsErrConflict(err)
	require.True(t, conflict)
	require.NoError(t, tx.Rollback())
	request := observationRequest(f)
	observed, observationEvents, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), request, target.RunnerId, 2, true, target)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, observed.Disposition)
	require.Equal(t, &dep.Id, observed.BaselineId)
	require.NotNil(t, observed.RunId)
	require.NotNil(t, observed.BaselineFingerprint)
	require.Len(t, observationEvents, 1)
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
	observationDeployment, manifest, observationTofu, graph, err := f.store.GetDeployment(f.ctx, nil, "queue-org", *observed.RunId, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, target, observationDeployment.NativeTarget)
	require.False(t, observationDeployment.HasPipeline)
	require.Equal(t, ObservationBaselineFingerprint(manifest, observationTofu, graph, dep.ModuleArtifacts, target), *observed.BaselineFingerprint)
	got, _, tofu, _, err := f.store.GetDeployment(f.ctx, nil, "queue-org", dep.Id, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, target, got.NativeTarget)
	require.Equal(t, RawTofu("terraform {}"), tofu)
}

func TestNativeTargetMigrationPreservesLegacyAndRefusesLossOfEachEvidenceStore(t *testing.T) {
	f := newQueueModelFixture(t)
	dep, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	db := f.store.(*databaser).DB
	source, err := fs.Sub(embedMigrations, "migrations")
	require.NoError(t, err)
	provider, err := goose.NewProvider(goose.DialectPostgres, db, source, goose.WithDisableGlobalRegistry(true), goose.WithGoMigrations(goose.NewGoMigration(2, &goose.GoFunc{RunTx: hstandardoutbox.MigrateUp01}, &goose.GoFunc{RunTx: hstandardoutbox.MigrateDown01})))
	require.NoError(t, err)
	_, err = provider.DownTo(f.ctx, 53)
	require.NoError(t, err)
	// This is the old writer's explicit INSERT column list, without the new field.
	_, err = db.ExecContext(f.ctx, `INSERT INTO deployments(id,de_id,created_at,mode,status,status_message,manifest,graph,tofu,runner_id) VALUES($1,$2,now(),'deploy','succeeded','', $3,$4,$5,'runner')`, uuid.New(), f.env, []byte(`{}`), []byte(`{}`), []byte("terraform {}"))
	require.NoError(t, err)
	_, err = provider.Up(f.ctx)
	require.NoError(t, err)
	var count int
	require.NoError(t, db.QueryRowContext(f.ctx, `SELECT count(*) FROM deployments WHERE native_target IS NULL`).Scan(&count))
	require.Equal(t, 2, count)
	target := testNativeTarget()
	raw, err := json.Marshal(target)
	require.NoError(t, err)
	for _, invalid := range []string{"null", "[]", "1", "true"} {
		_, err = db.ExecContext(f.ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, dep.Id, invalid)
		require.Error(t, err)
	}
	refuse := func() {
		t.Helper()
		_, err := provider.DownTo(f.ctx, 53)
		require.ErrorContains(t, err, "cannot discard immutable Native execution target evidence")
		_, err = provider.Up(f.ctx)
		require.NoError(t, err)
	}
	_, err = db.ExecContext(f.ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, dep.Id, raw)
	require.NoError(t, err)
	refuse()
	_, err = db.ExecContext(f.ctx, `UPDATE deployments SET native_target=NULL WHERE id=$1`, dep.Id)
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	intent := testDeploymentAdmissionIntent()
	intent.NativeTarget = target
	_, err = PrepareDeploymentAdmissionReceipt(f.ctx, tx, intent)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	refuse()
	_, err = db.ExecContext(f.ctx, `UPDATE deployment_admission_receipts SET native_target=NULL`)
	require.NoError(t, err)
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	snapshot, err := CreateOrGetExternalDeploymentPlanSnapshot(
		f.ctx,
		tx,
		ExternalDeploymentPlanSnapshot{
			NativeTarget:      target,
			OrgID:             "queue-org",
			PluginID:          "dev.example.plugin",
			RequestID:         "target-plan",
			ActorID:           uuid.New(),
			RequestBodySHA256: strings.Repeat("a", 64),
			ProjectID:         "p",
			ProjectUUID:       uuid.New(),
			EnvID:             "e",
			EnvironmentUUID:   f.env,
			BaseDeploymentID:  dep.Id,
			Fingerprint:       "sha256:" + strings.Repeat("b", 64),
			Manifest:          json.RawMessage(`{}`),
			Graph:             json.RawMessage(`{}`),
			Diff:              json.RawMessage(`{}`),
		},
	)
	require.NoError(t, err)
	require.Equal(t, target, snapshot.NativeTarget)
	require.NoError(t, tx.Commit())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	stored, err := GetExternalDeploymentPlanSnapshotByID(f.ctx, tx, "queue-org", "dev.example.plugin", snapshot.ID)
	require.NoError(t, err)
	require.Equal(t, target, stored.NativeTarget)
	require.NoError(t, tx.Commit())
	refuse()
	_, err = db.ExecContext(f.ctx, `UPDATE external_deployment_plan_snapshots SET native_target=NULL`)
	require.NoError(t, err)
	item, _, err := f.enqueue(t, func(n *NewDeploymentQueueItem) { n.Intent = json.RawMessage(`{"native_target":` + string(raw) + `}`) })
	require.NoError(t, err)
	refuse()
	_, err = db.ExecContext(f.ctx, `UPDATE environment_deployment_queue SET intent='{}' WHERE id=$1`, item.ID)
	require.NoError(t, err)
	_, err = db.ExecContext(f.ctx, `INSERT INTO deployment_pipeline_runs(deployment_id,backend,definition,progress,plan_capture) VALUES($1,'native','{}','{}',jsonb_build_object('native_target',$2::jsonb))`, dep.Id, raw)
	require.NoError(t, err)
	refuse()
	_, err = db.ExecContext(f.ctx, `UPDATE deployment_pipeline_runs SET plan_capture='{"native_target":null}' WHERE deployment_id=$1`, dep.Id)
	require.NoError(t, err)
	_, err = provider.DownTo(f.ctx, 53)
	require.NoError(t, err)
	_, err = provider.Up(f.ctx)
	require.NoError(t, err)
}
