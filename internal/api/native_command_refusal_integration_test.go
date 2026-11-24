//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

// Existing real PG/NATS fixtures supply retained commands, not Core admission proof.
func nativeRefusalDispatch(t *testing.T, observation bool) *nativeDispatchFixture {
	t.Helper()
	factory := newNativeDispatchFixture
	if observation {
		factory = newNativeObservationDispatchFixture
	}
	f := factory(t)
	if observation {
		f.exec(`INSERT INTO environment_observation_receipts(org_id,plugin_id,request_id,request_sha256,environment_uuid,actor_id,grant_id,grant_revision,receipt,deployment_id)
 SELECT e.org_id,'dev.example.diagnostic',$2,repeat('a',64),e.id,d.created_by,$3,1,jsonb_build_object('run_id',d.id,'disposition','accepted'),d.id FROM deployments d JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1`, f.dep.Id, uuid.New(), uuid.New())
		require.NoError(t, f.dispatcher.DispatchObservation(f.ctx, f.dep))
	} else {
		f.exec(`INSERT INTO deployment_admission_receipts(org_id,admission_id,project_uuid,environment_uuid,actor_id,request_digest,mode,module_versions,carry_forward_module_versions,confirmed_restricted_version_uuids,core_epoch,disposition,deployment_id,draft_test_snapshots,native_target)
 SELECT e.org_id,$2,$3,e.id,d.created_by,repeat('a',64),d.mode,'{}','{}','[]',1,'committed',d.id,'{}',d.native_target FROM deployments d JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1`, f.dep.Id, uuid.New(), uuid.New())
		require.NoError(t, f.dispatcher.Dispatch(f.ctx, f.dep))
	}
	return f
}

func nativeRefusalDelivery(t *testing.T, f *nativeDispatchFixture) hmessaging.Delivery {
	t.Helper()
	subject, err := hmessaging.DeadLetterSubject(f.subject)
	require.NoError(t, err)
	_, err = f.broker.PublishMsg(&nats.Msg{Subject: subject, Data: f.last().Data, Header: nats.Header{"Po-Dead-Letter-Reason": {"capability_refused"}}}, nats.MsgId(uuid.NewString()))
	require.NoError(t, err)
	raw, err := f.broker.GetLastMsg(hmessaging.DeadLettersStreamName, subject)
	require.NoError(t, err)
	return hmessaging.Delivery{Message: hmessaging.Message{ID: raw.Header.Get(nats.MsgIdHdr), Subject: raw.Subject, Data: raw.Data, Header: hmessaging.Header(raw.Header), CreatedAt: raw.Time}, Attempt: 1}
}

func nativeRefusalDomain(t *testing.T, f *nativeDispatchFixture) string {
	t.Helper()
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var snapshot string
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d)-'status_message','environment',to_jsonb(e),
 'pipeline',(SELECT to_jsonb(p) FROM deployment_pipeline_runs p WHERE p.deployment_id=d.id),'budget',(SELECT to_jsonb(b) FROM deployment_dispatch_budgets b WHERE b.deployment_id=d.id),
 'reservation',(SELECT to_jsonb(r) FROM runner_job_reservations r WHERE r.deployment_id=d.id),'lease',(SELECT to_jsonb(l) FROM environment_deployment_leases l WHERE l.deployment_id=d.id),
 'admission',(SELECT to_jsonb(a) FROM deployment_admission_receipts a WHERE a.deployment_id=d.id),'observation',(SELECT to_jsonb(o) FROM environment_observation_receipts o WHERE o.deployment_id=d.id),
 'effective',(SELECT to_jsonb(s) FROM deployment_effective_state s WHERE s.environment_id=e.id),'outbox',(SELECT count(*) FROM pending_event_messages))::text
 FROM deployments d JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1`, f.dep.Id).Scan(&snapshot))
	return snapshot
}

func TestNativeCommandRefusalRecordsOnlyHistoricalDiagnostic(t *testing.T) {
	for _, observation := range []bool{false, true} {
		t.Run(map[bool]string{false: "pipeline", true: "observation"}[observation], func(t *testing.T) {
			f := nativeRefusalDispatch(t, observation)
			delivery := nativeRefusalDelivery(t, f)
			before, command := nativeRefusalDomain(t, f), f.last()
			require.NoError(t, f.server.ApplyNativeCommandRefusal(f.ctx, delivery))
			delivery.ID = uuid.NewString()
			delivery.Header.Set("Po-Dead-Letter-Reason", "PRIVATE_REJECTION_VALUE")
			require.NoError(t, f.server.ApplyNativeCommandRefusal(f.ctx, delivery))
			require.Equal(t, before, nativeRefusalDomain(t, f))
			require.Equal(t, command.Sequence, f.last().Sequence, "diagnostics publish no extra execution command")
			stored, _, _, _, err := f.server.Database.GetDeployment(f.ctx, nil, f.dep.OrgId, f.dep.Id, model.GetModeDefault)
			require.NoError(t, err)
			require.Contains(t, stored.StatusMessage, "capability_refused")
			require.NotContains(t, stored.StatusMessage, "PRIVATE_REJECTION_VALUE")
			require.LessOrEqual(t, len(stored.StatusMessage), 512)
			tx, err := f.server.Database.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			var diagnostic, stopped int
			require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FILTER(WHERE event_type='command-delivery-refused'),count(*) FILTER(WHERE event_type='deployment-result') FROM runner_event_inbox WHERE deployment_id=$1`, f.dep.Id).Scan(&diagnostic, &stopped))
			require.Equal(t, 1, diagnostic)
			require.Zero(t, stopped)
			access := newQueueFixture(t, DeploymentQueueConfig{})
			access.server.Database = f.server.Database
			reader, denied := uuid.New(), uuid.New()
			access.iam.onlyDeploymentRead(reader)
			access.iam.deny(denied, true)
			path := "/orgs/" + f.dep.OrgId + "/deployments/" + f.dep.Id.String()
			visible := access.call(http.MethodGet, path, nil, reader, "")
			require.Equal(t, http.StatusOK, visible.Code, visible.Body.String())
			require.Contains(t, visible.Body.String(), stored.StatusMessage)
			require.NotContains(t, visible.Body.String(), "PRIVATE_REJECTION_VALUE")
			require.Equal(t, http.StatusForbidden, access.call(http.MethodGet, path, nil, denied, "").Code)
			require.Equal(t, http.StatusNotFound, access.call(http.MethodGet, "/orgs/foreign/deployments/"+f.dep.Id.String(), nil, reader, "").Code)
		})
	}
}

func TestNativeCommandRefusalRejectsForeignOrUnretainedCommands(t *testing.T) {
	for _, trial := range []string{"malformed", "changed-bytes", "foreign-org", "foreign-site", "missing-command", "missing-admission"} {
		t.Run(trial, func(t *testing.T) {
			f := nativeRefusalDispatch(t, false)
			delivery := nativeRefusalDelivery(t, f)
			switch trial {
			case "malformed":
				delivery.Data = []byte(`{"deployment_id":`)
			case "changed-bytes":
				delivery.Data = append(delivery.Data, ' ')
			case "foreign-org":
				var envelope hmessaging.CommandEnvelope
				require.NoError(t, json.Unmarshal(delivery.Data, &envelope))
				envelope.OrganizationID = "another-org"
				delivery.Data, _ = json.Marshal(envelope)
			case "foreign-site":
				delivery.Subject = strings.Replace(delivery.Subject, ".runners.", ".runners.foreign.", 1)
			case "missing-command":
				f.exec(`UPDATE deployment_pipeline_runs SET command=NULL WHERE deployment_id=$1`, f.dep.Id)
			case "missing-admission":
				f.exec(`DELETE FROM deployment_admission_receipts WHERE deployment_id=$1`, f.dep.Id)
			}
			before := nativeRefusalDomain(t, f)
			require.NoError(t, f.server.ApplyNativeCommandRefusal(f.ctx, delivery))
			require.Equal(t, before, nativeRefusalDomain(t, f))
			stored, _, _, _, err := f.server.Database.GetDeployment(f.ctx, nil, f.dep.OrgId, f.dep.Id, model.GetModeDefault)
			require.NoError(t, err)
			require.Equal(t, "Deploying...", stored.StatusMessage)
			tx, err := f.server.Database.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			var recorded int
			require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM runner_event_inbox WHERE deployment_id=$1`, f.dep.Id).Scan(&recorded))
			require.Zero(t, recorded)
		})
	}
}

func TestNativeCommandRefusalPreservesActiveAndNewerOutcomes(t *testing.T) {
	for _, trial := range []string{"active-effect", "newer-message", "completed", "untrusted-reason"} {
		t.Run(trial, func(t *testing.T) {
			f := nativeRefusalDispatch(t, false)
			delivery := nativeRefusalDelivery(t, f)
			message := "Operator is already investigating"
			switch trial {
			case "active-effect":
				f.exec(`UPDATE deployment_pipeline_runs SET no_effect_blocked=true,progress=jsonb_set(progress,'{phase}','"applying"') WHERE deployment_id=$1`, f.dep.Id)
			case "newer-message":
				f.exec(`UPDATE deployments SET status_message=$2 WHERE id=$1`, f.dep.Id, message)
			case "completed":
				f.exec(`UPDATE deployments SET status='succeeded',status_message=$2,completed_at=now(),revision=revision+1 WHERE id=$1`, f.dep.Id, message)
			case "untrusted-reason":
				delivery.Header.Set("Po-Dead-Letter-Reason", "PRIVATE_REJECTION_VALUE<script>")
			}
			before := nativeRefusalDomain(t, f)
			require.NoError(t, f.server.ApplyNativeCommandRefusal(f.ctx, delivery))
			require.Equal(t, before, nativeRefusalDomain(t, f))
			stored, _, _, _, err := f.server.Database.GetDeployment(f.ctx, nil, f.dep.OrgId, f.dep.Id, model.GetModeDefault)
			require.NoError(t, err)
			require.NotContains(t, stored.StatusMessage, "PRIVATE_REJECTION_VALUE")
			if trial == "newer-message" || trial == "completed" {
				require.Equal(t, message, stored.StatusMessage)
			}
		})
	}
}
