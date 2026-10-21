package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func (s *Server) retainLateRunnerResult(ctx context.Context, previous model.DeploymentSummary, runnerID string, envelope hmessaging.EventEnvelope, digest string, body DeploymentResultsUpdateBody) error {
	invalid := func(reason string) error { return fmt.Errorf("%w: %s", ErrRunnerEventTargetInvalid, reason) }
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := model.LockEnvironmentMutation(ctx, tx, previous.DeploymentEnvUuid); err != nil {
		return err
	}
	if envelope.EventID != previous.Id.String()+":deployment-result" {
		return invalid("late completion lacks exact gateway Deployment event identity")
	}
	var retainedDigest string
	err = tx.QueryRowContext(ctx, `SELECT payload_sha256 FROM deployment_late_runner_results WHERE deployment_id=$1 AND org_id=$2 AND runner_id=$3 AND event_id=$4`, previous.Id, previous.OrgId, runnerID, envelope.EventID).Scan(&retainedDigest)
	if err == nil {
		if retainedDigest != digest {
			return invalid("late completion conflicts with an immutable retained result")
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var alreadyApplied bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runner_event_inbox WHERE event_id=$1 AND org_id=$2 AND runner_id=$3
        AND deployment_id=$4 AND event_type='deployment-result')`, envelope.EventID, previous.OrgId, runnerID, previous.Id).Scan(&alreadyApplied); err != nil {
		return err
	}
	if alreadyApplied {
		return tx.Commit()
	}
	var head string
	if err := tx.QueryRowContext(ctx, `SELECT last_state_deployment_id::text FROM deployment_environments
        WHERE id=$1 AND org_id=$2 AND project_id=$3 AND env_id=$4 FOR UPDATE`, previous.DeploymentEnvUuid, previous.OrgId, previous.ProjectId, previous.EnvId).Scan(&head); err != nil {
		return err
	}
	dep, _, _, _, err := s.Database.GetDeployment(ctx, tx, previous.OrgId, previous.Id, model.GetModeForUpdate)
	if err != nil {
		return err
	}
	if dep.RunnerId != runnerID || dep.Status != model.DeploymentStatusFailed || !dep.CompletedAt.IsSet() || head != dep.Id.String() ||
		envelope.CreatedAt.Before(dep.CreatedAt) || !body.Status.Valid() {
		return invalid("late completion does not belong to the current failed Runner attempt")
	}
	budget, err := model.ReadDeploymentDispatchBudget(ctx, tx, dep.Id)
	if err != nil {
		return err
	}
	if !budget.FirstAttemptAt.Valid || budget.Revision+1 != dep.Revision || envelope.CreatedAt.Before(budget.FirstAttemptAt.Time) {
		return invalid("late completion does not match the durable dispatch attempt")
	}
	if envelope.CommandID != "" && envelope.CommandID != fmt.Sprintf("create-job:%s:%d", dep.Id, budget.Revision) {
		return invalid("late completion command does not match durable dispatch revision")
	}
	var foreignLease bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM environment_deployment_leases WHERE environment_id=$1
        AND (holder_id<>$2 OR deployment_id IS DISTINCT FROM $2) FOR UPDATE)`, dep.DeploymentEnvUuid, dep.Id).Scan(&foreignLease); err != nil {
		return err
	}
	if foreignLease {
		return invalid("late completion conflicts with another Environment lease")
	}
	// The immutable operation receipt and first-base anchor bind scope, fence,
	// and retained recovery target. No caller can supply a replacement lineage.
	var owned bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (
        SELECT 1 FROM external_operation_deployments r
        JOIN external_operation_rollback_anchors a USING (org_id,plugin_id,operation_ref,environment_uuid)
        JOIN deployments b ON b.id=a.rollback_base_deployment_id AND b.de_id=$3
        WHERE r.deployment_id=$1 AND r.org_id=$2 AND r.environment_uuid=$3
          AND r.project_id=$4 AND r.env_id=$5 AND r.project_uuid=a.project_uuid
          AND a.project_id=r.project_id AND a.env_id=r.env_id AND r.fence>0
          AND ((r.service_action='external-deployment.admit' AND (r.forward_deployment_id=$1 OR (r.forward_deployment_id IS NULL AND a.first_forward_deployment_id=$1)))
               OR (r.service_action='external-deployment.rollback' AND r.forward_deployment_id IS NOT NULL))
          AND b.status='succeeded' AND b.completed_at IS NOT NULL
          AND b.manifest IS NOT NULL AND octet_length(b.manifest)>0 AND b.graph IS NOT NULL AND octet_length(b.graph)>0 AND b.tofu IS NOT NULL AND octet_length(b.tofu)>0
        FOR SHARE OF r,a,b)`, dep.Id, dep.OrgId, dep.DeploymentEnvUuid, dep.ProjectId, dep.EnvId).Scan(&owned); err != nil {
		return err
	}
	if !owned {
		return invalid("late completion has no exact retained operation receipt and rollback anchor")
	}
	var baseID uuid.UUID
	if err := tx.QueryRowContext(ctx, `SELECT a.rollback_base_deployment_id
        FROM external_operation_deployments r
        JOIN external_operation_rollback_anchors a USING (org_id,plugin_id,operation_ref,environment_uuid)
        WHERE r.deployment_id=$1`, dep.Id).Scan(&baseID); err != nil {
		return err
	}
	base, manifest, tofu, graph, err := s.Database.GetDeployment(ctx, tx, dep.OrgId, baseID, model.GetModeDefault)
	if err != nil {
		return err
	}
	if base.DeploymentEnvUuid != dep.DeploymentEnvUuid || base.ProjectId != dep.ProjectId || base.EnvId != dep.EnvId ||
		base.Status != model.DeploymentStatusSucceeded || !base.CompletedAt.IsSet() || !model.CompleteDeploymentSnapshot(manifest, tofu, graph) {
		return invalid("late completion has no complete captured successful rollback base")
	}
	if body.Outputs != nil {
		if len(*body.Outputs) > maxExternalMaterialEncodedBytes {
			return invalid("late encrypted outputs exceed material retention limit")
		}
		decoded, err := base64.StdEncoding.DecodeString(*body.Outputs)
		if err != nil || !bytes.HasPrefix(decoded, []byte("age-encryption.org/v1\n")) {
			return invalid("late outputs are not age-encrypted material")
		}
		if !dep.EncryptedOutputsRecipient.IsSet() {
			return invalid("late outputs have no retained recipient")
		}
		var materialOwned bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM external_operation_deployments r
            JOIN external_operation_material m ON m.org_id=r.org_id AND m.plugin_id=r.plugin_id AND m.operation_ref::text=r.operation_ref
            WHERE r.deployment_id=$1 AND m.recipient=$2 AND m.encrypted_identity ~ '^vault:v[1-9][0-9]*:.+$' FOR SHARE OF r,m)`, dep.Id, dep.EncryptedOutputsRecipient.Must()).Scan(&materialOwned); err != nil {
			return err
		}
		if !materialOwned {
			return invalid("late outputs are not owned by the retained operation recipient")
		}
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO deployment_late_runner_results
        (deployment_id,event_id,org_id,runner_id,payload_sha256,result_status,occurred_at,encrypted_outputs,recipient)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING`, dep.Id, envelope.EventID, dep.OrgId, runnerID, digest, string(body.Status), envelope.CreatedAt, body.Outputs, dep.EncryptedOutputsRecipient.Ref())
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		var exact bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM deployment_late_runner_results
            WHERE deployment_id=$1 AND event_id=$2 AND org_id=$3 AND runner_id=$4 AND payload_sha256=$5
              AND result_status=$6 AND occurred_at=$7 AND encrypted_outputs IS NOT DISTINCT FROM $8::bytea AND recipient IS NOT DISTINCT FROM $9)`,
			dep.Id, envelope.EventID, dep.OrgId, runnerID, digest, string(body.Status), envelope.CreatedAt, body.Outputs, dep.EncryptedOutputsRecipient.Ref()).Scan(&exact); err != nil {
			return err
		}
		if !exact {
			return invalid("late completion conflicts with an immutable retained result")
		}
	}
	if err := model.RecordDeploymentCompletion(ctx, tx, dep.Id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM environment_deployment_leases WHERE deployment_id=$1 AND holder_id=$1`, dep.Id); err != nil {
		return err
	}
	return tx.Commit()
}

// Only successful late ciphertext can be read, under the same scoped material
// checks as an ordinary successful result. Failed status is never rewritten.
func lateSuccessfulOutputs(ctx context.Context, tx model.Tx, dep model.DeploymentSummary) (bool, error) {
	var found bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM deployment_late_runner_results
        WHERE deployment_id=$1 AND org_id=$2 AND runner_id=$3 AND result_status='success'
          AND encrypted_outputs IS NOT NULL AND recipient=$4)`, dep.Id, dep.OrgId, dep.RunnerId, dep.EncryptedOutputsRecipient.Ref()).Scan(&found)
	return found, err
}
