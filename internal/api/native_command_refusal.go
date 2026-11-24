package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

// A delivery refusal is historical transport evidence, never a stopped execution.
func (s *Server) ApplyNativeCommandRefusal(ctx context.Context, delivery hmessaging.Delivery) error {
	var envelope hmessaging.CommandEnvelope
	if len(delivery.Data) == 0 || len(delivery.Data) > 4<<20 || json.Unmarshal(delivery.Data, &envelope) != nil || envelope.Type != "execute-rollout-v1" {
		return nil
	}
	id, err := uuid.Parse(envelope.DeploymentID)
	if err != nil || id == uuid.Nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, RequestHandlerTimeout)
	defer cancel()
	initial, _, _, _, err := s.Database.GetDeployment(ctx, nil, envelope.OrganizationID, id, model.GetModeDefault)
	if _, missing := model.IsErrNotFound(err); missing {
		return nil
	}
	if err != nil {
		return err
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err = model.LockEnvironmentMutation(ctx, tx, initial.DeploymentEnvUuid); err != nil {
		return err
	}
	dep, _, _, _, err := s.Database.GetDeployment(ctx, tx, initial.OrgId, id, model.GetModeForUpdate)
	if _, missing := model.IsErrNotFound(err); missing {
		return nil
	}
	if err != nil {
		return err
	}
	matched, err := retainedNativeCommandRefusal(ctx, tx, dep, delivery)
	if err != nil || !matched {
		return err
	}
	digest := sha256.Sum256(delivery.Data)
	inserted, err := s.Database.TryRecordRunnerEvent(ctx, tx, hmessaging.NativeMessageIDPrefix+"delivery-refusal:"+hex.EncodeToString(digest[:]), dep.OrgId, dep.RunnerId, dep.Id, "command-delivery-refused")
	if err != nil {
		return err
	}
	if inserted {
		reason := delivery.Header.Get("Po-Dead-Letter-Reason")
		if !slices.Contains(strings.Fields("signature_refused command_expired catalog_refused steps_refused capability_refused environment_busy request_scope_refused command_scope_refused command_content_conflict invalid_command"), reason) {
			reason = "import_refused"
		}
		message := fmt.Sprintf("A worker refused a delivery attempt (%s). Execution may still be running. Inspect the Runner and execution before trying again.", reason)
		_, err = tx.ExecContext(ctx, `UPDATE deployments d SET status_message=$2
 WHERE d.id=$1 AND d.status='executing' AND d.status_message IN ('','Deploying...')
 AND EXISTS(SELECT 1 FROM deployment_dispatch_budgets b WHERE b.deployment_id=d.id AND b.deployment_revision=d.revision AND b.first_attempt_at IS NOT NULL AND b.completion_received_at IS NULL)`, dep.Id, message)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func retainedNativeCommandRefusal(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary, delivery hmessaging.Delivery) (bool, error) {
	if dep.NativeTarget == nil {
		return false, nil
	}
	var frozen []byte
	err := tx.QueryRowContext(ctx, `SELECT CASE WHEN d.mode='observation' THEN b.observation_command ELSE p.command END
 FROM deployments d JOIN deployment_dispatch_budgets b ON b.deployment_id=d.id
 LEFT JOIN deployment_pipeline_runs p ON p.deployment_id=d.id
 WHERE d.id=$1 AND ((d.mode='observation' AND EXISTS(SELECT 1 FROM environment_observation_receipts o WHERE o.deployment_id=d.id AND o.org_id=$2 AND o.environment_uuid=d.de_id AND o.grant_revision>0 AND o.receipt->>'run_id'=d.id::text AND o.receipt->>'disposition'='accepted'))
 OR (d.mode<>'observation' AND EXISTS(SELECT 1 FROM deployment_admission_receipts a WHERE a.deployment_id=d.id AND a.org_id=$2 AND a.environment_uuid=d.de_id AND a.disposition='committed' AND a.mode=d.mode AND a.native_target=d.native_target)))`, dep.Id, dep.OrgId).Scan(&frozen)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(frozen) == 0 || !bytes.Equal(frozen, delivery.Data) {
		return false, nil
	}
	if _, err = model.FrozenPipelinePayload(frozen, dep); err != nil {
		return false, nil
	}
	if dep.Mode == model.DeploymentModeObservation && validateFrozenObservationCommand(frozen, dep) != nil {
		return false, nil
	}
	route, err := hmessaging.NativeRunnerToken(dep.NativeTarget.RunnerUuid.String())
	if err != nil {
		return false, nil
	}
	subject, err := hmessaging.RunnerCommandSubject(dep.OrgId, route)
	if err != nil {
		return false, nil
	}
	dlq, err := hmessaging.DeadLetterSubject(subject)
	return err == nil && delivery.Subject == dlq, nil
}
