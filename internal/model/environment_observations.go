package model

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/genevents"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
)

const ObservationResultRetention = 30 * 24 * time.Hour
const ObservationResultUnavailableMessage = "observation result unavailable"
const observationReplacementActionCount = 2

// RequireObservationStopped fences all creation paths after an observation
// watchdog failure. A clock deadline is not proof of Runner termination.
func RequireObservationStopped(ctx context.Context, tx Tx, environmentID uuid.UUID) error {
	var held bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM environment_observation_receipts o JOIN runner_job_reservations r ON r.deployment_id=o.deployment_id JOIN deployments d ON d.id=o.deployment_id WHERE o.environment_uuid=$1 AND d.status='failed' AND r.released_at IS NULL)`, environmentID).
		Scan(&held)
	if err != nil {
		return err
	}
	if held {
		return NewErrCodedConflict(CodeOutcomeUnreconciled, "Environment observation may still own its state lock")
	}
	return nil
}

func ReadObservationReceipt(ctx context.Context, tx Tx, orgID, pluginID string, requestID uuid.UUID) (*observation.ObservationReceipt, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT receipt FROM environment_observation_receipts WHERE org_id=$1 AND plugin_id=$2 AND request_id=$3`, orgID, pluginID, requestID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewErrNotFound("observation request not found")
	}
	if err != nil {
		return nil, err
	}
	var receipt observation.ObservationReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return nil, err
	}
	return &receipt, nil
}

func TryStartObservation(ctx context.Context, db Databaser, orgID, pluginID string, actorID uuid.UUID, request observation.ObservationRequest, runnerID string, limit int, supported bool) (*observation.ObservationReceipt, []*hstandardoutbox.PendingEventMessage, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, nil, err
	}
	sum := sha256.Sum256(encoded)
	digest := hex.EncodeToString(sum[:])
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := LockObservationRequest(ctx, tx, ObservationRequestKindObservation, orgID, pluginID, request.RequestId); err != nil {
		return nil, nil, err
	}
	if err := RequireObservationRequestUnsealed(ctx, tx, ObservationRequestKindObservation, orgID, pluginID, request.RequestId, actorID, uuid.Nil, digest); err != nil {
		return nil, nil, err
	}
	var previousDigest string
	var previousActor uuid.UUID
	err = tx.QueryRowContext(ctx, `SELECT request_sha256,actor_id FROM environment_observation_receipts WHERE org_id=$1 AND plugin_id=$2 AND request_id=$3`, orgID, pluginID, request.RequestId).Scan(&previousDigest, &previousActor)
	if err == nil {
		if previousDigest != digest || previousActor != actorID {
			return nil, nil, NewErrConflict("observation request identity was already used with different content")
		}
		receipt, err := ReadObservationReceipt(ctx, tx, orgID, pluginID, request.RequestId)
		if err != nil {
			return nil, nil, err
		}
		return receipt, nil, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, err
	}
	now := time.Now().UTC()
	receipt := &observation.ObservationReceipt{RequestId: request.RequestId, Disposition: observation.ObservationReceiptDispositionSkipped, Reason: observation.ObservationReceiptReasonExpired, RecordedAt: now}
	var pending []*hstandardoutbox.PendingEventMessage
	if now.Before(request.AdmissionDeadline) {
		if err := LockEnvironmentMutation(ctx, tx, request.EnvironmentUuid); err != nil {
			return nil, nil, err
		}
		reason, baseline, manifest, tofu, graph, err := observationBaseline(ctx, db, tx, orgID, request)
		if err != nil {
			return nil, nil, err
		}
		receipt.Reason = reason
		if !time.Now().Before(request.AdmissionDeadline) {
			receipt.Reason = observation.ObservationReceiptReasonExpired
			reason = receipt.Reason
		}
		if reason == observation.ObservationReceiptReasonAccepted {
			if !supported || limit <= 0 {
				receipt.Reason = observation.ObservationReceiptReasonUnsupported
			} else {
				// A savepoint makes a full Runner a sealed skip without retaining a new
				// executing head, lease, deployment, budget, or outbox event.
				if _, err := tx.ExecContext(ctx, `SAVEPOINT observation_capacity`); err != nil {
					return nil, nil, err
				}
				// Runner discards private observation diagnostics. INFO retains aggregate costs and safe lifecycle metadata.
				dep, err := db.CreateDeployment(ctx, tx, orgID, request.ProjectId, request.EnvId, CreateDeploymentParams{
					CreatedBy: actorID, DeploymentEnvUuid: request.EnvironmentUuid, Mode: DeploymentModeObservation,
					Manifest: manifest, Tofu: tofu, Graph: graph, RunnerId: runnerID, RunnerLogLevel: "info",
					ModuleArtifacts: baseline.ModuleArtifacts, ModuleVersionRequestDigest: baseline.ModuleVersionRequestDigest, Metrics: baseline.Metrics,
				})
				if err != nil {
					return nil, nil, err
				}
				if err = ReserveRunnerJob(ctx, tx, dep, limit); errors.Is(err, ErrRunnerCapacityFull) || (err == nil && !time.Now().Before(request.AdmissionDeadline)) {
					if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT observation_capacity`); err != nil {
						return nil, nil, err
					}
					if err == nil {
						receipt.Reason = observation.ObservationReceiptReasonExpired
					} else {
						receipt.Reason = observation.ObservationReceiptReasonRunnerFull
					}
				} else if err != nil {
					return nil, nil, err
				} else {
					receipt.Disposition = observation.ObservationReceiptDispositionAccepted
					receipt.Reason = observation.ObservationReceiptReasonAccepted
					receipt.RunId = &dep.Id
					receipt.BaselineId = &baseline.Id
					value := ObservationBaselineFingerprint(manifest, tofu, graph, baseline.ModuleArtifacts)
					receipt.BaselineFingerprint = &value
					pending, err = db.InsertPendingEventMessages(ctx, tx, []*hstandardoutbox.PendingEventMessage{{Subject: string(genevents.IoPlatformOrchestratorDeploymentCreated), Payload: ConvertDeploymentToEventPayload(dep)}})
					if err != nil {
						return nil, nil, err
					}
				}
			}
		}
	}
	receipt.RecordedAt = time.Now().UTC()
	raw, err := json.Marshal(receipt)
	if err != nil {
		return nil, nil, err
	}
	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO environment_observation_receipts(org_id,plugin_id,request_id,request_sha256,environment_uuid,actor_id,grant_id,grant_revision,receipt,deployment_id,baseline_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		orgID,
		pluginID,
		request.RequestId,
		digest,
		request.EnvironmentUuid,
		actorID,
		request.GrantId,
		request.GrantRevision,
		raw,
		receipt.RunId,
		receipt.BaselineId,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	return receipt, pending, nil
}

func observationBaseline(ctx context.Context, db Databaser, tx Tx, orgID string, request observation.ObservationRequest) (observation.ObservationReceiptReason, *DeploymentSummary, EncodedDeploymentManifest, RawTofu, EncodedDeploymentGraph, error) {
	none := func(reason observation.ObservationReceiptReason) (observation.ObservationReceiptReason, *DeploymentSummary, EncodedDeploymentManifest, RawTofu, EncodedDeploymentGraph, error) {
		return reason, nil, nil, nil, nil, nil
	}
	if err := RequireObservationStopped(ctx, tx, request.EnvironmentUuid); err != nil {
		if _, ok := IsErrConflict(err); ok {
			return none(observation.ObservationReceiptReasonOutcomeUnreconciled)
		}
		return "", nil, nil, nil, nil, err
	}
	var busy bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM environment_deployment_leases WHERE environment_id=$1 AND expires_at>now()) OR EXISTS(SELECT 1 FROM deployments WHERE de_id=$1 AND status='executing') OR EXISTS(SELECT 1 FROM environment_deployment_queue WHERE environment_uuid=$1 AND state='queued')`, request.EnvironmentUuid).
		Scan(&busy); err != nil {
		return "", nil, nil, nil, nil, err
	}
	if busy {
		return none(observation.ObservationReceiptReasonBusy)
	}
	// Legacy ordinary replacement remains allowed, so an older failed attempt
	// can still own a Runner slot after a newer desired-state head succeeded.
	var uncertainOwner bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runner_job_reservations reservation JOIN deployments attempt ON attempt.id=reservation.deployment_id WHERE attempt.de_id=$1 AND attempt.status='failed' AND reservation.released_at IS NULL)`, request.EnvironmentUuid).
		Scan(&uncertainOwner); err != nil {
		return "", nil, nil, nil, nil, err
	}
	if uncertainOwner {
		return none(observation.ObservationReceiptReasonOutcomeUnreconciled)
	}
	head, _, _, _, err := db.GetLastDeployment(ctx, tx, orgID, request.ProjectId, request.EnvId, GetLastDeploymentParams{StateChangeOnly: true})
	if _, missing := IsErrNotFound(err); missing {
		return none(observation.ObservationReceiptReasonNoBaseline)
	}
	if err != nil {
		return "", nil, nil, nil, nil, err
	}
	base, manifest, tofu, graph, err := db.GetDeployment(ctx, tx, orgID, head.Id, GetModeDefault)
	if err != nil {
		return "", nil, nil, nil, nil, err
	}
	if base.DeploymentEnvUuid != request.EnvironmentUuid {
		return none(observation.ObservationReceiptReasonNoBaseline)
	}
	if base.Status != DeploymentStatusSucceeded || !base.CompletedAt.IsSet() || (base.Mode != DeploymentModeDeploy && base.Mode != DeploymentModeRollback) {
		unknown, err := DeploymentOutcomeUnreconciled(ctx, tx, base.Id)
		if err != nil {
			return "", nil, nil, nil, nil, err
		}
		if unknown {
			return none(observation.ObservationReceiptReasonOutcomeUnreconciled)
		}
		return none(observation.ObservationReceiptReasonNoBaseline)
	}
	if !CompleteDeploymentSnapshot(manifest, tofu, graph) {
		return none(observation.ObservationReceiptReasonNoBaseline)
	}
	parsed, parseErr := graphs.FromJson(bytes.NewReader(graph))
	if parseErr != nil {
		return none(observation.ObservationReceiptReasonNoBaseline)
	}
	for _, node := range parsed.Nodes {
		config := node.ModuleConfiguration
		if config == nil || config.Deleted {
			continue
		}
		if config.DraftTest != nil {
			return none(observation.ObservationReceiptReasonNoBaseline)
		}
		retained := false
		for _, artifact := range base.ModuleArtifacts {
			if artifact.ModuleID == config.DefinitionId && artifact.Version == config.VersionId {
				retained = artifact.HasObservationProvenance() && config.HasInlineSource == (artifact.Source == InlineModuleSource)
				break
			}
		}
		if !retained {
			return none(observation.ObservationReceiptReasonNoBaseline)
		}
	}
	for _, artifact := range base.ModuleArtifacts {
		if !artifact.HasObservationProvenance() {
			return none(observation.ObservationReceiptReasonNoBaseline)
		}
	}
	return observation.ObservationReceiptReasonAccepted, base, manifest, tofu, graph, nil
}

func ReadObservationRun(ctx context.Context, tx Tx, orgID, pluginID string, runID uuid.UUID) (*observation.ObservationRun, error) {
	var receiptRaw, resultRaw []byte
	var status, statusMessage string
	var completed, expires, currentCompleted sql.NullTime
	var currentID uuid.NullUUID
	var currentStatus, currentMode sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT o.receipt,COALESCE(d.status,'expired'),COALESCE(d.status_message,''),d.completed_at,o.result,o.result_expires_at,current.id,current.status,current.mode,current.completed_at FROM environment_observation_receipts o LEFT JOIN deployments d ON d.id=o.deployment_id LEFT JOIN deployment_environments e ON e.id=o.environment_uuid AND e.org_id=o.org_id LEFT JOIN deployments current ON current.id=e.last_state_deployment_id WHERE o.org_id=$1 AND o.plugin_id=$2 AND o.receipt ? 'run_id' AND o.receipt->>'run_id'=$3::text`, orgID, pluginID, runID).
		Scan(&receiptRaw, &status, &statusMessage, &completed, &resultRaw, &expires, &currentID, &currentStatus, &currentMode, &currentCompleted)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewErrNotFound("observation run not found")
	}
	if err != nil {
		return nil, err
	}
	var out observation.ObservationRun
	if err := json.Unmarshal(receiptRaw, &out.Receipt); err != nil {
		return nil, err
	}
	out.Status = observation.ObservationRunStatus(status)
	if status == string(DeploymentStatusFailed) && statusMessage == ObservationResultUnavailableMessage {
		out.Status = observation.ObservationRunStatusUnavailable
	}
	if completed.Valid {
		out.CompletedAt = &completed.Time
	}
	if status == string(DeploymentStatusSucceeded) {
		if expires.Valid && !time.Now().Before(expires.Time) {
			out.Status = observation.ObservationRunStatusExpired
		} else if len(resultRaw) == 0 || !expires.Valid {
			out.Status = observation.ObservationRunStatusUnavailable
		} else {
			if err := json.Unmarshal(resultRaw, &out.Result); err != nil {
				return nil, err
			}
		}
	}
	if out.Status == observation.ObservationRunStatusSucceeded {
		if !currentID.Valid || !currentCompleted.Valid || currentStatus.String != string(DeploymentStatusSucceeded) || (currentMode.String != string(DeploymentModeDeploy) && currentMode.String != string(DeploymentModeRollback)) {
			out.Status = observation.ObservationRunStatusUnavailable
		} else if out.Receipt.BaselineId == nil || currentID.UUID != *out.Receipt.BaselineId {
			out.Status = observation.ObservationRunStatusStale
		}
	}
	return &out, nil
}

var observationIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
var observationTypePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,127}$`)
var observationVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][a-zA-Z0-9.-]+)?$`)

func ValidateObservationResult(result observation.ObservationResult) error {
	if string(result.SchemaVersion) != "1" || !bool(result.Complete) || !result.Tool.Valid() || len(result.ToolVersion) > 64 || !observationVersionPattern.MatchString(result.ToolVersion) || result.OutputChangeCount < 0 || result.OutputChangeCount > 1000000 || result.ObservedChanges == nil ||
		result.ProposedChanges == nil ||
		len(result.ObservedChanges) > 2000 ||
		len(result.ProposedChanges) > 2000 {
		return NewErrBadRequest("observation result is incomplete or unsupported")
	}
	for _, changes := range [][]observation.ObservationChange{result.ObservedChanges, result.ProposedChanges} {
		seen := map[string]bool{}
		for _, change := range changes {
			if !observationIDPattern.MatchString(change.Id) || !observationTypePattern.MatchString(change.ResourceType) || len(change.Actions) < 1 || len(change.Actions) > observationReplacementActionCount || seen[change.Id] {
				return NewErrBadRequest("observation changes are invalid")
			}
			if len(change.Actions) == observationReplacementActionCount {
				replacement := (change.Actions[0] == observation.Create && change.Actions[1] == observation.Delete) || (change.Actions[0] == observation.Delete && change.Actions[1] == observation.Create)
				if !replacement {
					return NewErrBadRequest("observation action pair is not a supported replacement")
				}
			}
			seen[change.Id] = true
			for _, action := range change.Actions {
				if !action.Valid() {
					return NewErrBadRequest("observation action is unsupported")
				}
			}
		}
	}
	return nil
}

// StoreObservationResult accepts only the generated allowlisted wire shape.
// Runner diagnostics, metadata and encrypted outputs are not part of this table.
func StoreObservationResult(ctx context.Context, tx Tx, deploymentID uuid.UUID, result observation.ObservationResult) error {
	if err := ValidateObservationResult(result); err != nil {
		return err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return NewErrBadRequest("observation result exceeds retention bound")
	}
	_, err = tx.ExecContext(ctx, `UPDATE environment_observation_receipts SET result=$2,result_expires_at=now()+$3::interval WHERE deployment_id=$1`, deploymentID, raw, ObservationResultRetention.String())
	return err
}

// RecordObservationCompletion retains only a digest for replay and stop proof.
// Late completion never changes an already failed observation into a clean run.
func RecordObservationCompletion(ctx context.Context, tx Tx, deploymentID uuid.UUID, body any) (bool, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return false, err
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	var previous sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT completion_sha256 FROM environment_observation_receipts WHERE deployment_id=$1 FOR UPDATE`, deploymentID).Scan(&previous); err != nil {
		return false, err
	}
	if previous.Valid {
		if previous.String != digest {
			return false, NewErrConflict("observation completion conflicts with immutable retained result")
		}
		return true, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE environment_observation_receipts SET completion_sha256=$2 WHERE deployment_id=$1`, deploymentID, digest); err != nil {
		return false, err
	}
	if err := RecordDeploymentCompletion(ctx, tx, deploymentID); err != nil {
		return false, err
	}
	return false, nil
}

func ReplayObservationRequest(ctx context.Context, tx Tx, orgID, pluginID string, actorID uuid.UUID, request observation.ObservationRequest) (*observation.ObservationReceipt, error) {
	digest, err := ObservationRequestDigest(request)
	if err != nil {
		return nil, err
	}
	if err := LockObservationRequest(ctx, tx, ObservationRequestKindObservation, orgID, pluginID, request.RequestId); err != nil {
		return nil, err
	}
	if err := RequireObservationRequestUnsealed(ctx, tx, ObservationRequestKindObservation, orgID, pluginID, request.RequestId, actorID, uuid.Nil, digest); err != nil {
		return nil, err
	}
	var previousDigest string
	var previousActor uuid.UUID
	err = tx.QueryRowContext(ctx, `SELECT request_sha256,actor_id FROM environment_observation_receipts WHERE org_id=$1 AND plugin_id=$2 AND request_id=$3`, orgID, pluginID, request.RequestId).Scan(&previousDigest, &previousActor)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewErrNotFound("observation request not found")
	}
	if err != nil {
		return nil, err
	}
	if digest != previousDigest || previousActor != actorID {
		return nil, NewErrConflict("observation request identity was already used with different content")
	}
	return ReadObservationReceipt(ctx, tx, orgID, pluginID, request.RequestId)
}

func ObservationBaselineFingerprint(manifest EncodedDeploymentManifest, tofu RawTofu, graph EncodedDeploymentGraph, artifacts []ModuleArtifactRequirement) string {
	raw, _ := json.Marshal(artifacts)
	fingerprint := sha256.New()
	for _, part := range [][]byte{manifest, tofu, graph, raw} {
		_, _ = fmt.Fprintf(fingerprint, "%d:", len(part))
		_, _ = fingerprint.Write(part)
	}
	return "sha256:" + hex.EncodeToString(fingerprint.Sum(nil))
}
