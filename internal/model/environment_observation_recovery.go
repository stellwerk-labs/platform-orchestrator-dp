package model

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
)

type ObservationRequestKind string

const (
	ObservationRequestKindObservation ObservationRequestKind = "observation"
	ObservationRequestKindRedeploy    ObservationRequestKind = "redeploy"
)

var ErrObservationRequestSealed = errors.New("observation request was sealed without starting")
var ErrObservationIsolationUnsupported = errors.New("observation request arbitration requires READ COMMITTED isolation")

// LockObservationRequest is shared by forwarding and safety resolution. Neither
// a missing receipt nor an elapsed deadline establishes that a request stopped.
func LockObservationRequest(ctx context.Context, tx Tx, kind ObservationRequestKind, orgID, pluginID string, requestID uuid.UUID) error {
	var prefix string
	switch kind {
	case ObservationRequestKindObservation:
		prefix = "observation-request:"
	case ObservationRequestKindRedeploy:
		prefix = "observation-redeploy:"
	default:
		return NewErrBadRequest("invalid observation request kind")
	}
	// A transaction-wide snapshot taken before a competing lock holder commits
	// cannot prove absence after waiting. Fail closed rather than seal or admit
	// using a stale view, including when the connection default was overridden.
	var isolation string
	if err := tx.QueryRowContext(ctx, `SELECT current_setting('transaction_isolation')`).Scan(&isolation); err != nil {
		return err
	}
	if isolation != "read committed" {
		return ErrObservationIsolationUnsupported
	}
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, prefix+orgID+":"+pluginID+":"+requestID.String())
	return err
}

func ObservationRequestDigest(request any) (string, error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// RequireObservationRequestUnsealed must run while holding the request lock
// before an admission can create work. An exact negative receipt is immutable.
func RequireObservationRequestUnsealed(ctx context.Context, tx Tx, kind ObservationRequestKind, orgID, pluginID string, requestID, actorID, runID uuid.UUID, digest string) error {
	var previousActor uuid.UUID
	var previousRun uuid.NullUUID
	var previousDigest string
	err := tx.QueryRowContext(ctx, `SELECT actor_id,run_id,request_sha256 FROM environment_observation_request_seals WHERE org_id=$1 AND plugin_id=$2 AND request_kind=$3 AND request_id=$4`, orgID, pluginID, kind, requestID).Scan(&previousActor, &previousRun, &previousDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if previousActor != actorID || previousDigest != digest || previousRun.Valid != (runID != uuid.Nil) || previousRun.Valid && previousRun.UUID != runID {
		return NewErrConflict("sealed observation request identity has different content")
	}
	return ErrObservationRequestSealed
}

func sealObservationRequest(ctx context.Context, tx Tx, kind ObservationRequestKind, orgID, pluginID string, requestID, actorID, runID uuid.UUID, digest string) error {
	var run any
	if runID != uuid.Nil {
		run = runID
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO environment_observation_request_seals(org_id,plugin_id,request_kind,request_id,actor_id,request_sha256,run_id) VALUES($1,$2,$3,$4,$5,$6,$7)`, orgID, pluginID, kind, requestID, actorID, digest, run)
	return err
}

func ResolveObservationRequest(ctx context.Context, db Databaser, orgID, pluginID string, actorID uuid.UUID, request observation.ObservationRequest) (*observation.ObservationResolution, error) {
	digest, err := ObservationRequestDigest(request)
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err = LockObservationRequest(ctx, tx, ObservationRequestKindObservation, orgID, pluginID, request.RequestId); err != nil {
		return nil, err
	}
	out := &observation.ObservationResolution{RequestId: request.RequestId, Disposition: observation.ObservationResolutionDispositionNotStarted}
	err = RequireObservationRequestUnsealed(ctx, tx, ObservationRequestKindObservation, orgID, pluginID, request.RequestId, actorID, uuid.Nil, digest)
	if errors.Is(err, ErrObservationRequestSealed) {
		return out, tx.Commit()
	}
	if err != nil {
		return nil, err
	}
	receipt, err := ReplayObservationRequest(ctx, tx, orgID, pluginID, actorID, request)
	if err == nil {
		switch receipt.Disposition {
		case observation.ObservationReceiptDispositionAccepted:
			if receipt.RunId == nil || *receipt.RunId == uuid.Nil {
				return nil, errors.New("retained accepted observation receipt has no run identity")
			}
			out.Disposition = observation.ObservationResolutionDispositionAccepted
			out.Receipt = receipt
		case observation.ObservationReceiptDispositionSkipped:
			var ownsDeployment bool
			if err := tx.QueryRowContext(ctx, `SELECT deployment_id IS NOT NULL FROM environment_observation_receipts WHERE org_id=$1 AND plugin_id=$2 AND request_id=$3`, orgID, pluginID, request.RequestId).Scan(&ownsDeployment); err != nil {
				return nil, err
			}
			if ownsDeployment || receipt.RunId != nil {
				return nil, errors.New("retained skipped observation receipt has an execution identity")
			}
		default:
			return nil, errors.New("retained observation receipt has an unknown disposition")
		}
		return out, tx.Commit()
	}
	if _, missing := IsErrNotFound(err); !missing {
		return nil, err
	}
	if err = sealObservationRequest(ctx, tx, ObservationRequestKindObservation, orgID, pluginID, request.RequestId, actorID, uuid.Nil, digest); err != nil {
		return nil, err
	}
	return out, tx.Commit()
}

func ResolveObservationRedeploy(ctx context.Context, db Databaser, orgID, pluginID string, actorID uuid.UUID, runID uuid.UUID, request observation.ObservationRedeployRequest) (*observation.ObservationRedeployResolution, error) {
	digest, err := ObservationRequestDigest(request)
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err = LockObservationRequest(ctx, tx, ObservationRequestKindRedeploy, orgID, pluginID, request.RequestId); err != nil {
		return nil, err
	}
	out := &observation.ObservationRedeployResolution{RequestId: request.RequestId, Disposition: observation.ObservationRedeployResolutionDispositionNotStarted}
	err = RequireObservationRequestUnsealed(ctx, tx, ObservationRequestKindRedeploy, orgID, pluginID, request.RequestId, actorID, runID, digest)
	if errors.Is(err, ErrObservationRequestSealed) {
		return out, tx.Commit()
	}
	if err != nil {
		return nil, err
	}
	var previousActor, previousRun uuid.UUID
	var previousDigest string
	receipt := &observation.ObservationRedeployReceipt{}
	err = tx.QueryRowContext(ctx, `SELECT r.actor_id,r.run_id,r.request_sha256,r.request_id,r.deployment_id,r.baseline_id,COALESCE(d.status,'unavailable') FROM environment_observation_redeploy_receipts r LEFT JOIN deployments d ON d.id=r.deployment_id WHERE r.org_id=$1 AND r.plugin_id=$2 AND r.request_id=$3`, orgID, pluginID, request.RequestId).
		Scan(&previousActor, &previousRun, &previousDigest, &receipt.RequestId, &receipt.DeploymentId, &receipt.BaselineId, &receipt.Status)
	if err == nil {
		if previousActor != actorID || previousRun != runID || previousDigest != digest {
			return nil, NewErrConflict("redeploy request identity already used with different content")
		}
		out.Disposition = observation.ObservationRedeployResolutionDispositionAccepted
		out.Receipt = receipt
		return out, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if err = sealObservationRequest(ctx, tx, ObservationRequestKindRedeploy, orgID, pluginID, request.RequestId, actorID, runID, digest); err != nil {
		return nil, err
	}
	return out, tx.Commit()
}
