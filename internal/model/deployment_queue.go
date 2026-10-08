package model

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

// The Environment deployment queue holds deploy and rollback requests that
// found their Environment busy and asked to wait. See docs/deployment-queue.md.
//
//	enqueue --> [queued] --cancel--> [cancelled]
//	              |  \----TTL------> [expired]
//	              +--promote--> [dispatched]  (the Deployment owns everything after this)
//	              +--promote--> [rejected]    (permanent failure, reason_code)

type DeploymentQueueState string

const (
	DeploymentQueueQueued     DeploymentQueueState = "queued"
	DeploymentQueueDispatched DeploymentQueueState = "dispatched"
	DeploymentQueueCancelled  DeploymentQueueState = "cancelled"
	DeploymentQueueExpired    DeploymentQueueState = "expired"
	DeploymentQueueRejected   DeploymentQueueState = "rejected"
)

const (
	CodeQueueItemDispatching = "queue_item_dispatching"
	CodeQueueItemSettled     = "queue_item_settled"

	QueueReasonCancelled = "cancelled"
	QueueReasonExpired   = "expired"
)

type DeploymentQueueItem struct {
	ID                       uuid.UUID
	Seq                      int64
	OrgID                    string
	ProjectID                string
	EnvID                    string
	EnvironmentUUID          uuid.UUID
	Mode                     DeploymentMode
	Intent                   json.RawMessage
	IntentSHA256             string
	IdempotencyKeyDigest     opt.Opt[string]
	ExpectedBaseDeploymentID opt.Opt[uuid.UUID]
	CreatedBy                uuid.UUID
	CreatedAt                time.Time
	ExpiresAt                time.Time
	State                    DeploymentQueueState
	Attempts                 int
	NextAttemptAt            time.Time
	LastError                opt.Opt[string]
	ReasonCode               opt.Opt[string]
	ReasonMessage            opt.Opt[string]
	SettledBy                opt.Opt[uuid.UUID]
	SettledAt                opt.Opt[time.Time]
	DeploymentID             opt.Opt[uuid.UUID]
	// Position is the 1-based place in the Environment's queue while queued, otherwise 0.
	Position int
}

type NewDeploymentQueueItem struct {
	OrgID                    string
	ProjectID                string
	EnvID                    string
	EnvironmentUUID          uuid.UUID
	Mode                     DeploymentMode
	Intent                   json.RawMessage
	IdempotencyKeyDigest     opt.Opt[string]
	ExpectedBaseDeploymentID opt.Opt[uuid.UUID]
	CreatedBy                uuid.UUID
	TTL                      time.Duration
}

// IntentSHA256 identifies the request for idempotent replay. The requesting
// actor is deliberately not part of it, as for Deployment idempotency.
func (n NewDeploymentQueueItem) IntentSHA256() string {
	encoded, _ := json.Marshal(struct {
		ProjectID    string             `json:"project_id"`
		EnvID        string             `json:"env_id"`
		Mode         DeploymentMode     `json:"mode"`
		Intent       json.RawMessage    `json:"intent"`
		ExpectedBase opt.Opt[uuid.UUID] `json:"expected_base_deployment_id"`
	}{n.ProjectID, n.EnvID, n.Mode, n.Intent, n.ExpectedBaseDeploymentID})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

type DeploymentQueueSettlement struct {
	ReasonCode    string
	ReasonMessage string
	SettledBy     opt.Opt[uuid.UUID]
	DeploymentID  opt.Opt[uuid.UUID]
}

type ListDeploymentQueueParams struct {
	ProjectID opt.Opt[string]
	EnvID     opt.Opt[string]
	State     opt.Opt[DeploymentQueueState]
}

const deploymentQueueColumns = `q.id, q.seq, q.org_id, q.project_id, q.env_id, q.environment_uuid, q.mode, q.intent,
	q.intent_sha256, q.idempotency_key_digest, q.expected_base_deployment_id, q.created_by, q.created_at, q.expires_at,
	q.state, q.attempts, q.next_attempt_at, q.last_error, q.reason_code, q.reason_message, q.settled_by, q.settled_at,
	q.deployment_id,
	CASE WHEN q.state = 'queued' THEN (SELECT count(*) FROM environment_deployment_queue ahead
		WHERE ahead.environment_uuid = q.environment_uuid AND ahead.state = 'queued' AND ahead.seq <= q.seq) ELSE 0 END`

func scanDeploymentQueueItem(row interface{ Scan(...any) error }) (*DeploymentQueueItem, error) {
	var item DeploymentQueueItem
	var intent []byte
	if err := row.Scan(&item.ID, &item.Seq, &item.OrgID, &item.ProjectID, &item.EnvID, &item.EnvironmentUUID, &item.Mode, &intent,
		&item.IntentSHA256, opt.Scan(&item.IdempotencyKeyDigest), opt.Scan(&item.ExpectedBaseDeploymentID), &item.CreatedBy,
		&item.CreatedAt, &item.ExpiresAt, &item.State, &item.Attempts, &item.NextAttemptAt, opt.Scan(&item.LastError),
		opt.Scan(&item.ReasonCode), opt.Scan(&item.ReasonMessage), opt.Scan(&item.SettledBy), opt.Scan(&item.SettledAt),
		opt.Scan(&item.DeploymentID), &item.Position); err != nil {
		return nil, err
	}
	item.Intent = intent
	return &item, nil
}

// CheckEnvironmentAvailable reports why a mutating Deployment cannot start in
// the Environment now: ErrEnvironmentBusy, ErrOutcomeUnreconciled or nil.
// queueItem names the queue head being promoted, uuid.Nil for everyone else.
// It is an early read that keeps busy requests away from remote effects; only
// the same checks in CreateDeployment, under the Environment lock, decide.
func CheckEnvironmentAvailable(ctx context.Context, tx Tx, orgID, projectID, envID string, environmentUUID, queueItem uuid.UUID) error {
	var leased, executing bool
	var stateHead, queueHead uuid.NullUUID
	if err := tx.QueryRowContext(ctx, `SELECT
		EXISTS (SELECT 1 FROM environment_deployment_leases WHERE environment_id = $4 AND expires_at > now()),
		EXISTS (SELECT 1 FROM deployment_environments e JOIN deployments d ON d.de_id = e.id AND d.id = e.last_executing_deployment_id
			WHERE e.org_id = $1 AND e.project_id = $2 AND e.env_id = $3 AND d.status = 'executing'),
		(SELECT last_state_deployment_id FROM deployment_environments WHERE org_id = $1 AND project_id = $2 AND env_id = $3),
		(SELECT id FROM environment_deployment_queue WHERE environment_uuid = $4 AND state = 'queued' ORDER BY seq LIMIT 1)`,
		orgID, projectID, envID, environmentUUID).Scan(&leased, &executing, &stateHead, &queueHead); err != nil {
		return errors.Wrap(err, "failed to read environment availability")
	}
	if leased || executing {
		return NewErrCodedConflict(CodeEnvironmentBusy, "the Environment has a running Deployment")
	}
	if err := requireReplaceableStateHead(ctx, tx, stateHead.UUID); err != nil {
		return err
	}
	if queueHead.Valid && queueHead.UUID != queueItem {
		return NewErrCodedConflict(CodeEnvironmentBusy, "Deployments are queued for the Environment")
	}
	return nil
}

func requireReplaceableStateHead(ctx context.Context, tx Tx, stateHead uuid.UUID) error {
	fence, err := ReadEnvironmentFence(ctx, tx, stateHead)
	if err != nil {
		return err
	}
	if fence.IsSet() {
		return NewErrCodedConflict(CodeOutcomeUnreconciled, fmt.Sprintf("failed plugin-dispatched Deployment '%s' may still have an active Runner; outcome is unreconciled", stateHead))
	}
	return nil
}

// requireQueueHead lets only the promoted head through while any item waits.
// Callers hold the Environment lock, so a concurrent enqueue cannot slip past.
func requireQueueHead(ctx context.Context, tx Tx, environmentUUID, queueItem uuid.UUID) error {
	var head uuid.UUID
	err := tx.QueryRowContext(ctx, `SELECT id FROM environment_deployment_queue
		WHERE environment_uuid = $1 AND state = 'queued' ORDER BY seq LIMIT 1`, environmentUUID).Scan(&head)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if queueItem != uuid.Nil {
			return NewErrConflict("the promoted queue item is no longer waiting")
		}
		return nil
	case err != nil:
		return errors.Wrap(err, "failed to read deployment queue head")
	case head != queueItem:
		return NewErrCodedConflict(CodeEnvironmentBusy, "Deployments are queued for the Environment")
	}
	return nil
}

// PendingEnvironmentChanges counts the Deployments that will change an
// Environment before anything requested now: the running deploy, rollback or
// destroy and the requests waiting in its queue.
type PendingEnvironmentChanges struct {
	Running int
	Queued  int
}

func (p PendingEnvironmentChanges) Any() bool {
	return p.Running > 0 || p.Queued > 0
}

func (d *databaser) ReadPendingEnvironmentChanges(ctx context.Context, optionalTx Tx, orgID, projectID, envID string, environmentUUID uuid.UUID) (PendingEnvironmentChanges, error) {
	var pending PendingEnvironmentChanges
	err := d.txOrDb(optionalTx).QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM deployment_environments e JOIN deployments d ON d.de_id = e.id AND d.id = e.last_state_deployment_id
			WHERE e.org_id = $1 AND e.project_id = $2 AND e.env_id = $3 AND d.status = 'executing' AND d.mode IN ('deploy', 'rollback', 'destroy')),
		(SELECT count(*) FROM environment_deployment_queue WHERE environment_uuid = $4 AND state = 'queued')`,
		orgID, projectID, envID, environmentUUID).Scan(&pending.Running, &pending.Queued)
	return pending, errors.Wrap(err, "failed to read pending environment changes")
}

// EnqueueDeployment appends a request to its Environment's queue, or replays
// the item an identical keyed request created within the idempotency window.
func EnqueueDeployment(ctx context.Context, tx Tx, request NewDeploymentQueueItem, maxDepth int) (*DeploymentQueueItem, bool, error) {
	if tx == nil {
		return nil, false, errors.New("transaction is required")
	}
	if request.Mode != DeploymentModeDeploy && request.Mode != DeploymentModeRollback {
		return nil, false, NewErrBadRequest("only deploy and rollback requests can wait in the deployment queue")
	}
	if request.TTL <= 0 || maxDepth <= 0 {
		return nil, false, errors.New("deployment queue TTL and depth must be positive")
	}
	if err := LockEnvironmentMutation(ctx, tx, request.EnvironmentUUID); err != nil {
		return nil, false, err
	}
	intentSHA256 := request.IntentSHA256()
	if request.IdempotencyKeyDigest.IsSet() {
		existing, err := FindDeploymentQueueItemByIdempotencyKey(ctx, tx, request.OrgID, request.EnvironmentUUID, request.IdempotencyKeyDigest.Must())
		if err == nil {
			if existing.IntentSHA256 != intentSHA256 {
				return nil, false, NewErrConflict("incorrect request for this idempotency key")
			}
			return existing, true, nil
		}
		if _, missing := IsErrNotFound(err); !missing {
			return nil, false, err
		}
	}
	var stateHead uuid.NullUUID
	if err := tx.QueryRowContext(ctx, `SELECT last_state_deployment_id FROM deployment_environments
		WHERE org_id = $1 AND project_id = $2 AND env_id = $3`, request.OrgID, request.ProjectID, request.EnvID).Scan(&stateHead); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, false, errors.Wrap(err, "failed to read environment state head")
	}
	if err := requireReplaceableStateHead(ctx, tx, stateHead.UUID); err != nil {
		return nil, false, err
	}
	var depth int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM environment_deployment_queue
		WHERE environment_uuid = $1 AND state = 'queued'`, request.EnvironmentUUID).Scan(&depth); err != nil {
		return nil, false, errors.Wrap(err, "failed to count queued deployments")
	}
	if depth >= maxDepth {
		return nil, false, NewErrCodedConflict(CodeDeploymentQueueFull, fmt.Sprintf("the Environment already has %d queued Deployments", depth))
	}
	item, err := scanDeploymentQueueItem(tx.QueryRowContext(ctx, `INSERT INTO environment_deployment_queue AS q
		(id, org_id, project_id, env_id, environment_uuid, mode, intent, intent_sha256, idempotency_key_digest,
		 expected_base_deployment_id, created_by, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now() + $12::interval)
		RETURNING `+deploymentQueueColumns,
		uuid.New(), request.OrgID, request.ProjectID, request.EnvID, request.EnvironmentUUID, request.Mode, []byte(request.Intent),
		intentSHA256, request.IdempotencyKeyDigest.Ref(), request.ExpectedBaseDeploymentID.Ref(), request.CreatedBy, request.TTL.String()))
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to enqueue deployment")
	}
	// The position subquery in RETURNING cannot see the row it is inserting.
	item.Position = depth + 1
	return item, false, nil
}

// FindDeploymentQueueItemByIdempotencyKey returns the newest item a key created within the idempotency window.
func FindDeploymentQueueItemByIdempotencyKey(ctx context.Context, tx Tx, orgID string, environmentUUID uuid.UUID, keyDigest string) (*DeploymentQueueItem, error) {
	item, err := scanDeploymentQueueItem(tx.QueryRowContext(ctx, `SELECT `+deploymentQueueColumns+`
		FROM environment_deployment_queue q
		WHERE q.org_id = $1 AND q.environment_uuid = $2 AND q.idempotency_key_digest = $3 AND q.created_at > $4
		ORDER BY q.seq DESC LIMIT 1`, orgID, environmentUUID, keyDigest, time.Now().Add(-idempotencyDigestTimeLimit)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewErrNotFound("no deployment queue item for this idempotency key")
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to find deployment queue item by idempotency key")
	}
	return item, nil
}

func GetDeploymentQueueItem(ctx context.Context, tx Tx, orgID string, id uuid.UUID) (*DeploymentQueueItem, error) {
	item, err := scanDeploymentQueueItem(tx.QueryRowContext(ctx, `SELECT `+deploymentQueueColumns+`
		FROM environment_deployment_queue q WHERE q.org_id = $1 AND q.id = $2`, orgID, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewErrNotFound(fmt.Sprintf("deployment queue item '%s' not found", id))
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to get deployment queue item")
	}
	return item, nil
}

// ListDeploymentQueueItems pages through an organisation's queue items in FIFO order.
func ListDeploymentQueueItems(ctx context.Context, tx Tx, orgID, pageToken string, perPage int, params ListDeploymentQueueParams) ([]DeploymentQueueItem, string, error) {
	var after int64
	if pageToken != "" {
		parsed, err := strconv.ParseInt(pageToken, 10, 64)
		if err != nil || parsed < 0 {
			return nil, "", NewErrBadRequest(fmt.Sprintf("failed to parse page token %q", pageToken))
		}
		after = parsed
	}
	limit := max(1, perPage)
	rows, err := tx.QueryContext(ctx, `SELECT `+deploymentQueueColumns+`
		FROM environment_deployment_queue q
		WHERE q.org_id = $1 AND q.seq > $2
		  AND ($3::text IS NULL OR q.project_id = $3) AND ($4::text IS NULL OR q.env_id = $4)
		  AND ($5::text IS NULL OR q.state = $5)
		ORDER BY q.seq LIMIT $6`, orgID, after, params.ProjectID.Ref(), params.EnvID.Ref(), params.State.Ref(), limit+1)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to list deployment queue items")
	}
	defer func() { _ = rows.Close() }()
	items := make([]DeploymentQueueItem, 0, limit)
	for rows.Next() {
		item, err := scanDeploymentQueueItem(rows)
		if err != nil {
			return nil, "", errors.Wrap(err, "failed to scan deployment queue item")
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", errors.Wrap(err, "failed to iterate deployment queue items")
	}
	if len(items) > limit {
		items = items[:limit]
		return items, strconv.FormatInt(items[limit-1].Seq, 10), nil
	}
	return items, "", nil
}

// ClaimDeploymentQueueHead locks the Environment's queue head for the rest of
// the transaction. It returns nil when nothing waits or another promoter holds
// the head, so concurrent promoters never touch the same item.
func ClaimDeploymentQueueHead(ctx context.Context, tx Tx, environmentUUID uuid.UUID) (*DeploymentQueueItem, error) {
	item, err := scanDeploymentQueueItem(tx.QueryRowContext(ctx, `SELECT `+deploymentQueueColumns+`
		FROM environment_deployment_queue q
		WHERE q.id = (SELECT id FROM environment_deployment_queue
		              WHERE environment_uuid = $1 AND state = 'queued' ORDER BY seq LIMIT 1)
		  AND q.state = 'queued'
		FOR UPDATE OF q SKIP LOCKED`, environmentUUID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to claim deployment queue head")
	}
	return item, nil
}

// SettleDeploymentQueueItem moves one waiting item to a terminal state exactly once.
func SettleDeploymentQueueItem(ctx context.Context, tx Tx, id uuid.UUID, state DeploymentQueueState, settlement DeploymentQueueSettlement) error {
	if state == DeploymentQueueQueued {
		return errors.New("a settled queue item cannot be queued")
	}
	result, err := tx.ExecContext(ctx, `UPDATE environment_deployment_queue
		SET state = $2, reason_code = NULLIF($3, ''), reason_message = NULLIF($4, ''), settled_by = $5,
		    deployment_id = $6, settled_at = now()
		WHERE id = $1 AND state = 'queued'`, id, state, settlement.ReasonCode, settlement.ReasonMessage,
		settlement.SettledBy.Ref(), settlement.DeploymentID.Ref())
	if err != nil {
		return errors.Wrap(err, "failed to settle deployment queue item")
	}
	if count, err := result.RowsAffected(); err != nil {
		return errors.Wrap(err, "failed to read settled queue item count")
	} else if count != 1 {
		return NewErrConflict("the queue item is no longer waiting")
	}
	return nil
}

// SettleQueuedDeployments settles every waiting item of an Environment at once.
func SettleQueuedDeployments(ctx context.Context, tx Tx, environmentUUID uuid.UUID, state DeploymentQueueState, reasonCode, reasonMessage string) (int64, error) {
	if state == DeploymentQueueQueued || state == DeploymentQueueDispatched || reasonCode == "" {
		return 0, errors.New("bulk queue settlement requires a terminal state without a Deployment and a reason")
	}
	result, err := tx.ExecContext(ctx, `UPDATE environment_deployment_queue
		SET state = $2, reason_code = $3, reason_message = NULLIF($4, ''), settled_at = now()
		WHERE environment_uuid = $1 AND state = 'queued'`, environmentUUID, state, reasonCode, reasonMessage)
	if err != nil {
		return 0, errors.Wrap(err, "failed to settle queued deployments")
	}
	return result.RowsAffected()
}

// DeferDeploymentQueueItem records a transient promotion failure and when the sweeper may retry.
func DeferDeploymentQueueItem(ctx context.Context, tx Tx, id uuid.UUID, lastError string, delay time.Duration) error {
	_, err := tx.ExecContext(ctx, `UPDATE environment_deployment_queue
		SET attempts = attempts + 1, last_error = $2, next_attempt_at = now() + $3::interval
		WHERE id = $1 AND state = 'queued'`, id, lastError, delay.String())
	return errors.Wrap(err, "failed to defer deployment queue item")
}

// CancelDeploymentQueueItem cancels a waiting item for its actor. NOWAIT turns
// a race with an in-flight promotion into a conflict the caller re-reads,
// instead of a cancellation that silently loses to the dispatch.
func CancelDeploymentQueueItem(ctx context.Context, tx Tx, orgID string, id, actor uuid.UUID, reason string) (*DeploymentQueueItem, bool, error) {
	item, err := scanDeploymentQueueItem(tx.QueryRowContext(ctx, `SELECT `+deploymentQueueColumns+`
		FROM environment_deployment_queue q WHERE q.org_id = $1 AND q.id = $2 FOR UPDATE OF q NOWAIT`, orgID, id))
	var pqErr *pq.Error
	switch {
	case errors.As(err, &pqErr) && pqErr.Code == "55P03":
		return nil, false, NewErrCodedConflict(CodeQueueItemDispatching, "the queue item is being promoted; read it again")
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, NewErrNotFound(fmt.Sprintf("deployment queue item '%s' not found", id))
	case err != nil:
		return nil, false, errors.Wrap(err, "failed to lock deployment queue item")
	}
	if item.State == DeploymentQueueCancelled && item.SettledBy == opt.Of(actor) && item.ReasonMessage.Or("") == reason {
		return item, true, nil
	}
	if item.State != DeploymentQueueQueued {
		return nil, false, NewErrCodedConflict(CodeQueueItemSettled, fmt.Sprintf("the queue item is already %s", item.State))
	}
	if err := SettleDeploymentQueueItem(ctx, tx, id, DeploymentQueueCancelled, DeploymentQueueSettlement{
		ReasonCode: QueueReasonCancelled, ReasonMessage: reason, SettledBy: opt.Of(actor),
	}); err != nil {
		return nil, false, err
	}
	item, err = GetDeploymentQueueItem(ctx, tx, orgID, id)
	return item, false, err
}

// ExpireDeploymentQueueItems expires waiting items past their TTL, skipping
// items a promoter holds. It returns the Environments whose queue changed.
func ExpireDeploymentQueueItems(ctx context.Context, tx Tx, limit int) ([]uuid.UUID, error) {
	rows, err := tx.QueryContext(ctx, `UPDATE environment_deployment_queue
		SET state = 'expired', reason_code = 'expired', reason_message = 'the request waited longer than its queue TTL', settled_at = now()
		WHERE id IN (SELECT id FROM environment_deployment_queue WHERE state = 'queued' AND expires_at <= now()
		             ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED)
		RETURNING environment_uuid`, limit)
	if err != nil {
		return nil, errors.Wrap(err, "failed to expire deployment queue items")
	}
	return scanEnvironmentUUIDs(rows)
}

// DueDeploymentQueueEnvironments lists Environments whose queue head may be retried now.
func DueDeploymentQueueEnvironments(ctx context.Context, tx Tx, limit int) ([]uuid.UUID, error) {
	rows, err := tx.QueryContext(ctx, `SELECT environment_uuid FROM (
			SELECT DISTINCT ON (environment_uuid) environment_uuid, next_attempt_at
			FROM environment_deployment_queue WHERE state = 'queued' ORDER BY environment_uuid, seq
		) head WHERE next_attempt_at <= now() ORDER BY next_attempt_at LIMIT $1`, limit)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list due deployment queues")
	}
	return scanEnvironmentUUIDs(rows)
}

// PruneDeploymentQueue deletes settled items. A dispatched item's Deployment remains the evidence.
func PruneDeploymentQueue(ctx context.Context, tx Tx, settledBefore time.Time) (int64, error) {
	result, err := tx.ExecContext(ctx, `DELETE FROM environment_deployment_queue WHERE state <> 'queued' AND settled_at < $1`, settledBefore)
	if err != nil {
		return 0, errors.Wrap(err, "failed to prune deployment queue")
	}
	return result.RowsAffected()
}

func scanEnvironmentUUIDs(rows *sql.Rows) ([]uuid.UUID, error) {
	defer func() { _ = rows.Close() }()
	seen := map[uuid.UUID]struct{}{}
	out := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Wrap(err, "failed to scan environment")
		}
		if _, duplicate := seen[id]; !duplicate {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out, errors.Wrap(rows.Err(), "failed to iterate environments")
}
