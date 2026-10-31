package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/herrors"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

// Promotion turns the head of an Environment's queue into a Deployment. Any
// Data Plane replica may promote; there is no leader. One transaction holds,
// in this order:
//
//	head row      FOR UPDATE SKIP LOCKED   other promoters skip, cancel gets NOWAIT
//	env lock      pg_advisory_xact_lock    taken by CreateDeployment, serialises every creation path
//	lease         environment_deployment_leases, unchanged, acquired by CreateDeployment
//	state CAS     UPDATE ... WHERE state = 'queued'
//
// Nothing outside the database happens before commit except Core admission,
// which has its own durable receipt and is sealed absent when the attempt
// rolls back or the process dies.

const (
	DeploymentQueueSweepInterval     = 15 * time.Second
	deploymentQueuePromotionTimeout  = time.Minute
	deploymentQueueRetention         = 30 * 24 * time.Hour
	deploymentQueueSweepBatch        = 100
	deploymentQueueRetryBackoff      = 5 * time.Second
	deploymentQueueMaxRetryBackoff   = 2 * time.Minute
	deploymentQueueReasonForbidden   = "actor_forbidden"
	deploymentQueueReasonAdmission   = "admission_denied"
	deploymentQueueReasonInvalid     = "invalid_request"
	deploymentQueueReasonNotFound    = "not_found"
	deploymentQueueReasonConflict    = "conflict"
	deploymentQueueReasonEnvironment = "environment_not_found"
)

type queuePromotionVerdict int

const (
	queueRetryLater queuePromotionVerdict = iota
	queueRejectItem
	queueRejectAll
)

// PromoteDeploymentQueue promotes the Environment's queue heads until one
// becomes a Deployment, the Environment is busy, or nothing is left.
func (s *Server) PromoteDeploymentQueue(ctx context.Context, environmentUUID uuid.UUID) error {
	for {
		next, err := s.promoteDeploymentQueueHead(ctx, environmentUUID)
		if err != nil || !next {
			return err
		}
	}
}

// promoteDeploymentQueueHead reports whether the next item may be tried at once.
func (s *Server) promoteDeploymentQueueHead(ctx context.Context, environmentUUID uuid.UUID) (bool, error) {
	logger := s.Logger.With(zap.String("environment_uuid", environmentUUID.String()))
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return false, errors.Wrap(err, "failed to begin queue promotion")
	}
	defer func() { _ = tx.Rollback() }()
	// Bound how long a stalled promoter can hold the head or wait for the Environment lock.
	for _, statement := range []string{`SET LOCAL lock_timeout = '5s'`, `SET LOCAL idle_in_transaction_session_timeout = '60s'`} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return false, errors.Wrap(err, "failed to bound queue promotion")
		}
	}
	item, err := model.ClaimDeploymentQueueHead(ctx, tx, environmentUUID)
	if err != nil || item == nil {
		return false, err
	}
	logger = logger.With(zap.String("queue_item_id", item.ID.String()))
	if !item.ExpiresAt.After(time.Now()) {
		if err := model.SettleDeploymentQueueItem(ctx, tx, item.ID, model.DeploymentQueueExpired, model.DeploymentQueueSettlement{
			ReasonCode: model.QueueReasonExpired, ReasonMessage: "the request waited longer than its queue TTL",
		}); err != nil {
			return false, err
		}
		return true, errors.Wrap(tx.Commit(), "failed to commit queue expiry")
	}
	switch err := model.CheckEnvironmentAvailable(ctx, tx, item.OrgID, item.ProjectID, item.EnvID, environmentUUID, item.ID); {
	case errors.Is(err, model.ErrEnvironmentBusy):
		return false, nil
	case errors.Is(err, model.ErrOutcomeUnreconciled):
		return false, s.rejectQueuedDeployments(ctx, logger, tx, environmentUUID, err)
	case err != nil:
		return false, err
	}
	if failed, err := model.HaltingDeploymentQueuePredecessor(ctx, tx, item); err != nil {
		return false, err
	} else if failed.IsSet() {
		logger.Info("halted queued deployment after its predecessor failed", zap.String("deployment_id", failed.Must().String()))
		if err := model.SettleDeploymentQueueItem(ctx, tx, item.ID, model.DeploymentQueueRejected, model.DeploymentQueueSettlement{
			ReasonCode: model.QueueReasonPreviousDeploymentFailed, ReasonMessage: fmt.Sprintf("the previous Deployment '%s' failed", failed.Must()),
			ReasonDeploymentID: failed,
		}); err != nil {
			return false, err
		}
		return true, errors.Wrap(tx.Commit(), "failed to commit queue halt")
	}

	if _, err := tx.ExecContext(ctx, `SAVEPOINT queue_promotion`); err != nil {
		return false, errors.Wrap(err, "failed to start queue promotion attempt")
	}
	dep, messages, admission, err := s.createQueuedDeployment(ctx, logger, tx, item)
	if err == nil {
		if err := model.SettleDeploymentQueueItem(ctx, tx, item.ID, model.DeploymentQueueDispatched, model.DeploymentQueueSettlement{DeploymentID: opt.Of(dep.Id)}); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			if sealErr := admission.SealAbsent(ctx); sealErr != nil {
				logger.Warn("failed to seal absent queue admission after commit failure; reconciliation seals it", zap.Error(sealErr))
			}
			return false, errors.Wrap(err, "failed to commit queue promotion")
		}
		logger.Info("dispatched queued deployment", zap.String("deployment_id", dep.Id.String()))
		reliableoutbox.OptimisticPublish(ctx, logger, s.Database.AsReliableOutboxStore(), s.EventPublisher, messages)
		return false, nil
	}
	if _, rollbackErr := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT queue_promotion`); rollbackErr != nil {
		return false, errors.Wrap(rollbackErr, "failed to roll back queue promotion attempt")
	}
	if sealErr := admission.SealAbsent(ctx); sealErr != nil {
		logger.Warn("failed to seal absent queue admission; reconciliation seals it", zap.Error(sealErr))
	}
	verdict, reason := classifyQueuePromotionError(err)
	switch verdict {
	case queueRejectAll:
		return false, s.rejectQueuedDeployments(ctx, logger, tx, environmentUUID, err)
	case queueRejectItem:
		logger.Info("rejected queued deployment", zap.String("reason", reason), zap.Error(err))
		conflict, _ := model.IsErrConflict(err)
		if err := model.SettleDeploymentQueueItem(ctx, tx, item.ID, model.DeploymentQueueRejected, model.DeploymentQueueSettlement{
			ReasonCode: reason, ReasonMessage: err.Error(), ReasonDeploymentID: conflict.DeploymentID,
		}); err != nil {
			return false, err
		}
		return true, errors.Wrap(tx.Commit(), "failed to commit queue rejection")
	default:
		logger.Info("queued deployment waits after a transient failure", zap.Int("attempts", item.Attempts+1), zap.Error(err))
		if err := model.DeferDeploymentQueueItem(ctx, tx, item.ID, err.Error(), deploymentQueueBackoff(item.Attempts+1)); err != nil {
			return false, err
		}
		return false, errors.Wrap(tx.Commit(), "failed to commit queue retry")
	}
}

// createQueuedDeployment resolves the stored intent like a new request, as
// the requesting user and against the Environment as it is now.
func (s *Server) createQueuedDeployment(ctx context.Context, logger *zap.Logger, tx model.Tx, item *model.DeploymentQueueItem) (*model.DeploymentSummary, []*hstandardoutbox.PendingEventMessage, *OrdinaryDeploymentAdmissionAttempt, error) {
	var request deploymentRequest
	if err := json.Unmarshal(item.Intent, &request); err != nil {
		return nil, nil, nil, model.NewErrBadRequest("the stored deployment request is unreadable")
	}
	response, err := s.ControlPlaneClient.GetEnvironmentWithResponse(ctx, item.OrgID, item.ProjectID, item.EnvID)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to get environment")
	}
	switch {
	case response.StatusCode() == http.StatusNotFound:
		return nil, nil, nil, model.NewErrCodedConflict(deploymentQueueReasonEnvironment, "the Environment no longer exists")
	case response.StatusCode() != http.StatusOK || response.JSON200 == nil:
		return nil, nil, nil, errors.Errorf("unexpected status code when getting environment: %s", response.Status())
	case response.JSON200.Uuid != item.EnvironmentUUID:
		return nil, nil, nil, model.NewErrCodedConflict(deploymentQueueReasonEnvironment, "the Environment was deleted and created again")
	}
	env := *response.JSON200
	if err := s.checkEnvAuthorization(ctx, item.CreatedBy, item.OrgID, env.Uuid, PermissionDeploymentWrite); err != nil {
		return nil, nil, nil, err
	}
	if len(request.DraftTestSnapshots) > 0 {
		if err := validateDraftTestSelection(request.DraftTestSnapshots); err != nil {
			return nil, nil, nil, err
		}
		if item.Mode != model.DeploymentModeDeploy && item.Mode != model.DeploymentModeDeployPlan {
			return nil, nil, nil, model.NewErrBadRequest("queued Draft test selection requires deploy or plan-only")
		}
	}
	options := s.deploymentOptions(ctx, item.CreatedBy, item.OrgID, env, item.Mode, request, "", false)
	options.ExpectedBaseDeploymentID = item.ExpectedBaseDeploymentID
	options.IdempotencyKeyDigest = item.IdempotencyKeyDigest
	options.QueueItemID = opt.Of(item.ID)
	dep, messages, _, err := CreateDeploymentWithOptions(
		ctx, logger, item.CreatedBy, item.OrgID, item.ProjectID, item.EnvID, env, item.Mode, opt.OfRef(request.RollbackToDeploymentID),
		ref.DerefOr(request.Manifest, DeploymentManifest{}), request.EncryptedOutputsRecipient, request.EncryptedLogsRecipient, nil, false,
		request.RunnerLogLevel, s.ControlPlaneClient, s.Database, tx, options,
	)
	return dep, messages, options.Admission, err
}

func (s *Server) rejectQueuedDeployments(ctx context.Context, logger *zap.Logger, tx model.TxWithCommit, environmentUUID uuid.UUID, cause error) error {
	fence, _ := model.IsErrConflict(cause)
	count, err := model.SettleQueuedDeployments(ctx, tx, environmentUUID, model.DeploymentQueueRejected, model.DeploymentQueueSettlement{
		ReasonCode: model.CodeOutcomeUnreconciled, ReasonMessage: cause.Error(), ReasonDeploymentID: fence.DeploymentID,
	})
	if err != nil {
		return err
	}
	logger.Info("rejected queued deployments behind an unreconciled plugin-dispatched Deployment", zap.Int64("count", count))
	return errors.Wrap(tx.Commit(), "failed to commit queue rejection")
}

// classifyQueuePromotionError decides whether a failed promotion is worth
// retrying. Anything the Environment's state or Core can resolve by itself is
// transient and bounded by the item's TTL; a decision about this request is
// permanent and lets the next item proceed.
func classifyQueuePromotionError(err error) (queuePromotionVerdict, string) {
	var denied *deploymentAdmissionDeniedError
	var platformErr *herrors.PlatformOrchestratorError
	switch {
	case errors.Is(err, model.ErrOutcomeUnreconciled):
		return queueRejectAll, model.CodeOutcomeUnreconciled
	case errors.Is(err, model.ErrEnvironmentBusy):
		return queueRetryLater, ""
	case errors.As(err, &denied):
		if denied.code != "" {
			return queueRejectItem, denied.code
		}
		return queueRejectItem, deploymentQueueReasonAdmission
	case errors.As(err, &platformErr) && platformErr.StatusCode == http.StatusForbidden:
		return queueRejectItem, deploymentQueueReasonForbidden
	}
	if conflict, ok := model.IsErrConflict(err); ok {
		if conflict.Code != "" {
			return queueRejectItem, conflict.Code
		}
		return queueRejectItem, deploymentQueueReasonConflict
	} else if _, ok := model.IsErrBadRequest(err); ok {
		return queueRejectItem, deploymentQueueReasonInvalid
	} else if _, ok := model.IsErrNotFound(err); ok {
		return queueRejectItem, deploymentQueueReasonNotFound
	}
	return queueRetryLater, ""
}

func deploymentQueueBackoff(attempt int) time.Duration {
	delay := deploymentQueueRetryBackoff
	for retry := 1; retry < attempt && delay < deploymentQueueMaxRetryBackoff; retry++ {
		delay *= 2
	}
	return min(delay, deploymentQueueMaxRetryBackoff)
}

// deploymentQueueKicks coalesces promotion requests per Environment within
// this process: a kick during a running promotion runs it once more after.
type deploymentQueueKicks struct {
	mu      sync.Mutex
	pending map[uuid.UUID]bool
	running sync.WaitGroup
}

// KickDeploymentQueue promotes the Environment's queue in the background.
// Triggers: an enqueue or cancel commits, a refused direct attempt released
// Core's admission guard, a Deployment ended and Core settled its admission.
// Promotion deliberately starts from a fresh context: a request context
// carries the echo context, whose organization-token shortcut must never
// authorize the re-check of another user's queued request.
func (s *Server) KickDeploymentQueue(environmentUUID uuid.UUID) {
	kicks := &s.queueKicks
	kicks.mu.Lock()
	if kicks.pending == nil {
		kicks.pending = map[uuid.UUID]bool{}
	}
	if _, running := kicks.pending[environmentUUID]; running {
		kicks.pending[environmentUUID] = true
		kicks.mu.Unlock()
		return
	}
	kicks.pending[environmentUUID] = false
	kicks.running.Add(1)
	kicks.mu.Unlock()
	go func() {
		defer kicks.running.Done()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), deploymentQueuePromotionTimeout)
			if err := s.PromoteDeploymentQueue(ctx, environmentUUID); err != nil {
				s.Logger.Warn("deployment queue promotion failed; the sweeper retries", zap.String("environment_uuid", environmentUUID.String()), zap.Error(err))
			}
			cancel()
			kicks.mu.Lock()
			if !kicks.pending[environmentUUID] {
				delete(kicks.pending, environmentUUID)
				kicks.mu.Unlock()
				return
			}
			kicks.pending[environmentUUID] = false
			kicks.mu.Unlock()
		}
	}()
}

// WaitDeploymentQueueKicks waits for background promotions to finish.
func (s *Server) WaitDeploymentQueueKicks() {
	s.queueKicks.running.Wait()
}

// SweepDeploymentQueue expires waiting items past their TTL, prunes settled
// history and retries queue heads whose backoff ended. Events normally promote
// queues; the sweeper covers lost events, lease expiry and process restarts.
func (s *Server) SweepDeploymentQueue(ctx context.Context) error {
	var changed, due []uuid.UUID
	if err := s.inTx(ctx, func(tx model.Tx) error {
		var err error
		if changed, err = model.ExpireDeploymentQueueItems(ctx, tx, deploymentQueueSweepBatch); err != nil {
			return err
		}
		_, err = model.PruneDeploymentQueue(ctx, tx, time.Now().Add(-deploymentQueueRetention))
		return err
	}); err != nil {
		return err
	}
	if err := s.inTx(ctx, func(tx model.Tx) error {
		var err error
		due, err = model.DueDeploymentQueueEnvironments(ctx, tx, deploymentQueueSweepBatch)
		return err
	}); err != nil {
		return err
	}
	seen := map[uuid.UUID]struct{}{}
	for _, environmentUUID := range append(changed, due...) {
		if _, duplicate := seen[environmentUUID]; duplicate {
			continue
		}
		seen[environmentUUID] = struct{}{}
		if err := s.PromoteDeploymentQueue(ctx, environmentUUID); err != nil {
			s.Logger.Warn("deployment queue promotion failed", zap.String("environment_uuid", environmentUUID.String()), zap.Error(err))
		}
	}
	return nil
}
