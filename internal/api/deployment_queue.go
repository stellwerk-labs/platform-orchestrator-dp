package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

// DeploymentQueueConfig bounds every Environment's deployment queue. Zero
// fields fall back to DefaultDeploymentQueueConfig.
type DeploymentQueueConfig struct {
	MaxDepth   int
	DefaultTTL time.Duration
	MaxTTL     time.Duration
}

const (
	defaultDeploymentQueueDepth = 10
	defaultDeploymentQueueTTL   = 2 * time.Hour
	maxDeploymentQueueTTL       = 24 * time.Hour
)

var DefaultDeploymentQueueConfig = DeploymentQueueConfig{MaxDepth: defaultDeploymentQueueDepth, DefaultTTL: defaultDeploymentQueueTTL, MaxTTL: maxDeploymentQueueTTL}

const maxQueueCancelReasonLength = 1000

func (s *Server) deploymentQueueConfig() DeploymentQueueConfig {
	config := s.DeploymentQueue
	if config.MaxDepth <= 0 {
		config.MaxDepth = DefaultDeploymentQueueConfig.MaxDepth
	}
	if config.DefaultTTL <= 0 {
		config.DefaultTTL = DefaultDeploymentQueueConfig.DefaultTTL
	}
	if config.MaxTTL <= 0 {
		config.MaxTTL = DefaultDeploymentQueueConfig.MaxTTL
	}
	return config
}

// deploymentRequest is a deploy or rollback request exactly as the client
// sent it. A queued request stores this intent, never a resolved graph, and
// is resolved like a new request when it reaches the head of its queue.
type deploymentRequest struct {
	Manifest                  *DeploymentManifest                 `json:"manifest,omitempty"`
	RollbackToDeploymentID    *uuid.UUID                          `json:"rollback_to_deployment_id,omitempty"`
	ModuleVersions            map[string]string                   `json:"module_versions,omitempty"`
	RestrictedConfirmations   []uuid.UUID                         `json:"confirm_restricted_module_version_uuids,omitempty"`
	EncryptedOutputsRecipient *string                             `json:"encrypted_outputs_recipient,omitempty"`
	EncryptedLogsRecipient    *string                             `json:"encrypted_logs_recipient,omitempty"`
	RunnerLogLevel            *DeploymentCreateBodyRunnerLogLevel `json:"runner_log_level,omitempty"`
}

func newDeploymentQueueItem(
	actor uuid.UUID, orgID, projectID, envID string, environmentUUID uuid.UUID, mode model.DeploymentMode,
	request deploymentRequest, idempotencyKey *string, expectedBase opt.Opt[uuid.UUID], ttl time.Duration,
) (model.NewDeploymentQueueItem, error) {
	intent, err := json.Marshal(request)
	if err != nil {
		return model.NewDeploymentQueueItem{}, errors.Wrap(err, "failed to encode deployment request")
	}
	item := model.NewDeploymentQueueItem{
		OrgID: orgID, ProjectID: projectID, EnvID: envID, EnvironmentUUID: environmentUUID, Mode: mode, Intent: intent,
		ExpectedBaseDeploymentID: expectedBase, CreatedBy: actor, TTL: ttl,
	}
	if idempotencyKey != nil {
		item.IdempotencyKeyDigest = opt.Of(digestIdempotencyKey(*idempotencyKey))
	}
	return item, nil
}

// findReplay looks for an earlier request on the same idempotency key within
// the idempotency window. It returns the queue item while that request has not
// become a Deployment, and refuses a different request on the key. When a
// Deployment carries the key, it reports deploymentReplay: that Deployment's
// own replay answers, also while it still runs and keeps the Environment busy.
func (s *Server) findReplay(ctx context.Context, request model.NewDeploymentQueueItem) (*model.DeploymentQueueItem, bool, error) {
	var replay *model.DeploymentQueueItem
	var deploymentReplay bool
	err := s.inTx(ctx, func(tx model.Tx) error {
		item, err := model.FindDeploymentQueueItemByIdempotencyKey(ctx, tx, request.OrgID, request.EnvironmentUUID, request.IdempotencyKeyDigest.Must())
		if _, missing := model.IsErrNotFound(err); missing {
			_, _, err = s.Database.GetDeploymentByIdempotencyKeyDigest(ctx, tx, request.OrgID, request.ProjectID, request.EnvID, request.IdempotencyKeyDigest.Must())
			if _, missing := model.IsErrNotFound(err); missing {
				return nil
			}
			deploymentReplay = err == nil
			return err
		} else if err != nil {
			return err
		}
		if item.IntentSHA256 != request.IntentSHA256() {
			return model.NewErrConflict("incorrect request for this idempotency key")
		}
		if item.State == model.DeploymentQueueDispatched {
			deploymentReplay = true
		} else {
			replay = item
		}
		return nil
	})
	return replay, deploymentReplay, err
}

func (s *Server) checkEnvironmentAvailable(ctx context.Context, request model.NewDeploymentQueueItem) error {
	return s.inTx(ctx, func(tx model.Tx) error {
		return model.CheckEnvironmentAvailable(ctx, tx, request.OrgID, request.ProjectID, request.EnvID, request.EnvironmentUUID, uuid.Nil)
	})
}

func (s *Server) enqueueDeployment(ctx context.Context, request model.NewDeploymentQueueItem) (CreateDeploymentResponseObject, error) {
	// Promotion re-checks the requester's permissions; a deprecated
	// organization token leaves no user whose authority could be re-checked.
	if organizationTokenUsed(ctx) {
		return CreateDeployment409JSONResponse{N409ConflictJSONResponse: Generate409FromModelErr(model.NewErrCodedConflict(model.CodeEnvironmentBusy,
			"the Environment is busy; queueing a Deployment requires an attributable user, not an organization token"))}, nil
	}
	var item *model.DeploymentQueueItem
	var replayed bool
	err := s.inTx(ctx, func(tx model.Tx) error {
		var err error
		item, replayed, err = model.EnqueueDeployment(ctx, tx, request, s.deploymentQueueConfig().MaxDepth)
		return err
	})
	if err != nil {
		return createDeploymentErrorResponse(err)
	}
	if !replayed {
		s.KickDeploymentQueue(request.EnvironmentUUID) //nolint:contextcheck // promotion must not inherit request authority
	}
	return CreateDeployment202JSONResponse(apiQueueItemFromModel(*item, false)), nil
}

func (s *Server) ListDeploymentQueueItems(ctx context.Context, request ListDeploymentQueueItemsRequestObject) (ListDeploymentQueueItemsResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgAuthorization(ctx, uid, request.OrgId, PermissionDeploymentRead); err != nil {
		return nil, err
	}
	if request.Params.EnvId != nil && request.Params.ProjectId == nil {
		return ListDeploymentQueueItems400JSONResponse{N400BadRequestJSONResponse: Generate400Response("project_id param required if env_id is specified")}, nil
	}
	params := model.ListDeploymentQueueParams{ProjectID: opt.OfRef(request.Params.ProjectId), EnvID: opt.OfRef(request.Params.EnvId)}
	if request.Params.State != nil {
		params.State = opt.Of(model.DeploymentQueueState(*request.Params.State))
	}
	perPage := min(opt.OfRef(request.Params.PerPage).Or(defaultPaginationSize), defaultPaginationSize)
	var items []model.DeploymentQueueItem
	var next string
	err := s.inTx(ctx, func(tx model.Tx) error {
		var err error
		items, next, err = model.ListDeploymentQueueItems(ctx, tx, request.OrgId, opt.OfRef(request.Params.Page).Or(""), perPage, params)
		return err
	})
	if e, ok := model.IsErrBadRequest(err); ok {
		return ListDeploymentQueueItems400JSONResponse{N400BadRequestJSONResponse: Generate400FromModelErr(e)}, nil
	} else if err != nil {
		return nil, err
	}
	page := DeploymentQueueItemPage{Items: make([]DeploymentQueueItem, len(items))}
	for index, item := range items {
		page.Items[index] = apiQueueItemFromModel(item, false)
	}
	if next != "" {
		page.NextPageToken = &next
	}
	return ListDeploymentQueueItems200JSONResponse(page), nil
}

func (s *Server) GetDeploymentQueueItem(ctx context.Context, request GetDeploymentQueueItemRequestObject) (GetDeploymentQueueItemResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgAuthorization(ctx, uid, request.OrgId, PermissionDeploymentRead); err != nil {
		return nil, err
	}
	var item *model.DeploymentQueueItem
	err := s.inTx(ctx, func(tx model.Tx) error {
		var err error
		item, err = model.GetDeploymentQueueItem(ctx, tx, request.OrgId, request.QueueItemId)
		return err
	})
	if e, ok := model.IsErrNotFound(err); ok {
		return GetDeploymentQueueItem404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
	} else if err != nil {
		return nil, err
	}
	return GetDeploymentQueueItem200JSONResponse(apiQueueItemFromModel(*item, true)), nil
}

// CancelDeploymentQueueItem lets any writer of the Environment withdraw
// obsolete waiting work; the item records who cancelled it and why.
func (s *Server) CancelDeploymentQueueItem(ctx context.Context, request CancelDeploymentQueueItemRequestObject) (CancelDeploymentQueueItemResponseObject, error) {
	uid, authErr := GetAuthenticatedUserIdOr401(ctx)
	if authErr != nil {
		return nil, authErr
	}
	reason := ""
	if request.Body != nil {
		reason = strings.TrimSpace(ref.DerefOr(request.Body.Reason, ""))
	}
	if utf8.RuneCountInString(reason) > maxQueueCancelReasonLength {
		return CancelDeploymentQueueItem400JSONResponse{N400BadRequestJSONResponse: Generate400Response(fmt.Sprintf("a reason must not exceed %d characters", maxQueueCancelReasonLength))}, nil
	}
	var item *model.DeploymentQueueItem
	err := s.inTx(ctx, func(tx model.Tx) error {
		var err error
		item, err = model.GetDeploymentQueueItem(ctx, tx, request.OrgId, request.QueueItemId)
		return err
	})
	if e, ok := model.IsErrNotFound(err); ok {
		middleware.SetAuthCheckedCtx(ctx)
		return CancelDeploymentQueueItem404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
	} else if err != nil {
		return nil, err
	}
	if err := s.checkEnvAuthorization(ctx, uid, request.OrgId, item.EnvironmentUUID, PermissionDeploymentWrite); err != nil {
		return nil, err
	}
	err = s.inTx(ctx, func(tx model.Tx) error {
		var err error
		item, _, err = model.CancelDeploymentQueueItem(ctx, tx, request.OrgId, request.QueueItemId, uid, reason)
		return err
	})
	if e, ok := model.IsErrNotFound(err); ok {
		return CancelDeploymentQueueItem404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
	} else if e, ok := model.IsErrConflict(err); ok {
		return CancelDeploymentQueueItem409JSONResponse{N409ConflictJSONResponse: Generate409FromModelErr(e)}, nil
	} else if err != nil {
		return nil, err
	}
	// The cancelled item may have been the head that held the rest of the queue.
	s.KickDeploymentQueue(item.EnvironmentUUID) //nolint:contextcheck // promotion must not inherit request authority
	return CancelDeploymentQueueItem200JSONResponse(apiQueueItemFromModel(*item, false)), nil
}

func apiQueueItemFromModel(item model.DeploymentQueueItem, withManifest bool) DeploymentQueueItem {
	var request deploymentRequest
	_ = json.Unmarshal(item.Intent, &request)
	out := DeploymentQueueItem{
		Id: item.ID, OrgId: item.OrgID, ProjectId: item.ProjectID, EnvId: item.EnvID,
		Mode: DeploymentQueueItemMode(item.Mode), State: DeploymentQueueItemState(item.State), Position: item.Position,
		RollbackToDeploymentId: request.RollbackToDeploymentID, ExpectedBaseDeploymentId: item.ExpectedBaseDeploymentID.Ref(),
		CreatedBy: item.CreatedBy, CreatedAt: item.CreatedAt, ExpiresAt: item.ExpiresAt, Attempts: item.Attempts,
		LastError: item.LastError.Ref(), ReasonCode: item.ReasonCode.Ref(), ReasonMessage: item.ReasonMessage.Ref(),
		SettledBy: item.SettledBy.Ref(), SettledAt: item.SettledAt.Ref(), DeploymentId: item.DeploymentID.Ref(),
	}
	if withManifest {
		out.Manifest = request.Manifest
	}
	return out
}

// inTx runs fn in one transaction and commits only when fn succeeds.
func (s *Server) inTx(ctx context.Context, fn func(model.Tx) error) error {
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return errors.Wrap(tx.Commit(), "failed to commit transaction")
}
