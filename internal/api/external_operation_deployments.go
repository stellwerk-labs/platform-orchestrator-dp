package api

import (
	"context"
	"database/sql"
	"slices"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/plugincontext"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

const (
	ExternalDeploymentAudience       = "platform-orchestrator-dp"
	ExternalDeploymentAction         = "external-deployment.admit"
	ExternalDeploymentRollbackAction = "external-deployment.rollback"
)

// ExternalDeploymentGrantRequest contains the resource identity supplied by a
// plugin. Its operation reference and fence are untrusted until Core returns an
// exact authorization from ExternalDeploymentGrantAuthorizer.
type ExternalDeploymentGrantRequest struct {
	OperationRef  string
	Fence         int64
	ServiceAction string
	DispatchID    string
	RequestDigest string
	ActorID       string
	ProjectUUID   uuid.UUID
	EnvUUID       uuid.UUID
	ProjectID     string
	EnvID         string
}

// AuthorizedExternalDeployment is the authoritative resource operation and
// fence returned by Core after checking the plugin service grant, delegated
// actor, organization and body-bound request.
type AuthorizedExternalDeployment struct {
	OperationRef  string
	Fence         int64
	ServiceAction string
	DispatchID    string
	RequestDigest string
	ActorID       string
	ProjectUUID   uuid.UUID
	EnvUUID       uuid.UUID
}

type ExternalDeploymentGrantAuthorizer interface {
	AuthorizeExternalDeployment(context.Context, plugincontext.Claims, ExternalDeploymentGrantRequest) (AuthorizedExternalDeployment, error)
}

// ExternalDeploymentCreator must invoke the ordinary deployment path inside
// the supplied transaction. It is intentionally not a second deployment
// engine, and the service verifies the returned Deployment's owner fields.
type ExternalDeploymentCreation struct {
	Deployment                  *model.DeploymentSummary
	PendingEvents               []*hstandardoutbox.PendingEventMessage
	PlanSnapshotID              opt.Opt[uuid.UUID]
	BaseDeploymentID            opt.Opt[uuid.UUID]
	ForwardDeploymentID         opt.Opt[uuid.UUID]
	RetryOfRollbackDeploymentID opt.Opt[uuid.UUID]
}

type ExternalDeploymentCreator func(context.Context, model.Tx, uuid.UUID) (*ExternalDeploymentCreation, error)

type ExternalDeploymentAdmissionRequest struct {
	Claims        plugincontext.Claims
	OperationRef  string
	Fence         int64
	ServiceAction string
	ProjectUUID   uuid.UUID
	EnvUUID       uuid.UUID
	ProjectID     string
	EnvID         string
	Create        ExternalDeploymentCreator
}

type ExternalDeploymentAdmissionResult struct {
	Deployment    *model.DeploymentSummary
	PendingEvents []*hstandardoutbox.PendingEventMessage
	Replayed      bool
}

// ExternalDeploymentAdmissionService is an unrouted application boundary. An
// HTTP adapter may call it only after independently verifying a DP-audience
// assertion for the exact request. The service locks the durable dispatch key
// before reauthorizing the concrete Core operation/fence. This ordering lets a
// post-revocation receipt read wait until an already-admitted transaction has
// either committed its receipt or rolled back without an effect.
type ExternalDeploymentAdmissionService struct {
	Database   model.Databaser
	Authorizer ExternalDeploymentGrantAuthorizer
	Logger     *zap.Logger
}

func (s ExternalDeploymentAdmissionService) Admit(ctx context.Context, request ExternalDeploymentAdmissionRequest) (*ExternalDeploymentAdmissionResult, error) {
	if err := validateExternalDeploymentAdmission(request); err != nil {
		return nil, err
	}
	if s.Database == nil || s.Authorizer == nil {
		return nil, errors.New("external deployment admission service is not configured")
	}
	actorID, err := uuid.Parse(request.Claims.ActorID)
	if err != nil {
		return nil, model.NewErrBadRequest("external deployment actor is not a UUID")
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin external deployment transaction")
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) && s.Logger != nil {
			s.Logger.Error("failed to roll back external deployment transaction", zap.Error(rollbackErr))
		}
	}()

	key := model.ExternalOperationDeploymentKey{
		OrgID:     request.Claims.OrgID,
		PluginID:  request.Claims.PluginID,
		RequestID: request.Claims.RequestID,
	}
	receipt, receiptErr := model.GetExternalOperationDeploymentReceipt(ctx, tx, key)
	if receiptErr != nil {
		if _, missing := model.IsErrNotFound(receiptErr); !missing {
			return nil, errors.Wrap(receiptErr, "failed to inspect external deployment receipt")
		}
	}
	authorized, err := s.Authorizer.AuthorizeExternalDeployment(ctx, request.Claims, ExternalDeploymentGrantRequest{
		OperationRef:  request.OperationRef,
		Fence:         request.Fence,
		ServiceAction: request.ServiceAction,
		DispatchID:    request.Claims.RequestID,
		RequestDigest: request.Claims.BodySHA256,
		ActorID:       request.Claims.ActorID,
		ProjectUUID:   request.ProjectUUID,
		EnvUUID:       request.EnvUUID,
		ProjectID:     request.ProjectID,
		EnvID:         request.EnvID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "external deployment operation was not authorized")
	}
	if !externalDeploymentAuthorizationMatches(authorized, request) {
		return nil, model.NewErrConflict("Core authorized a different external deployment intent")
	}
	if receiptErr == nil {
		if !externalDeploymentReceiptMatches(receipt, request, actorID, authorized) {
			return nil, model.NewErrConflict("external deployment request identity was reused with different inputs")
		}
		deployment, _, _, _, err := s.Database.GetDeployment(ctx, tx, request.Claims.OrgID, receipt.DeploymentID, model.GetModeDefault)
		if err != nil {
			if _, missing := model.IsErrNotFound(err); missing {
				return nil, model.NewErrConflict("external deployment receipt exists but its deployment is no longer retained")
			}
			return nil, errors.Wrap(err, "failed to load external deployment receipt target")
		}
		if err := tx.Commit(); err != nil {
			return nil, errors.Wrap(err, "failed to commit external deployment replay")
		}
		return &ExternalDeploymentAdmissionResult{Deployment: deployment, Replayed: true}, nil
	}

	creation, err := request.Create(ctx, tx, actorID)
	if err != nil {
		return nil, err
	}
	if creation == nil {
		return nil, errors.New("normal deployment creator returned no result")
	}
	deployment := creation.Deployment
	if deployment == nil || deployment.OrgId != request.Claims.OrgID || deployment.ProjectId != request.ProjectID ||
		deployment.EnvId != request.EnvID || deployment.CreatedBy != actorID {
		return nil, errors.New("normal deployment creator returned mismatched ownership")
	}
	if _, err := model.CreateExternalOperationDeploymentReceipt(ctx, tx, model.ExternalOperationDeploymentReceipt{
		ExternalOperationDeploymentKey: key,
		OperationRef:                   authorized.OperationRef,
		Fence:                          authorized.Fence,
		AssertionAction:                request.Claims.OperationID,
		ServiceAction:                  request.ServiceAction,
		RequestBodySHA256:              request.Claims.BodySHA256,
		ActorID:                        request.Claims.ActorID,
		ProjectID:                      request.ProjectID,
		EnvID:                          request.EnvID,
		ProjectUUID:                    opt.Of(request.ProjectUUID),
		EnvironmentUUID:                opt.Of(request.EnvUUID),
		PlanSnapshotID:                 creation.PlanSnapshotID,
		BaseDeploymentID:               creation.BaseDeploymentID,
		ForwardDeploymentID:            creation.ForwardDeploymentID,
		RetryOfRollbackDeploymentID:    creation.RetryOfRollbackDeploymentID,
		DeploymentID:                   deployment.Id,
	}); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "failed to commit external deployment")
	}
	return &ExternalDeploymentAdmissionResult{
		Deployment:    deployment,
		PendingEvents: slices.Clone(creation.PendingEvents),
	}, nil
}

func validateExternalDeploymentAdmission(request ExternalDeploymentAdmissionRequest) error {
	if request.Claims.Audience != ExternalDeploymentAudience || request.Claims.OperationID != ExternalDeploymentAction {
		return model.NewErrBadRequest("invalid external deployment audience or action")
	}
	if request.Claims.OrgID == "" || request.Claims.PluginID == "" || request.Claims.ActorID == "" ||
		request.Claims.RequestID == "" || request.Claims.BodySHA256 == "" || request.OperationRef == "" ||
		request.Fence <= 0 || request.ServiceAction == "" || request.ProjectUUID == uuid.Nil || request.EnvUUID == uuid.Nil ||
		request.ProjectID == "" || request.EnvID == "" || request.Create == nil {
		return model.NewErrBadRequest("external deployment admission is incomplete")
	}
	if request.ServiceAction != ExternalDeploymentAction && request.ServiceAction != ExternalDeploymentRollbackAction {
		return model.NewErrBadRequest("invalid external deployment service action")
	}
	return nil
}

func externalDeploymentReceiptMatches(receipt *model.ExternalOperationDeploymentReceipt, request ExternalDeploymentAdmissionRequest, actorID uuid.UUID, authorized AuthorizedExternalDeployment) bool {
	return receipt.OperationRef == authorized.OperationRef &&
		receipt.Fence == authorized.Fence &&
		receipt.AssertionAction == request.Claims.OperationID &&
		receipt.ServiceAction == request.ServiceAction &&
		receipt.RequestBodySHA256 == request.Claims.BodySHA256 &&
		receipt.ActorID == actorID.String() &&
		receipt.ProjectID == request.ProjectID &&
		receipt.EnvID == request.EnvID &&
		receipt.ProjectUUID.IsSet() && receipt.ProjectUUID.Must() == request.ProjectUUID &&
		receipt.EnvironmentUUID.IsSet() && receipt.EnvironmentUUID.Must() == request.EnvUUID
}

func externalDeploymentAuthorizationMatches(authorized AuthorizedExternalDeployment, request ExternalDeploymentAdmissionRequest) bool {
	return authorized.OperationRef == request.OperationRef &&
		authorized.Fence == request.Fence &&
		authorized.ServiceAction == request.ServiceAction &&
		authorized.DispatchID == request.Claims.RequestID &&
		authorized.RequestDigest == request.Claims.BodySHA256 &&
		authorized.ActorID == request.Claims.ActorID &&
		authorized.ProjectUUID == request.ProjectUUID &&
		authorized.EnvUUID == request.EnvUUID
}
