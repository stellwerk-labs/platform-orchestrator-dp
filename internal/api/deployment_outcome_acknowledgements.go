package api

import (
	"context"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/herrors"
	"github.com/stellwerk-labs/golib/hlogger"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const maxOutcomeAcknowledgementReasonLength = 1000

// InternalAcknowledgeDeploymentOutcome is the audited operator path out of an
// Environment fenced by a failed plugin-dispatched Deployment whose Runner
// outcome is unknown. The acting user becomes the audit record, so neither the
// system user nor a deprecated organization token may use it.
func (s *Server) InternalAcknowledgeDeploymentOutcome(ctx context.Context, request InternalAcknowledgeDeploymentOutcomeRequestObject) (InternalAcknowledgeDeploymentOutcomeResponseObject, error) {
	uid, authErr := GetAuthenticatedUserIdOr401(ctx)
	if authErr != nil {
		return nil, authErr
	}
	if uid == userid.InternalSystemUuid || organizationTokenUsed(ctx) {
		return nil, &herrors.PlatformOrchestratorError{StatusCode: http.StatusForbidden, Code: "HTTP-403", Message: "acknowledging a Deployment outcome requires an attributable user"}
	}
	reason := ""
	if request.Body != nil {
		reason = strings.TrimSpace(request.Body.Reason)
	}
	if reason == "" || utf8.RuneCountInString(reason) > maxOutcomeAcknowledgementReasonLength {
		return InternalAcknowledgeDeploymentOutcome400JSONResponse{N400BadRequestJSONResponse: Generate400Response("a reason of 1 to 1000 characters is required")}, nil
	}

	deployment, _, _, _, err := s.Database.GetDeployment(ctx, nil, request.OrgId, request.DeploymentId, model.GetModeDefault)
	if err != nil {
		if e, ok := model.IsErrNotFound(err); ok {
			middleware.SetAuthCheckedCtx(ctx)
			return InternalAcknowledgeDeploymentOutcome404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to get deployment")
	}
	if err := s.checkEnvAuthorization(ctx, uid, request.OrgId, deployment.DeploymentEnvUuid, PermissionDeploymentOutcomeAcknowledge); err != nil {
		return nil, err
	}

	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
	}
	defer func() { _ = tx.Rollback() }()
	ack, replayed, err := model.AcknowledgeDeploymentOutcome(ctx, tx, request.OrgId, request.DeploymentId, uid, reason, time.Now())
	if err != nil {
		if e, ok := model.IsErrNotFound(err); ok {
			return InternalAcknowledgeDeploymentOutcome404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
		}
		if e, ok := model.IsErrConflict(err); ok {
			return InternalAcknowledgeDeploymentOutcome409JSONResponse{N409ConflictJSONResponse: Generate409FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to acknowledge deployment outcome")
	}
	if err := tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "failed to commit deployment outcome acknowledgement")
	}

	response := DeploymentOutcomeAcknowledgement{
		DeploymentId: ack.DeploymentID, EnvironmentId: ack.EnvironmentID, PluginId: ack.PluginID, OperationRef: ack.OperationRef,
		ActorId: ack.ActorID, Reason: ack.Reason, AcknowledgedAt: ack.AcknowledgedAt, Replayed: replayed,
	}
	if replayed {
		return InternalAcknowledgeDeploymentOutcome200JSONResponse(response), nil
	}
	hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).Info("acknowledged unreconciled Deployment outcome",
		zap.String("org_id", ack.OrgID), zap.String("deployment_id", ack.DeploymentID.String()),
		zap.String("environment_id", ack.EnvironmentID.String()), zap.String("plugin_id", ack.PluginID),
		zap.String("actor_id", ack.ActorID.String()))
	return InternalAcknowledgeDeploymentOutcome201JSONResponse(response), nil
}

func organizationTokenUsed(ctx context.Context) bool {
	c := middleware.GetEchoCtx(ctx)
	return c != nil && c.Request().Header.Get("Po-Org-Id") != ""
}
