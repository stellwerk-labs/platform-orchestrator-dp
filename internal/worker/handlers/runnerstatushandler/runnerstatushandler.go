package runnerstatushandler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genevents"
)

const (
	MaxStatusCheckDuration = time.Hour
	RetryInterval          = 30 * time.Second
	MaxDeliveries          = int(MaxStatusCheckDuration/RetryInterval) + 2
)

type RunnerStatusHandler struct {
	db                 model.Databaser
	publisher          hmessaging.Publisher
	controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface
	runnerFactory      runners.RunnerFactory
}

func New(
	db model.Databaser,
	publisher hmessaging.Publisher,
	controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface,
	runnerFactory runners.RunnerFactory,
) *RunnerStatusHandler {
	return &RunnerStatusHandler{
		db:                 db,
		publisher:          publisher,
		controlPlaneClient: controlPlaneClient,
		runnerFactory:      runnerFactory,
	}
}

func (h *RunnerStatusHandler) Handle(ctx context.Context, logger *zap.Logger, delivery hmessaging.Delivery) error {
	var body events.CloudEvent[genevents.RunnerStatusCheckData]
	if err := json.Unmarshal(delivery.Data, &body); err != nil {
		return hmessaging.NewTerminalError(errors.Wrap(err, "failed to unmarshal runner status check event"))
	}

	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.OrgId = body.Data.OrgId
	ids.DeployId = body.Data.DeploymentId.String()
	ids.RunnerId = body.Data.RunnerId
	logger = hlogger.TraceScopedLoggerFromCtx(logger, ctx).WithLazy(ids.AsLogField())

	deployment, _, _, _, err := h.db.GetDeployment(ctx, nil, body.Data.OrgId, body.Data.DeploymentId, model.GetModeDefault)
	if err != nil {
		if _, ok := model.IsErrNotFound(err); ok {
			logger.Info("deployment no longer exists, stopping status checks")
			return nil
		}
		return hmessaging.NewRetryErrorWithDelay(errors.Wrap(err, "failed to get deployment"), RetryInterval)
	}
	if deployment.Status != model.DeploymentStatusExecuting {
		logger.Info("deployment no longer executing, stopping status checks", zap.String("status", string(deployment.Status)))
		return nil
	}
	if time.Now().After(deployment.CreatedAt.Add(MaxStatusCheckDuration)) {
		logger.Warn("max status check duration exceeded, failing deployment",
			zap.Time("deployment_created_at", deployment.CreatedAt),
			zap.Duration("max_duration", MaxStatusCheckDuration))
		return h.failDeployment(ctx, logger, deployment, "deployment timeout - runner did not complete within expected time")
	}

	response, err := h.controlPlaneClient.GetInternalRunnerWithResponse(ctx, body.Data.OrgId, body.Data.RunnerId)
	if err != nil {
		return hmessaging.NewRetryErrorWithDelay(errors.Wrap(err, "failed to get runner configuration"), RetryInterval)
	}
	if response.StatusCode() == http.StatusNotFound {
		return h.failDeployment(ctx, logger, deployment, fmt.Sprintf("no runner has been configured with id '%s'", body.Data.RunnerId))
	}
	if response.StatusCode() != http.StatusOK {
		return hmessaging.NewRetryErrorWithDelay(errors.Errorf(
			"unexpected status code when getting runner '%s': %s: %s",
			body.Data.RunnerId, response.Status(), string(response.Body)), RetryInterval)
	}

	runner, err := h.runnerFactory.CreateRunner(ctx, *response.JSON200, deployment)
	if err != nil {
		return h.failDeployment(ctx, logger, deployment, errors.Wrap(err, "failed to create runner").Error())
	}
	status, err := runner.CheckStatus(ctx)
	if err != nil {
		if errors.Is(err, runners.ErrKubernetesAgentNotReachableRetry) {
			return hmessaging.NewRetryErrorWithDelay(errors.Wrap(err, "kubernetes agent temporarily not reachable"), RetryInterval)
		}
		return h.failDeployment(ctx, logger, deployment, errors.Wrap(err, "failed to check runner status").Error())
	}

	logger = logger.With(
		zap.Bool("completed", status.IsCompleted),
		zap.Bool("stuck", status.IsStuck),
		zap.String("message", status.Message),
	)
	if status.IsStuck {
		return h.failDeployment(ctx, logger, deployment, status.Message)
	}
	if status.IsCompleted {
		logger.Info("runner completed, waiting for its result event if the deployment is still executing")
		return nil
	}
	return hmessaging.NewRetryErrorWithDelay(errors.New("runner still running"), RetryInterval)
}

func (h *RunnerStatusHandler) failDeployment(ctx context.Context, logger *zap.Logger, deployment *model.DeploymentSummary, statusMessage string) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			logger.Error("failed to rollback transaction", zap.Error(rollbackErr))
		}
	}()

	deployment, err = h.db.UpdateDeploymentStatusAndOutputs(ctx, tx, deployment.Id, model.UpdateDeploymentStatusAndOutputsParams{
		Status:           model.DeploymentStatusFailed,
		StatusMessage:    statusMessage,
		Metrics:          deployment.Metrics,
		ExpectedRevision: opt.Of(int64(deployment.Revision)),
	})
	if err != nil {
		return errors.Wrap(err, "failed to update deployment status")
	}
	if err := h.db.CreateDeploymentHistoryRecord(ctx, tx, deployment); err != nil {
		return errors.Wrap(err, "failed to create deployment history record")
	}
	messages, err := h.db.InsertPendingEventMessages(ctx, tx, []*hstandardoutbox.PendingEventMessage{{
		Subject: string(genevents.IoPlatformOrchestratorDeploymentUpdated),
		Payload: model.ConvertDeploymentToEventPayload(deployment),
	}})
	if err != nil {
		return errors.Wrap(err, "failed to insert pending event messages")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}
	logger.Info("deployment completed", zap.String("status", string(model.DeploymentStatusFailed)), zap.String("status_message", statusMessage))
	reliableoutbox.OptimisticPublish(ctx, logger, h.db.AsReliableOutboxStore(), h.publisher, messages)
	return nil
}
