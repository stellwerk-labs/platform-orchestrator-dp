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
	"github.com/stellwerk-labs/golib/hrabbitmq"
	v2 "github.com/stellwerk-labs/golib/hrabbitmq/delayqueues/v2"
	"github.com/stellwerk-labs/golib/hrabbitmq/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardreliableoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/wagslane/go-rabbitmq"
	"go.uber.org/zap"

	usererrors "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/genevents"
)

const (
	MaxStatusCheckDuration = 1 * time.Hour
	RetryInterval          = 30 * time.Second
)

type RunnerStatusHandler struct {
	db                 model.Databaser
	publisher          hrabbitmq.Publisher
	controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface
	runnerFactory      runners.RunnerFactory
}

func New(
	db model.Databaser,
	publisher hrabbitmq.Publisher,
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

func (h *RunnerStatusHandler) Handle(ctx context.Context, logger *zap.Logger, d *rabbitmq.Delivery) error {
	var body events.CloudEvent[genevents.RunnerStatusCheckData]
	if err := json.Unmarshal(d.Body, &body); err != nil {
		return errors.Wrap(err, "failed to unmarshal runner status check event")
	}

	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.OrgId = body.Data.OrgId
	ids.DeployId = body.Data.DeploymentId.String()
	ids.RunnerId = body.Data.RunnerId
	logger = hlogger.TraceScopedLoggerFromCtx(logger, ctx).WithLazy(ids.AsLogField())

	// Get current deployment
	deployment, _, _, _, err := h.db.GetDeployment(ctx, nil, body.Data.OrgId, body.Data.DeploymentId, model.GetModeDefault)
	if err != nil {
		if _, ok := model.IsErrNotFound(err); ok {
			logger.Info("deployment no longer exists, stopping status checks")
			return nil
		}
		return errors.Wrap(err, "failed to get deployment")
	}

	// Skip if deployment is no longer executing
	if deployment.Status != model.DeploymentStatusExecuting {
		logger.Info("deployment no longer executing, stopping status checks", zap.String("status", string(deployment.Status)))
		return nil
	}

	// Check if we've exceeded the maximum status check duration
	if time.Now().After(deployment.CreatedAt.Add(MaxStatusCheckDuration)) {
		logger.Warn("max status check duration exceeded, failing deployment",
			zap.Time("deployment_created_at", deployment.CreatedAt),
			zap.Duration("max_duration", MaxStatusCheckDuration))
		return h.failDeployment(ctx, logger, deployment, "deployment timeout - runner did not complete within expected time")
	}

	// Get runner configuration
	res, err := h.controlPlaneClient.GetInternalRunnerWithResponse(ctx, body.Data.OrgId, body.Data.RunnerId)
	if err != nil {
		return errors.Wrap(err, "failed to get runner configuration")
	} else if res.StatusCode() == http.StatusNotFound {
		return usererrors.NewUserError(fmt.Sprintf("no runner has been configured with id '%s'", body.Data.RunnerId))
	} else if res.StatusCode() != http.StatusOK {
		return errors.Errorf("unexpected status code when getting runner '%s': %s: %s", body.Data.RunnerId, res.Status(), string(res.Body))
	}

	// Create runner instance
	rn := *res.JSON200
	runner, err := h.runnerFactory.CreateRunner(ctx, rn, deployment)
	if err != nil {
		return errors.Wrap(err, "failed to create runner")
	}

	// Check runner status
	status, err := runner.CheckStatus(ctx)
	if err != nil {
		if errors.Is(err, runners.ErrKubernetesAgentNotReachableRetry) {
			logger.Warn("kubernetes agent temporary not reachable, will retry", zap.Error(err))
			return v2.NewGracefulRetryErrorWithDelay(errors.Wrap(err, "kubernetes agent temporary not reachable"), RetryInterval)
		}
		return h.failDeployment(ctx, logger, deployment, errors.Wrap(err, "failed to check runner status").Error())
	}

	logger = logger.With(
		zap.Bool("completed", status.IsCompleted),
		zap.Bool("stuck", status.IsStuck),
		zap.String("message", status.Message),
	)

	// Handle scenario of runner stuck
	if status.IsStuck {
		return h.failDeployment(ctx, logger, deployment, status.Message)
	} else if status.IsCompleted {
		logger.Info("runner completed successfully, deployment should be updated by runner itself shortly if not already")
		return nil
	}

	// Schedule next check - runner is still running, time check already done above
	return v2.NewGracefulRetryErrorWithDelay(
		errors.New("runner still running, scheduling next status check"),
		RetryInterval,
	)
}

func (h *RunnerStatusHandler) failDeployment(ctx context.Context, logger *zap.Logger, dep *model.DeploymentSummary, statusMessage string) error {
	if tx, err := h.db.BeginTx(ctx, nil); err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	} else {
		defer func() {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				logger.Error("failed to rollback transaction", zap.Error(err))
			}
		}()

		if dep, err = h.db.UpdateDeploymentStatusAndOutputs(ctx, tx, dep.Id, model.UpdateDeploymentStatusAndOutputsParams{
			Status:           model.DeploymentStatusFailed,
			StatusMessage:    statusMessage,
			Metrics:          dep.Metrics,
			ExpectedRevision: opt.Of(int64(dep.Revision)),
		}); err != nil {
			return errors.Wrap(err, "failed to update deployment status")
		}

		if err := h.db.CreateDeploymentHistoryRecord(ctx, tx, dep); err != nil {
			return errors.Wrap(err, "failed to create deployment history record")
		}

		messages, err := h.db.InsertPendingEventMessages(ctx, tx, []*hstandardreliableoutbox.PendingEventMessage{
			{
				Exchange:   events.DefaultExchange,
				RoutingKey: string(genevents.IoPlatformOrchestratorDeploymentUpdated),
				Payload:    model.ConvertDeploymentToEventPayload(dep),
			},
		})
		if err != nil {
			return errors.Wrap(err, "failed to insert pending event messages")
		}
		if err := tx.Commit(); err != nil {
			return errors.Wrap(err, "failed to commit transaction")
		}
		reliableoutbox.OptimisticPublish(ctx, logger, h.db.AsReliableOutboxStore(), h.publisher, messages)
		return nil
	}
}
