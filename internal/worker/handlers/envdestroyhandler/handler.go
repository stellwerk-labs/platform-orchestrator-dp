package envdestroyhandler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	cpevents "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genevents"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/logging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers"

	dpevents "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genevents"
)

// BranchPattern is the branch match pattern for incoming events.
var BranchPattern = regexp.MustCompile(regexp.QuoteMeta(string(cpevents.IoPlatformOrchestratorEnvironmentUpdated)) +
	"|" + regexp.QuoteMeta(string(dpevents.IoPlatformOrchestratorDeploymentUpdated)))

// EnvDestroyHandler handles events indicating that the env needs to be destroyed, has been destroyed, or has failed to
// be destroyed. See BranchPattern for the routing key matches: env.updated (status=deleting), deploy.updated (status=failed/succeeded).
// If the env is deleting, we want to check if the env has state, if it does, then we destroy it, if it doesn't, we force delete
// the env from the CP. If the destroy fails, we update the CP env to destroy_failed so the user can retrigger it later.
type EnvDestroyHandler struct {
	cpClient platformorchestratorcp.ClientWithResponsesInterface
	db       model.Databaser
	pub      hmessaging.Publisher
	logDel   runners.RunnerLogsDeleter
}

// New is the constructor so that we don't miss new arguments.
func New(cpClient platformorchestratorcp.ClientWithResponsesInterface, db model.Databaser, pub hmessaging.Publisher, logDel runners.RunnerLogsDeleter) *EnvDestroyHandler {
	return &EnvDestroyHandler{
		cpClient: cpClient,
		db:       db,
		pub:      pub,
		logDel:   logDel,
	}
}

// Handle is the entrypoint for new messages. Here we unwrap the typed data and call handleInner if it applies.
func (h *EnvDestroyHandler) Handle(ctx context.Context, logger *zap.Logger, d hmessaging.Delivery) error {
	switch d.Subject {
	case string(cpevents.IoPlatformOrchestratorEnvironmentUpdated):
		var e events.CloudEvent[cpevents.EnvChangedData]
		if err := json.Unmarshal(d.Data, &e); err != nil {
			return errors.Wrap(err, "failed to unmarshal event body")
		}
		force := ref.DerefOr(e.Data.Force, false)
		return h.handleInner(ctx, logger, e.Data.OrgId, e.Data.ProjectId, e.Data.EnvId, e.Data.EnvUuid, force, ref.DerefOr(e.Data.DeleteRules, false))
	case string(dpevents.IoPlatformOrchestratorDeploymentCreated):
		fallthrough
	case string(dpevents.IoPlatformOrchestratorDeploymentUpdated):
		var e events.CloudEvent[dpevents.DeploymentChangedData]
		if err := json.Unmarshal(d.Data, &e); err != nil {
			return errors.Wrap(err, "failed to unmarshal event body")
		}
		return h.handleInner(ctx, logger, e.Data.OrgId, e.Data.ProjectId, e.Data.EnvId, e.Data.EnvUuid, false, false)
	default:
		return nil
	}
}

// updateEnvStatus is a helper we use a few times to update the status/message on an env.
func (h *EnvDestroyHandler) updateEnvStatus(ctx context.Context, logger *zap.Logger, orgId, projectId, envId string, body platformorchestratorcp.EnvironmentInternalUpdateBody) error {
	if r, err := h.cpClient.InternalUpdateEnvironmentWithResponse(ctx, orgId, projectId, envId, body); err != nil {
		return errors.Wrap(err, "failed to update environment")
	} else if r.StatusCode() != http.StatusOK {
		return errors.Errorf("unexpected status code when updating environment: %s: %s", r.Status(), string(r.Body))
	} else {
		logger.Info("updated status on environment", zap.Any("update", body))
		return nil
	}
}

func (h *EnvDestroyHandler) handleInner(ctx context.Context, logger *zap.Logger, orgId, projectId, envId string, envUuid uuid.UUID, force, deleteRules bool) error {
	logger = logger.With(logging.ZapOrgId(orgId), logging.ZapProjectId(projectId), logging.ZapEnvId(envId))

	// first step is to check if the env is deleting, if it isn't or doesn't exist anymore we can ignore it here
	var env platformorchestratorcp.Environment
	if r, err := h.cpClient.GetEnvironmentWithResponse(ctx, orgId, projectId, envId); err != nil {
		return errors.Wrap(err, "failed to get environment")
	} else if r.StatusCode() == http.StatusNotFound {
		logger.Info("environment is gone")
		return nil
	} else if r.StatusCode() != http.StatusOK {
		return errors.Errorf("unexpected status code when getting environment: %s: %s", r.Status(), string(r.Body))
	} else {
		env = *r.JSON200
		if env.Uuid != envUuid {
			logger.Info("environment uuid mismatch", zap.String("uuid", envUuid.String()), zap.String("cp_uuid", env.Uuid.String()))
			return nil
		}
		if env.Status != platformorchestratorcp.EnvironmentStatusDeleting {
			logger.Debug("environment is not deleting", zap.String("status", string(env.Status)))
			return nil
		}
	}

	// at this point we know the CP state indicates this env should be cleaned up, so we need to check our own DP state
	// from inside a transaction.
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			logger.Error("failed to rollback transaction", zap.Error(err))
		}
	}()

	isDestroyed, dep, err := h.isEnvironmentDestroyed(ctx, tx, logger, orgId, projectId, envId)
	if err != nil {
		return errors.Wrap(err, "failed to check if environment is destroyed")
	}

	if force || isDestroyed {
		logger = logger.With(logging.ZapDeploymentId(dep.Id.String()))
		if force {
			logger.Info("force deleting deployments and cp env")
		} else {
			logger.Info("environment has been destroyed - deleting deployments and cp env")
		}
		// once we're done, we can delete all the data and then force delete the env from the CP
		if err := h.db.DeleteDeploymentsForEnv(ctx, tx, orgId, projectId, envId, force); err != nil {
			return errors.Wrap(err, "failed to delete deployments for env")
		}

		// commit the deployments cleanup here
		if err := tx.Commit(); err != nil {
			return errors.Wrap(err, "failed to commit transaction")
		}

		if err := h.logDel(ctx, envUuid.String()); err != nil {
			logger.Warn("failed to delete deployment logs", zap.Error(err))
			// we don't return error here because we want to proceed with env deletion even if logs deletion fails
		}

		if r, err := h.cpClient.InternalForceDeleteEnvironmentWithResponse(ctx, orgId, projectId, envId, &platformorchestratorcp.InternalForceDeleteEnvironmentParams{DeleteRules: &deleteRules}); err != nil {
			return errors.Wrap(err, "failed to force delete environment")
		} else if r.StatusCode() != http.StatusNoContent {
			return errors.Errorf("unexpected status code when force deleting environment: %s: %s", r.Status(), string(r.Body))
		} else {
			logger.Info("environment has been deleted from the cp")
			return nil
		}
	}

	if dep.Status == model.DeploymentStatusExecuting {
		logger.Info("deployment is still executing, will wait for deployment completed event", logging.ZapDeploymentId(dep.Id.String()))
		return nil
	}

	if dep.Mode == model.DeploymentModeDestroy && strings.Contains(ref.DerefOr(env.StatusMessage, ""), dep.Id.String()) {
		logger.Info("destroy deployment has failed", logging.ZapDeploymentId(dep.Id.String()))
		return h.updateEnvStatus(ctx, logger, orgId, projectId, envId, platformorchestratorcp.InternalUpdateEnvironmentJSONRequestBody{
			Status:        ref.Ref(platformorchestratorcp.EnvironmentStatusDeleteFailed),
			StatusMessage: ref.Ref(fmt.Sprintf("destroy deployment '%s' failed. Inspect the deployment status for a cause and retry the DeleteEnvironment operation", dep.Id)),
		})
	}

	logger.Info("launching destroy deployment")
	// TODO: implement encrypted logs logic also on destroy
	newDep, messages, _, err := api.CreateDeployment(
		ctx, logger, userid.InternalSystemUuid, orgId, projectId, envId, env, model.DeploymentModeDestroy, opt.Empty[uuid.UUID](),
		api.DeploymentManifest{Workloads: map[string]api.DeploymentManifestWorkload{}}, nil, nil, nil, false, nil,
		h.cpClient, h.db, tx,
	)
	if err != nil {
		if me, ok := model.IsErrBadRequest(err); ok {
			return h.updateEnvStatus(ctx, logger, orgId, projectId, envId, platformorchestratorcp.InternalUpdateEnvironmentJSONRequestBody{
				Status:        ref.Ref(platformorchestratorcp.EnvironmentStatusDeleteFailed),
				StatusMessage: ref.Ref(fmt.Sprintf("failed to start destroy deployment: %s", me.Message)),
			})
		} else if me, ok := model.IsErrConflict(err); ok {
			return h.updateEnvStatus(ctx, logger, orgId, projectId, envId, platformorchestratorcp.InternalUpdateEnvironmentJSONRequestBody{
				Status:        ref.Ref(platformorchestratorcp.EnvironmentStatusDeleteFailed),
				StatusMessage: ref.Ref(fmt.Sprintf("failed to start destroy deployment: %s", me.Message)),
			})
		}
		return errors.Wrap(err, "failed to create deployment")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}
	logger = logger.With(logging.ZapDeploymentId(newDep.Id.String()))
	logger.Info("created destroy deployment")

	reliableoutbox.OptimisticPublish(ctx, logger, h.db.AsReliableOutboxStore(), h.pub, messages)
	return h.updateEnvStatus(ctx, logger, orgId, projectId, envId, platformorchestratorcp.InternalUpdateEnvironmentJSONRequestBody{
		Status:        ref.Ref(platformorchestratorcp.EnvironmentStatusDeleting),
		StatusMessage: ref.Ref(fmt.Sprintf("Waiting for destroy deployment '%s' to complete", newDep.Id)),
	})
}

func (h *EnvDestroyHandler) isEnvironmentDestroyed(ctx context.Context, tx model.Tx, logger *zap.Logger, orgId, projectId, envId string) (bool, model.DeploymentSummary, error) {
	var dep model.DeploymentSummary
	page, _, err := h.db.ListLastDeployments(ctx, tx, orgId, "", 1, model.ListLastDeploymentsParams{
		ProjectId:       opt.Of(projectId),
		EnvId:           opt.Of(envId),
		StateChangeOnly: true,
	})
	if err != nil {
		if _, ok := model.IsErrNotFound(err); ok {
			logger.Info("environment has never been deployed")
			return true, dep, nil
		}
		return false, dep, errors.Wrap(err, "failed to list last deployments of deleting env")
	}

	if len(page) == 0 || page[0].Mode == model.DeploymentModeDeployPlan {
		logger.Info("environment has no stateful deployment")
		return true, dep, nil
	}

	dep = page[0]
	if dep.Mode == model.DeploymentModeDestroy && dep.Status == model.DeploymentStatusSucceeded {
		logger.Info("the last deployment in the environment is a successful destroy", logging.ZapDeploymentId(dep.Id.String()))
		return true, dep, nil
	}

	logger.Info("the last deployment in the environment is not a successful destroy", logging.ZapDeploymentId(dep.Id.String()))
	return false, dep, nil
}

var _ handlers.Handler = (*EnvDestroyHandler)(nil)
