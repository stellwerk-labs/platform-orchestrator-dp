package api

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
	"github.com/stellwerk-labs/golib/hrabbitmq/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardreliableoutbox"
	"github.com/stellwerk-labs/golib/htelemetry"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/logging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/completionhooks"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/genevents"
)

const (
	defaultPaginationSize = 100
	headerMode            = 0644
	headerDirectoryMode   = 0755

	// Destroy is an internal mode - not documented on the API
	Destroy DeploymentCreateBodyMode = "destroy"

	providerFullIdentifierExpectedLength = 2
)

var stripIdentifierPattern = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

func apiModeAndPlanFromModel(m model.DeploymentMode) (string, bool) {
	switch m {
	case model.DeploymentModeDeploy:
		return string(DeploymentCreateBodyModeDeploy), false
	case model.DeploymentModeDeployPlan:
		return string(DeploymentCreateBodyModeDeploy), true
	case model.DeploymentModeRollback:
		return string(DeploymentCreateBodyModeRollback), false
	case model.DeploymentModeRollbackPlan:
		return string(DeploymentCreateBodyModeRollback), true
	case model.DeploymentModeDestroy:
		return string(Destroy), false
	default:
		return string(m), false
	}
}

func apiModeSliceToModel(modes *[]string) []model.DeploymentMode {
	if modes == nil {
		return nil
	}
	out := make([]model.DeploymentMode, 0, len(*modes))
	for _, m := range *modes {
		switch m {
		case string(ListDeploymentsParamsByModeDeploy):
			out = append(out, model.DeploymentModeDeploy)
		case string(ListDeploymentsParamsByModePlanOnly):
			out = append(out, model.DeploymentModeDeployPlan)
		case string(ListDeploymentsParamsByModeRollback):
			out = append(out, model.DeploymentModeRollback)
		case string(ListDeploymentsParamsByModeRollbackPlan):
			out = append(out, model.DeploymentModeRollbackPlan)
		case string(ListDeploymentsParamsByModeDestroy):
			out = append(out, model.DeploymentModeDestroy)
		}
	}
	return out
}

func apiStatusSliceToModel(statuses *[]string) []model.DeploymentStatus {
	if statuses == nil {
		return nil
	}
	out := make([]model.DeploymentStatus, 0, len(*statuses))
	for _, s := range *statuses {
		switch s {
		case string(Executing):
			out = append(out, model.DeploymentStatusExecuting)
		case string(Failed):
			out = append(out, model.DeploymentStatusFailed)
		case string(Succeeded):
			out = append(out, model.DeploymentStatusSucceeded)
		}
	}
	return out
}

func apiDepFromModel(m model.DeploymentSummary, manifest model.EncodedDeploymentManifest) Deployment {
	mode, plan := apiModeAndPlanFromModel(m.Mode)
	out := Deployment{
		OrgId:                  m.OrgId,
		ProjectId:              m.ProjectId,
		EnvId:                  m.EnvId,
		Id:                     m.Id,
		CreatedAt:              m.CreatedAt,
		CreatedBy:              m.CreatedBy,
		CompletedAt:            m.CompletedAt.Ref(),
		Mode:                   mode,
		PlanOnly:               plan,
		RollbackToDeploymentId: m.RollbackToId.Ref(),
		RunnerId:               m.RunnerId,
		Status:                 string(m.Status),
		StatusMessage:          m.StatusMessage,
		Metrics:                apiMetricsFromModel(m.Metrics, m.Status == model.DeploymentStatusSucceeded),
	}
	_ = json.Unmarshal(manifest, &out.Manifest)
	return out
}

func apiDepSummaryFromModel(m model.DeploymentSummary) DeploymentSummary {
	mode, plan := apiModeAndPlanFromModel(m.Mode)
	return DeploymentSummary{
		OrgId:                  m.OrgId,
		ProjectId:              m.ProjectId,
		EnvId:                  m.EnvId,
		Id:                     m.Id,
		CreatedAt:              m.CreatedAt,
		CreatedBy:              m.CreatedBy,
		CompletedAt:            m.CompletedAt.Ref(),
		Mode:                   mode,
		PlanOnly:               plan,
		RollbackToDeploymentId: m.RollbackToId.Ref(),

		Status:        string(m.Status),
		StatusMessage: m.StatusMessage,
		Metrics:       apiMetricsFromModel(m.Metrics, m.Status == model.DeploymentStatusSucceeded),
	}
}

func apiMetricsFromModel(m model.DeploymentMetrics, hasTfMetrics bool) DeploymentMetrics {
	d := DeploymentMetrics{
		NumWorkloads:     m.Workloads,
		NumResourceNodes: m.ResourceNodes,
	}
	if hasTfMetrics {
		d.NumTfResources = &m.TfResources
		d.NumTfResourcesAdded = &m.TfResourcesAdded
		d.NumTfResourcesChanged = &m.TfResourcesChanged
		d.NumTfResourcesRemoved = &m.TfResourcesRemoved
	}
	return d
}

// validWorkloadNamePattern is borrowed from Score
var validWorkloadNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$`)

// validSharedAliasPattern is borrowed from Score
var validSharedAliasPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$`)

func (s *Server) CreateDeployment(ctx context.Context, request CreateDeploymentRequestObject) (CreateDeploymentResponseObject, error) {
	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.OrgId = request.OrgId
	ids.ProjectId = request.Body.ProjectId
	ids.EnvId = request.Body.EnvId
	logger := hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).WithLazy(ids.AsLogField())

	var env platformorchestratorcp.Environment

	uid, err := GetAuthenticatedUserIdOr401(ctx)
	if err != nil {
		return nil, err
	}

	if e, err := s.ControlPlaneClient.GetEnvironmentWithResponse(ctx, request.OrgId, request.Body.ProjectId, request.Body.EnvId); err != nil {
		return nil, errors.Wrap(err, "failed to get environment")
	} else if e.StatusCode() == http.StatusNotFound {
		middleware.SetAuthCheckedCtx(ctx)
		return CreateDeployment409JSONResponse{N409ConflictJSONResponse: Generate409Response("environment not found")}, nil
	} else if e.StatusCode() != http.StatusOK {
		return nil, errors.Errorf("unexpected status code when getting environment: %s: %s", e.Status(), string(e.Body))
	} else {
		if err := s.checkEnvWriteAuthorization(ctx, uid, request.OrgId, e.JSON200.Uuid); err != nil {
			return nil, err
		}
		env = *e.JSON200
	}

	// apply additional validation not done by the API spec

	if needsNoManifest := request.Body.Mode == DeploymentCreateBodyModeRollback || request.Body.Mode == Destroy; needsNoManifest && request.Body.Manifest != nil {
		return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400Response(fmt.Sprintf("manifest cannot be provided when mode is %s", request.Body.Mode))}, nil
	} else if !needsNoManifest && request.Body.Manifest == nil {
		return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400Response(fmt.Sprintf("manifest must be provided when mode is %s", request.Body.Mode))}, nil
	}

	if request.Body.Mode == DeploymentCreateBodyModeRollback && request.Body.RollbackToDeploymentId == nil {
		return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400Response(fmt.Sprintf("rollback_to_deployment_id must be set when mode is %s", request.Body.Mode))}, nil
	} else if request.Body.Mode != DeploymentCreateBodyModeRollback && request.Body.RollbackToDeploymentId != nil {
		return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400Response(fmt.Sprintf("rollback_to_deployment_id must not be set when mode is %s", request.Body.Mode))}, nil
	}

	if request.Body.Manifest != nil {
		for name, workload := range request.Body.Manifest.Workloads {
			if !validWorkloadNamePattern.MatchString(name) {
				return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400Response(fmt.Sprintf("workload name '%s' is invalid", name))}, nil
			}

			// Variables are deprecated, convert it to outputs
			if workload.Variables != nil {
				if workload.Outputs != nil {
					return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400Response("variables and outputs cannot be used together, please use outputs instead")}, nil
				}
				workload.Outputs = workload.Variables
				workload.Variables = nil
				request.Body.Manifest.Workloads[name] = workload
			}
		}
		for alias := range request.Body.Manifest.Shared {
			if !validSharedAliasPattern.MatchString(alias) {
				return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400Response(fmt.Sprintf("shared resource alias '%s' is invalid", alias))}, nil
			}
		}
	}

	if request.Body.EncryptedOutputsRecipient != nil {
		if _, err := age.ParseX25519Recipient(*request.Body.EncryptedOutputsRecipient); err != nil {
			logger.Error("invalid age recipient", zap.Error(err))
			return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400Response(fmt.Sprintf("encrypted_outputs_recipient: failed to parse '%s' as age public key", *request.Body.EncryptedOutputsRecipient))}, nil
		}
	}

	if request.Body.EncryptedLogsRecipient != nil {
		if _, err := age.ParseX25519Recipient(*request.Body.EncryptedLogsRecipient); err != nil {
			logger.Error("invalid age recipient", zap.Error(err))
			return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400Response(fmt.Sprintf("encrypted_logs_recipient: failed to parse '%s' as age public key", *request.Body.EncryptedLogsRecipient))}, nil
		}
	}

	var modelMode model.DeploymentMode
	switch request.Body.Mode {
	case DeploymentCreateBodyModeDeploy:
		modelMode = model.DeploymentModeDeploy
		if request.Body.PlanOnly != nil && *request.Body.PlanOnly {
			modelMode = model.DeploymentModeDeployPlan
		}
	case DeploymentCreateBodyModePlanOnly:
		modelMode = model.DeploymentModeDeployPlan
		// backwards compatibility
		if request.Body.PlanOnly != nil && !*request.Body.PlanOnly {
			return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400Response("plan_only cannot be false if mode is plan_only")}, nil
		}
	case DeploymentCreateBodyModeRollback:
		modelMode = model.DeploymentModeRollback
		if request.Body.PlanOnly != nil && *request.Body.PlanOnly {
			modelMode = model.DeploymentModeRollbackPlan
		}
	case Destroy:
		modelMode = model.DeploymentModeDestroy
	default:
		return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400Response(fmt.Sprintf("invalid mode '%s'", request.Body.Mode))}, nil
	}

	if tx, err := s.Database.BeginTx(ctx, nil); err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
	} else {
		defer func() {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				logger.Error("failed to rollback transaction", zap.Error(err))
			}
		}()

		dep, messages, diff, err := CreateDeployment(
			ctx, logger, uid,
			request.OrgId, request.Body.ProjectId, request.Body.EnvId, env, modelMode, opt.OfRef(request.Body.RollbackToDeploymentId),
			ref.DerefOr(request.Body.Manifest, DeploymentManifest{}), request.Body.EncryptedOutputsRecipient, request.Body.EncryptedLogsRecipient, request.Params.IdempotencyKey, request.Body.IsDryRun,
			request.Body.RunnerLogLevel, s.ControlPlaneClient, s.Database, tx,
		)
		if err != nil {
			if e, ok := model.IsErrBadRequest(err); ok {
				return CreateDeployment400JSONResponse{N400BadRequestJSONResponse: Generate400FromModelErr(e)}, nil
			} else if e, ok := model.IsErrConflict(err); ok {
				return CreateDeployment409JSONResponse{N409ConflictJSONResponse: Generate409FromModelErr(e)}, nil
			} else if e, ok := model.IsErrNotFound(err); ok {
				return CreateDeployment409JSONResponse{N409ConflictJSONResponse: Generate409Response(e.Message)}, nil
			}
			return nil, errors.Wrap(err, "failed to create deployment")
		}

		if request.Body.IsDryRun {
			return CreateDeployment200JSONResponse{RunnerId: dep.RunnerId, Diff: *diff}, nil
		}

		if err := tx.Commit(); err != nil {
			return nil, errors.Wrap(err, "failed to commit transaction")
		}
		ids.DeployId = dep.Id.String()
		logger.Info("Created deployment")

		rawManifest, _ := json.Marshal(request.Body.Manifest)
		reliableoutbox.OptimisticPublish(ctx, logger, s.Database.AsReliableOutboxStore(), s.RabbitMqPublisher, messages)
		return CreateDeployment201JSONResponse(apiDepFromModel(*dep, rawManifest)), nil
	}
}

func (s *Server) ListDeployments(ctx context.Context, request ListDeploymentsRequestObject) (ListDeploymentsResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgReadAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	if ok, err := checkIfOrganizationExists(ctx, s.ControlPlaneClient, request.OrgId); err != nil {
		return nil, errors.Wrap(err, "failed to check if organization exists")
	} else if !ok {
		return ListDeployments404JSONResponse{Generate404FromModelErr(model.ErrNotFound{
			Message: fmt.Sprintf("organization %s not found", request.OrgId),
		})}, nil
	}

	if request.Params.EnvId != nil && request.Params.ProjectId == nil {
		return ListDeployments400JSONResponse{N400BadRequestJSONResponse: Generate400Response("app_id param required if env_id is specified")}, nil
	} else if items, npt, err := s.Database.ListDeployments(
		ctx, nil, request.OrgId, opt.OfRef(request.Params.Page).Or(""), opt.OfRef(request.Params.PerPage).Or(defaultPaginationSize), model.ListDeploymentsParams{
			ProjectId: opt.OfRef(request.Params.ProjectId),
			EnvId:     opt.OfRef(request.Params.EnvId),
			ByMode:    apiModeSliceToModel(request.Params.ByMode),
			ByStatus:  apiStatusSliceToModel(request.Params.ByStatus),
		},
	); err != nil {
		if e, ok := model.IsErrBadRequest(err); ok {
			return ListDeployments400JSONResponse{N400BadRequestJSONResponse: Generate400FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to list deployments")
	} else {
		out := make([]DeploymentSummary, 0, len(items))
		for _, item := range items {
			out = append(out, apiDepSummaryFromModel(item))
		}
		return ListDeployments200JSONResponse{
			Items:         out,
			NextPageToken: ref.RefStringEmptyNil(npt),
		}, nil
	}
}

func (s *Server) ListLastDeployments(ctx context.Context, request ListLastDeploymentsRequestObject) (ListLastDeploymentsResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgReadAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	if ok, err := checkIfOrganizationExists(ctx, s.ControlPlaneClient, request.OrgId); err != nil {
		return nil, errors.Wrap(err, "failed to check if organization exists")
	} else if !ok {
		return ListLastDeployments404JSONResponse{Generate404FromModelErr(model.ErrNotFound{
			Message: fmt.Sprintf("organization %s not found", request.OrgId),
		})}, nil
	}

	if request.Params.EnvId != nil && request.Params.ProjectId == nil {
		return ListLastDeployments400JSONResponse{N400BadRequestJSONResponse: Generate400Response("app_id param required if env_id is specified")}, nil
	} else if items, npt, err := s.Database.ListLastDeployments(
		ctx, nil, request.OrgId, opt.OfRef(request.Params.Page).Or(""), opt.OfRef(request.Params.PerPage).Or(defaultPaginationSize), model.ListLastDeploymentsParams{
			ProjectId:       opt.OfRef(request.Params.ProjectId),
			EnvId:           opt.OfRef(request.Params.EnvId),
			StateChangeOnly: ref.DerefOr(request.Params.StateChangeOnly, false),
		},
	); err != nil {
		if e, ok := model.IsErrBadRequest(err); ok {
			return ListLastDeployments400JSONResponse{N400BadRequestJSONResponse: Generate400FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to list deployments")
	} else {
		out := make([]DeploymentSummary, 0, len(items))
		for _, item := range items {
			out = append(out, apiDepSummaryFromModel(item))
		}
		return ListLastDeployments200JSONResponse{
			Items:         out,
			NextPageToken: ref.RefStringEmptyNil(npt),
		}, nil
	}
}

func (s *Server) GetDeployment(ctx context.Context, request GetDeploymentRequestObject) (GetDeploymentResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgReadAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	if d, rawManifest, _, _, err := s.Database.GetDeployment(ctx, nil, request.OrgId, request.DeploymentId, model.GetModeDefault); err != nil {
		if e, ok := model.IsErrNotFound(err); ok {
			return GetDeployment404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to get deployment")
	} else {
		return GetDeployment200JSONResponse(apiDepFromModel(*d, rawManifest)), nil
	}
}

func (s *Server) GetDeploymentBundle(ctx context.Context, request GetDeploymentBundleRequestObject) (GetDeploymentBundleResponseObject, error) {
	middleware.SetAuthCheckedCtx(ctx)
	if request.Params.XDeploymentToken == "" || util.GenerateHashedRunnerToken(s.RunnerTokenSalt, request.OrgId, request.DeploymentId.String()) != request.Params.XDeploymentToken {
		return GetDeploymentBundle400JSONResponse{N400BadRequestJSONResponse: Generate400Response("the runner token is not valid")}, nil
	}

	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.OrgId = request.OrgId
	ids.DeployId = request.DeploymentId.String()

	deployment, _, tofu, rawGraph, err := s.Database.GetDeployment(ctx, nil, request.OrgId, request.DeploymentId, model.GetModeDefault)
	if err != nil {
		if e, ok := model.IsErrNotFound(err); ok {
			return GetDeploymentBundle404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to get deployment")
	}
	ids.ProjectId = deployment.ProjectId
	ids.EnvId = deployment.EnvId
	ids.RunnerId = deployment.RunnerId

	graph, err := graphs.FromJson(bytes.NewReader(rawGraph))
	if err != nil {
		return nil, err
	}

	// We need to include the source code of any inline modules here within the bundle.
	var inlineModules []platformorchestratorcp.InternalModuleCatalogueModule
	versionsToGet := make([]string, 0)
	for rc := range graph.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePreOrder) {
		n := graph.Nodes[rc]
		if n.ModuleConfiguration != nil && n.ModuleConfiguration.HasInlineSource {
			versionsToGet = append(versionsToGet, fmt.Sprintf("%s@%s", n.ModuleConfiguration.DefinitionId, n.ModuleConfiguration.VersionId))
		}
	}
	if len(versionsToGet) > 0 {
		if r, err := s.ControlPlaneClient.GenerateInternalModuleCatalogueWithResponse(ctx, request.OrgId, deployment.ProjectId, deployment.EnvId, platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{
			AreRulesIgnored:      true,
			PinnedModuleVersions: versionsToGet,
		}); err != nil {
			return nil, errors.Wrap(err, "failed to generate module catalogue")
		} else if r.StatusCode() != http.StatusOK {
			return nil, errors.Errorf("unexpected status code %d when fetching module catalogue: %s", r.StatusCode(), string(r.Body))
		} else {
			inlineModules = r.JSON200.Modules
		}
	}

	if archive, err := compressTofu(tofu, inlineModules); err != nil {
		return nil, errors.Wrap(err, "failed to build bundle")
	} else {
		return GetDeploymentBundle200ApplicationxGzipResponse{Body: archive, ContentLength: int64(archive.Len())}, nil
	}
}

func (s *Server) GetDeploymentTf(ctx context.Context, request GetDeploymentTfRequestObject) (GetDeploymentTfResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgManageAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.OrgId = request.OrgId
	ids.DeployId = request.DeploymentId.String()

	deployment, _, tofu, _, err := s.Database.GetDeployment(ctx, nil, request.OrgId, request.DeploymentId, model.GetModeDefault)
	if err != nil {
		if e, ok := model.IsErrNotFound(err); ok {
			return GetDeploymentTf404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to get deployment")
	}
	ids.ProjectId = deployment.ProjectId
	ids.EnvId = deployment.EnvId
	ids.RunnerId = deployment.RunnerId
	return GetDeploymentTf200TextResponse(tofu), nil
}

func (s *Server) InternalDeleteDeployments(ctx context.Context, request InternalDeleteDeploymentsRequestObject) (InternalDeleteDeploymentsResponseObject, error) {
	if request.Params.ProjectId == nil {
		return InternalDeleteDeployments400JSONResponse{N400BadRequestJSONResponse: Generate400Response("app_id param required")}, nil
	} else if request.Params.EnvId == nil {
		return InternalDeleteDeployments400JSONResponse{N400BadRequestJSONResponse: Generate400Response("env_id param required")}, nil
	}

	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.OrgId = request.OrgId
	ids.ProjectId = *request.Params.ProjectId
	ids.EnvId = *request.Params.EnvId
	logger := hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).WithLazy(ids.AsLogField())

	if tx, err := s.Database.BeginTx(ctx, nil); err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
	} else {
		defer func() {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				logger.Error("failed to rollback transaction", zap.Error(err))
			}
		}()

		force := ref.DerefOr(request.Params.Force, false)
		if !force {
			if d, _, _, _, err := s.Database.GetLastDeployment(ctx, tx, request.OrgId, *request.Params.ProjectId, *request.Params.EnvId, model.GetLastDeploymentParams{}); err != nil {
				if _, ok := model.IsErrNotFound(err); ok {
					return InternalDeleteDeployments204Response{}, nil
				}
				return nil, errors.Wrap(err, "failed to get last deployment")
			} else if d.Status == model.DeploymentStatusExecuting {
				return InternalDeleteDeployments409JSONResponse{N409ConflictJSONResponse: Generate409Response(fmt.Sprintf("deployment '%s' is still executing", d.Id))}, nil
			} else if d.Mode != model.DeploymentModeDestroy || d.Status != model.DeploymentStatusSucceeded {
				return InternalDeleteDeployments409JSONResponse{N409ConflictJSONResponse: Generate409Response("cannot delete deployment environment without a successful destroy")}, nil
			}
		}

		if err := s.Database.DeleteDeploymentsForEnv(ctx, tx, request.OrgId, *request.Params.ProjectId, *request.Params.EnvId, force); err != nil {
			if e, ok := model.IsErrNotFound(err); ok {
				return InternalDeleteDeployments404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
			} else if e, ok := model.IsErrConflict(err); ok {
				return InternalDeleteDeployments409JSONResponse{N409ConflictJSONResponse: Generate409FromModelErr(e)}, nil
			}
			return nil, errors.Wrap(err, "failed to delete deployments")
		} else if err := tx.Commit(); err != nil {
			return nil, errors.Wrap(err, "failed to commit transaction")
		}
		logger.Info("deleted deployment environments", zap.Bool("forced", force))
	}

	return InternalDeleteDeployments204Response{}, nil
}

// enrichDeploymentError enriches deployment error messages with additional context based on entity type.
// It takes a TF_DIAGNOSTIC_ERROR message and adds module/provider/output specific information.
// Returns the enriched error message as a JSON string, or the original message if enrichment is not applicable.
func enrichDeploymentError(ctx context.Context, db model.Databaser, orgId string, deploymentId uuid.UUID, errorCode, errorMessage string) (string, error) {
	// Only enrich TF_DIAGNOSTIC_ERROR errors
	if errorCode != "TF_DIAGNOSTIC_ERROR" {
		return fmt.Sprintf("runner failed with code %s: %s", errorCode, errorMessage), nil
	}

	// Parse the error message
	var parsedErr map[string]interface{}
	if err := json.Unmarshal([]byte(errorMessage), &parsedErr); err != nil {
		// If we can't parse the message as JSON, return the default format
		return fmt.Sprintf("runner failed with code %s: %s", errorCode, errorMessage), nil
	}

	// Extract entity_id and entity_type
	entityId, _ := parsedErr["entity_id"].(string)
	entityType, _ := parsedErr["entity_type"].(string)
	entityVersion, _ := parsedErr["entity_version"].(string)

	if entityId != "" {
		// Enrich based on entity type
		switch entityType {
		case "module":
			graph, err := getDeploymentGraph(ctx, db, orgId, deploymentId)
			if err != nil {
				return "", errors.Wrap(err, "failed to get deployment graph for error enrichment")
			}
			if entityVersion != "" {
				parsedErr["module_id"] = entityId
				parsedErr["module_version"] = entityVersion
				enrichedJson, _ := json.Marshal(parsedErr)
				return string(enrichedJson), nil
			} else {
				// Find the last underscore to get the module name prefix
				if lastDashIdx := strings.LastIndex(entityId, "_"); lastDashIdx != -1 {
					moduleNamePrefix := entityId[:lastDashIdx]
					// Search for a matching node in the graph
					for rc, node := range graph.Nodes {
						nodeIdentifier := stripIdentifierPattern.ReplaceAllString(fmt.Sprintf("%s_%s_%s", rc.Type, rc.Class, rc.Id), "")
						if nodeIdentifier == moduleNamePrefix {
							if node.ModuleConfiguration != nil {
								// Enrich with module information
								parsedErr["module_id"] = node.ModuleConfiguration.DefinitionId
								parsedErr["module_version"] = node.ModuleConfiguration.VersionId
								enrichedJson, _ := json.Marshal(parsedErr)
								return string(enrichedJson), nil
							}
						}
					}
				}
			}

		case "provider":
			graph, err := getDeploymentGraph(ctx, db, orgId, deploymentId)
			if err != nil {
				return "", errors.Wrap(err, "failed to get deployment graph for error enrichment")
			}

			var providerLocalNameToIdentifier = make(map[string]string)
			for _, node := range graph.Nodes {
				if node.ModuleConfiguration != nil {
					// ProviderFullIdToHashVariantMapping uses full provider identifier as the key
					for fullProviderIdentifier, hash := range node.ModuleConfiguration.ProviderFullIdToHashVariantMapping {
						identifierSplitted := strings.SplitN(fullProviderIdentifier, ".", providerFullIdentifierExpectedLength)
						if len(identifierSplitted) != providerFullIdentifierExpectedLength {
							continue
						}
						providerLocalNameToIdentifier[graphs.LocalProviderName(identifierSplitted[0], identifierSplitted[1], hash)] = fullProviderIdentifier
					}
				}
			}

			for localName, identifier := range providerLocalNameToIdentifier {
				if localName == entityId {
					localRefSplitted := strings.SplitN(identifier, ".", providerFullIdentifierExpectedLength)
					if len(localRefSplitted) == providerFullIdentifierExpectedLength {
						parsedErr["provider_type"] = localRefSplitted[0]
						parsedErr["provider_id"] = localRefSplitted[1]
						enrichedJson, _ := json.Marshal(parsedErr)
						return string(enrichedJson), nil
					}
				}
			}
		case "output":
			// Output entity_id is the workload name
			parsedErr["workload"] = entityId
			enrichedJson, _ := json.Marshal(parsedErr)
			return string(enrichedJson), nil
		default:
			// Unknown entity type, return original error
		}
	}

	enrichedJson, _ := json.Marshal(parsedErr)
	return string(enrichedJson), nil
}

// Return the runner outcome for a specific deployment.
// (POST /orgs/{orgId}/deployments/{deploymentId}/results)
func (s *Server) UpdateDeploymentResults(ctx context.Context, request UpdateDeploymentResultsRequestObject) (UpdateDeploymentResultsResponseObject, error) {
	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.OrgId = request.OrgId
	ids.DeployId = request.DeploymentId.String()
	logger := hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).WithLazy(ids.AsLogField())

	if request.Params.XDeploymentToken == "" || util.GenerateHashedRunnerToken(s.RunnerTokenSalt, request.OrgId, request.DeploymentId.String()) != request.Params.XDeploymentToken {
		return UpdateDeploymentResults400JSONResponse{N400BadRequestJSONResponse: Generate400Response("the runner token is not valid")}, nil
	} else {
		middleware.SetAuthCheckedCtx(ctx)

		if request.Body.Outputs != nil {
			if _, err := io.Copy(io.Discard, base64.NewDecoder(base64.StdEncoding, strings.NewReader(*request.Body.Outputs))); err != nil {
				return UpdateDeploymentResults400JSONResponse{N400BadRequestJSONResponse: Generate400Response("outputs were invalid base64, expected std encoding")}, nil
			}
		}
		updateParams := model.UpdateDeploymentStatusAndOutputsParams{
			Outputs: opt.OfRef(request.Body.Outputs),
		}

		if request.Body.Status == Success {
			updateParams.Status = model.DeploymentStatusSucceeded
		} else {
			updateParams.Status = model.DeploymentStatusFailed
			updateParams.StatusMessage = "runner failed without error details"
			if request.Body.Error != nil {
				// Try to enrich the error message with additional context
				if enriched, err := enrichDeploymentError(ctx, s.Database, request.OrgId, request.DeploymentId, request.Body.Error.Error, request.Body.Error.Message); err != nil {
					return nil, err
				} else {
					updateParams.StatusMessage = enriched
				}
			}
		}

		if _, err := s.commonUpdateDeploymentResults(ctx, logger, request, updateParams); err != nil {
			if e, ok := model.IsErrNotFound(err); ok {
				return UpdateDeploymentResults404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
			} else if e, ok := model.IsErrConflict(err); ok {
				return UpdateDeploymentResults409JSONResponse{N409ConflictJSONResponse: Generate409FromModelErr(e)}, nil
			}
			return nil, errors.Wrap(err, "failed to update deployment")
		}
		return UpdateDeploymentResults204Response{}, nil
	}
}

func (s *Server) commonUpdateDeploymentResults(ctx context.Context, logger *zap.Logger, request UpdateDeploymentResultsRequestObject, updateParams model.UpdateDeploymentStatusAndOutputsParams) (*model.DeploymentSummary, error) {
	if tx, err := s.Database.BeginTx(ctx, nil); err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
	} else {
		defer func() {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				logger.Error("failed to rollback transaction", zap.Error(err))
			}
		}()

		if dep, _, _, _, err := s.Database.GetDeployment(ctx, tx, request.OrgId, request.DeploymentId, model.GetModeForUpdate); err != nil {
			return nil, errors.Wrap(err, "failed to get deployment")
		} else if dep.CompletedAt.IsSet() {
			return nil, model.NewErrConflict("deployment already completed")
		} else {
			ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
			ids.DeployId = dep.Id.String()
			ids.ProjectId = dep.ProjectId
			ids.EnvId = dep.EnvId
			ids.RunnerId = dep.RunnerId

			dep.Metrics.TfResources = request.Body.TfResourceCounts.NumResources
			dep.Metrics.TfResourcesAdded = request.Body.TfResourceCounts.NumResourcesAdded
			dep.Metrics.TfResourcesChanged = request.Body.TfResourceCounts.NumResourcesChanged
			dep.Metrics.TfResourcesRemoved = request.Body.TfResourceCounts.NumResourcesRemoved
			updateParams.Metrics = dep.Metrics

			if dep, err = s.Database.UpdateDeploymentStatusAndOutputs(ctx, tx, request.DeploymentId, updateParams); err != nil {
				return nil, errors.Wrap(err, "failed to update deployment")
			}

			if err := s.Database.CreateDeploymentHistoryRecord(ctx, tx, dep); err != nil {
				return nil, errors.Wrap(err, "failed to create deployment history record")
			}

			if dep.Mode != model.DeploymentModeDeployPlan && updateParams.Status == model.DeploymentStatusSucceeded {
				if err := s.Database.DiscardOldActiveResources(ctx, tx, dep.DeploymentEnvUuid, dep.Id); err != nil {
					return nil, errors.Wrap(err, "failed to discard old active resources")
				}
			}

			if len(request.Body.Metadata) > 0 {
				updateMetadataParams := make([]model.UpdateResourceNodeParams, 0)
				for _, m := range request.Body.Metadata {
					if len(m.Metadata) > 0 {
						updateMetadataParams = append(updateMetadataParams, model.UpdateResourceNodeParams{
							Hash:     m.NodeId,
							Metadata: m.Metadata,
						})
					}
				}
				if len(updateMetadataParams) > 0 {
					if err := s.Database.BulkUpdateActiveResources(ctx, tx, dep.DeploymentEnvUuid, dep.Id, updateMetadataParams); err != nil {
						return nil, errors.Wrap(err, "failed to update active resources")
					} else {
						logger.Info("update active resource nodes", zap.Int("num_updated_nodes", len(updateMetadataParams)))
					}
				}
			}

			msg := &hstandardreliableoutbox.PendingEventMessage{
				Exchange:   events.DefaultExchange,
				RoutingKey: string(genevents.IoPlatformOrchestratorDeploymentUpdated),
				Payload:    model.ConvertDeploymentToEventPayload(dep),
			}
			if messages, err := s.Database.InsertPendingEventMessages(ctx, tx, []*hstandardreliableoutbox.PendingEventMessage{msg}); err != nil {
				return nil, errors.Wrap(err, "failed to insert pending event messages")
			} else if err := tx.Commit(); err != nil {
				return nil, errors.Wrap(err, "failed to commit transaction")
			} else {
				logger.Info(
					"deployment completed",
					zap.String("status", string(updateParams.Status)),
					zap.String("status_message", updateParams.StatusMessage),
					zap.Int("tf_resources", updateParams.Metrics.TfResources),
					zap.Int("tf_resources_added", updateParams.Metrics.TfResourcesAdded),
					zap.Int("tf_resources_changed", updateParams.Metrics.TfResourcesChanged),
					zap.Int("tf_resources_removed", updateParams.Metrics.TfResourcesRemoved),
				)
				reliableoutbox.OptimisticPublish(ctx, logger, s.Database.AsReliableOutboxStore(), s.RabbitMqPublisher, messages)
				return dep, nil
			}
		}
	}
}

// Get the logs produced by the runner execution.
// (GET /orgs/{orgId}/deployments/{deploymentId}/actions/getLogs)
func (s *Server) GetDeploymentLogs(ctx context.Context, request GetDeploymentLogsRequestObject) (GetDeploymentLogsResponseObject, error) {
	logger := hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).
		With(logging.ZapOrgId(request.OrgId), logging.ZapDeploymentId(request.DeploymentId.String()))
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgReadAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	ds, _, _, _, err := s.Database.GetDeployment(ctx, nil, request.OrgId, request.DeploymentId, model.GetModeDefault)
	if err != nil {
		if e, ok := model.IsErrNotFound(err); ok {
			return GetDeploymentLogs404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
		}
		return nil, err
	}

	logsReader, err := s.RunnerLogsReader(ctx, ds.DeploymentEnvUuid.String()+"/"+request.DeploymentId.String())
	if errors.Is(err, storage.ErrObjectNotExist) {
		// Try to read the logs from the deprecated location
		logsReader, err = s.RunnerLogsReader(ctx, request.OrgId+"/"+request.DeploymentId.String())
	}
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return GetDeploymentLogs404JSONResponse{N404NotFoundJSONResponse: Generate404Response(fmt.Sprintf("logs for deployment %s not found", request.DeploymentId))}, nil
		}
		return nil, errors.Wrap(err, "failed to create log reader")
	}
	defer func() {
		if err := logsReader.Close(); err != nil {
			logger.Error("failed to close logs reader", zap.Error(err))
		}
	}()

	var logs bytes.Buffer
	if _, err := logs.ReadFrom(logsReader); err != nil {
		return nil, errors.Wrap(err, "failed to read logs")
	}

	decodedContent, err := base64.StdEncoding.DecodeString(logs.String())
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode encrypted logs content from base64")
	}

	if request.Params.DecryptKey == nil {
		return GetDeploymentLogs200TextResponse{Body: string(decodedContent), Headers: GetDeploymentLogs200ResponseHeaders{ContentDisposition: fmt.Sprintf("attachment; filename=\"%s.log\"", request.DeploymentId)}}, nil
	}

	agePrivateKey, err := age.ParseX25519Identity(*request.Params.DecryptKey)
	if err != nil {
		return GetDeploymentLogs400JSONResponse{N400BadRequestJSONResponse: Generate400Response("the supplied key is not a valid 'age' private key")}, nil
	}

	decryptedLogs, err := age.Decrypt(bytes.NewReader(decodedContent), agePrivateKey)
	if err != nil {
		return GetDeploymentLogs400JSONResponse{N400BadRequestJSONResponse: Generate400Response(fmt.Sprintf("logs can't be decrypted with the supplied key: %v", err))}, nil
	}

	var decryptedBuf bytes.Buffer
	if _, err := io.Copy(&decryptedBuf, decryptedLogs); err != nil {
		return nil, errors.Wrap(err, "failed to read decrypted logs")
	}

	return GetDeploymentLogs200TextResponse{
		Body: decryptedBuf.String(),
		Headers: GetDeploymentLogs200ResponseHeaders{
			ContentDisposition: fmt.Sprintf("attachment; filename=\"%s.log\"", request.DeploymentId),
		},
	}, nil
}

func compressTofu(content []byte, inlineModules []platformorchestratorcp.InternalModuleCatalogueModule) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)

	// gzip writer
	gw := gzip.NewWriter(buf)
	defer func() {
		_ = gw.Close()
	}()

	// archive writer
	tw := tar.NewWriter(gw)
	defer func() {
		_ = tw.Close()
	}()

	header := &tar.Header{
		Name:    "main.tf",
		Size:    int64(len(content)),
		Mode:    headerMode,
		ModTime: time.Now(),
	}

	if err := tw.WriteHeader(header); err != nil {
		return nil, errors.Wrap(err, "failed to write archive header")
	}

	if _, err := tw.Write(content); err != nil {
		return nil, errors.Wrap(err, "failed to write archive content")
	}

	for _, m := range inlineModules {
		dirName := fmt.Sprintf("modules/%s/%s", m.Id, m.VersionId)
		dirHeader := &tar.Header{
			Name:     dirName + "/",
			Mode:     headerDirectoryMode,
			ModTime:  time.Now(),
			Typeflag: tar.TypeDir,
		}
		if err := tw.WriteHeader(dirHeader); err != nil {
			return nil, errors.Wrapf(err, "failed to write directory header for %s", dirName)
		}
		// Write main.tf file inside the directory
		fileHeader := &tar.Header{
			Name:    dirName + "/main.tf",
			Mode:    headerMode,
			Size:    int64(len(*m.ModuleSourceCode)),
			ModTime: time.Now(),
		}
		if err := tw.WriteHeader(fileHeader); err != nil {
			return nil, errors.Wrapf(err, "failed to write file header for %s/main.tf", dirName)
		}
		if _, err := tw.Write([]byte(*m.ModuleSourceCode)); err != nil {
			return nil, errors.Wrap(err, "failed to write archive content")
		}
	}

	if err := tw.Close(); err != nil {
		return nil, errors.Wrap(err, "failed to close archive writer")
	}

	if err := gw.Close(); err != nil {
		return nil, errors.Wrap(err, "failed to close gzip writer")
	}
	return buf, nil
}

const waitForDeploymentPollTime = time.Second * 10

func (s *Server) WaitForDeploymentComplete(ctx context.Context, request WaitForDeploymentCompleteRequestObject) (WaitForDeploymentCompleteResponseObject, error) {
	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.DeployId = request.DeploymentId.String()

	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else {
		d, _, _, _, err := s.Database.GetDeployment(ctx, nil, request.OrgId, request.DeploymentId, model.GetModeDefault)
		if err != nil {
			if e, ok := model.IsErrNotFound(err); ok {
				middleware.SetAuthCheckedCtx(ctx)
				return WaitForDeploymentComplete404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
			}
			return nil, errors.Wrap(err, "failed to get deployment")
		} else {
			if err := s.checkEnvWriteAuthorization(ctx, uid, request.OrgId, d.DeploymentEnvUuid); err != nil {
				return nil, err
			}
		}
	}

	if request.Params.TimeoutInSeconds != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(max(1, *request.Params.TimeoutInSeconds))*time.Second)
		defer cancel()
	}

	// Create a channel and register it, make sure on shutdown, that we deregister it!
	ch, fin := s.DeploymentCompletedHooks.AddWaiter(completionhooks.DeploymentOrgAndId{OrgId: request.OrgId, DeploymentId: request.DeploymentId.String()})
	defer fin()

	var last bool
	for {
		d, m, _, _, err := s.Database.GetDeployment(ctx, nil, request.OrgId, request.DeploymentId, model.GetModeDefault)
		if err != nil {
			if e, ok := model.IsErrNotFound(err); ok {
				return WaitForDeploymentComplete404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
			}
			return nil, errors.Wrap(err, "failed to get deployment")
		}
		ids.ProjectId = d.ProjectId
		ids.EnvId = d.EnvId

		if d.Status != model.DeploymentStatusExecuting {
			return WaitForDeploymentComplete200JSONResponse(apiDepFromModel(*d, m)), nil
		} else if last {
			return WaitForDeploymentComplete408JSONResponse(Error{Error: "HTTP-408", Message: "deployment has not completed yet"}), nil
		}
		select {
		case <-ch:
			// if the channel is closed, we get 1 last chance to check it before we must return the 408. We cannot keep waiting on it
			// because it's now been removed from the set of waiting channels.
			last = true
			continue
		case <-time.After(waitForDeploymentPollTime):
			// every 10 seconds, we'll revert to polling the deployment again in case we've lost events
			continue
		case <-ctx.Done():
			// if the request context closes it's because the request has hung up, or the `timeout_in_seconds` query param
			// was met.
			return WaitForDeploymentComplete408JSONResponse(Error{Error: "HTTP-408", Message: "deployment has not completed yet"}), nil
		}
	}
}

func ScheduleDeploymentOutputsCleaning(ctx context.Context, interval time.Duration, logger *zap.Logger, db model.Databaser) {
	go func() {
		for {
			select {
			case <-time.After(interval + time.Duration((rand.Float64()-0.5)*0.5*float64(interval))): //nolint:gosec,mnd
			case <-ctx.Done():
				return
			}
			span := htelemetry.StartSpan("background:platform-orchestrator-dp-outputs-cleaner")
			subCtx := htelemetry.ContextWithSpan(ctx, span)
			subLogger := hlogger.TraceScopedLoggerFromCtx(logger, subCtx)
			if depsWithStaledOutputs, err := db.DeleteDeploymentOutputsByCompletionDate(ctx, nil, time.Now().Add(-interval)); err != nil {
				subLogger.With(zap.Error(err)).Error("failed to delete staled outputs")
				span.Finish(htelemetry.WithError(err))
			} else {
				if len(depsWithStaledOutputs) > 0 {
					for _, dep := range depsWithStaledOutputs {
						subLogger.Info("cleared outputs from stale deployment", logging.ZapOrgId(dep.OrgId), logging.ZapProjectId(dep.ProjectId), logging.ZapEnvId(dep.EnvId), logging.ZapDeploymentId(dep.Id.String()))
					}
				} else {
					subLogger.Debug("no stale deployment outputs to clear")
				}
			}
			span.Finish()
		}
	}()
}

func (s *Server) GetDeploymentEncryptedOutputs(ctx context.Context, request GetDeploymentEncryptedOutputsRequestObject) (GetDeploymentEncryptedOutputsResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgReadAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	if dep, _, _, _, err := s.Database.GetDeployment(ctx, nil, request.OrgId, request.DeploymentId, model.GetModeDefault); err != nil {
		if e, ok := model.IsErrNotFound(err); ok {
			return GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to get deployment")
	} else if dep.Mode == model.DeploymentModeDestroy {
		return GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404Response(fmt.Sprintf("outputs are not captured for deployments with mode '%s'", dep.Mode))}, nil
	} else if !dep.CompletedAt.IsSet() {
		return GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404Response(fmt.Sprintf("deployment '%s' has not completed yet", dep.Id))}, nil
	} else if dep.Status != model.DeploymentStatusSucceeded {
		return GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404Response(fmt.Sprintf("outputs are not captured for deployments with status '%s'", dep.Status))}, nil
	} else if !dep.EncryptedOutputsRecipient.IsSet() {
		return GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404Response("outputs are not captured for deployments with no outputs recipient key")}, nil
	}

	if out, err := s.Database.GetDeploymentEncryptedOutputs(ctx, nil, request.OrgId, request.DeploymentId); err != nil {
		return nil, errors.Wrap(err, "failed to get deployment outputs")
	} else if len(out) == 0 {
		return GetDeploymentEncryptedOutputs404JSONResponse{N404NotFoundJSONResponse: Generate404Response("deployment outputs are no longer available")}, nil
	} else {
		return GetDeploymentEncryptedOutputs200JSONResponse{Raw: out}, nil
	}
}

func (s *Server) InternalForceFailDeployment(ctx context.Context, request InternalForceFailDeploymentRequestObject) (InternalForceFailDeploymentResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if uid != userid.InternalSystemUuid {
		return nil, errors.New("only the system user id can force a deployment to fail")
	}

	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.DeployId = request.DeploymentId.String()
	logger := hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).WithLazy(ids.AsLogField())

	updateRequest := UpdateDeploymentResultsRequestObject{
		OrgId:        request.OrgId,
		DeploymentId: request.DeploymentId,
		Body:         &UpdateDeploymentResultsJSONRequestBody{},
	}
	updateParams := model.UpdateDeploymentStatusAndOutputsParams{
		Status:        model.DeploymentStatusFailed,
		StatusMessage: "deployment was manually failed by an operator",
	}

	if dep, err := s.commonUpdateDeploymentResults(ctx, logger, updateRequest, updateParams); err != nil {
		if e, ok := model.IsErrNotFound(err); ok {
			return InternalForceFailDeployment404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
		} else if e, ok := model.IsErrConflict(err); ok {
			return InternalForceFailDeployment409JSONResponse{N409ConflictJSONResponse: Generate409FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to update deployment")
	} else {
		return InternalForceFailDeployment200JSONResponse(apiDepSummaryFromModel(*dep)), nil
	}
}

func (s *Server) CalculateDeploymentDiff(ctx context.Context, request CalculateDeploymentDiffRequestObject) (CalculateDeploymentDiffResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgReadAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}
	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.DeployId = request.DeploymentId.String()

	var toDep *model.DeploymentSummary
	var toGraph, fromGraph *platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]
	if d, _, _, rawToGraph, err := s.Database.GetDeployment(ctx, nil, request.OrgId, request.DeploymentId, model.GetModeDefault); err != nil {
		if _, ok := model.IsErrNotFound(err); ok {
			return CalculateDeploymentDiff404JSONResponse{N404NotFoundJSONResponse: Generate404Response(fmt.Sprintf("deployment '%s' not found", request.DeploymentId))}, nil
		}
		return nil, errors.Wrap(err, "failed to get target deployment")
	} else if err := json.Unmarshal(rawToGraph, &toGraph); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal target deployment graph")
	} else {
		toDep = d
		ids.ProjectId = toDep.ProjectId
		ids.EnvId = toDep.EnvId
	}

	if request.Params.FromDeploymentId == nil {
		if page, _, err := s.Database.ListDeployments(ctx, nil, request.OrgId, "", 1, model.ListDeploymentsParams{
			ProjectId: opt.Of(toDep.ProjectId), EnvId: opt.Of(toDep.EnvId), CreatedBefore: opt.Of(toDep.CreatedAt), ByMode: []model.DeploymentMode{model.DeploymentModeRollback, model.DeploymentModeDeploy, model.DeploymentModeDestroy},
		}); err != nil {
			return nil, fmt.Errorf("failed to list deployments: %w", err)
		} else if len(page) == 0 {
			fromGraph = &platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{}
		} else {
			request.Params.FromDeploymentId = &page[0].Id
		}
	}

	if fromGraph == nil {
		if fromDep, _, _, rawFromGraph, err := s.Database.GetDeployment(ctx, nil, request.OrgId, *request.Params.FromDeploymentId, model.GetModeDefault); err != nil {
			if _, ok := model.IsErrNotFound(err); ok {
				return CalculateDeploymentDiff404JSONResponse{N404NotFoundJSONResponse: Generate404Response(fmt.Sprintf("deployment '%s' not found", request.DeploymentId))}, nil
			}
			return nil, errors.Wrap(err, "failed to get source deployment")
		} else if err := json.Unmarshal(rawFromGraph, &fromGraph); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal source deployment graph")
		} else if fromDep.DeploymentEnvUuid != toDep.DeploymentEnvUuid {
			return CalculateDeploymentDiff400JSONResponse{N400BadRequestJSONResponse: Generate400Response("the deployments must be in the same environment")}, nil
		}
	}

	diff := DiffGraphs(toDep.DeploymentEnvUuid, fromGraph, toGraph)
	diff.ToDeploymentId = ref.Ref(toDep.Id)
	diff.FromDeploymentId = request.Params.FromDeploymentId
	return CalculateDeploymentDiff200JSONResponse(diff), nil
}

func getDeploymentGraph(ctx context.Context, db model.Databaser, orgId string, deploymentId uuid.UUID) (*platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig], error) {
	_, _, _, encodedGraph, err := db.GetDeployment(ctx, nil, orgId, deploymentId, model.GetModeDefault)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get deployment for error enrichment")
	}

	// Parse the graph
	graph, err := graphs.FromJson(bytes.NewReader(encodedGraph))
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse deployment graph for error enrichment")
	}
	return graph, nil
}
