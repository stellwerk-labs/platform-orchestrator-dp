package api

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/logging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func (s *Server) ListActiveResourceNodes(ctx context.Context, request ListActiveResourceNodesRequestObject) (ListActiveResourceNodesResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgAuthorization(ctx, uid, request.OrgId, PermissionActiveResourceRead); err != nil {
		return nil, err
	}

	if request.Params.ProjectId == nil {
		return ListActiveResourceNodes400JSONResponse{N400BadRequestJSONResponse: Generate400Response("project_id param required")}, nil
	} else if request.Params.EnvId == nil {
		return ListActiveResourceNodes400JSONResponse{N400BadRequestJSONResponse: Generate400Response("env_id param required")}, nil
	}
	logger := hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).
		With(logging.ZapOrgId(request.OrgId), logging.ZapProjectId(*request.Params.ProjectId), logging.ZapEnvId(*request.Params.EnvId))

	if tx, err := s.Database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true}); err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
	} else {
		defer func() {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				logger.Error("failed to rollback transaction", zap.Error(err))
			}
		}()

		dep, _, _, _, err := s.Database.GetLastDeployment(ctx, tx, request.OrgId, *request.Params.ProjectId, *request.Params.EnvId, model.GetLastDeploymentParams{StateChangeOnly: true})
		if err != nil {
			if me, ok := model.IsErrNotFound(err); ok {
				return ListActiveResourceNodes404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(me)}, nil
			}
			return nil, errors.Wrap(err, "failed to get last deployment")
		}

		items, err := s.Database.GetActiveResources(ctx, tx, dep.DeploymentEnvUuid)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get active resources")
		}
		outItems := make([]ActiveResourceNode, 0, len(items))
		for _, x := range items {
			if x.Edges == nil {
				x.Edges = make(map[string]string)
			}
			outItems = append(outItems, ActiveResourceNode{
				ProjectId:     dep.ProjectId,
				EnvId:         dep.EnvId,
				Id:            x.Hash,
				ResourceType:  x.ResourceType,
				ResourceClass: x.ResourceClass,
				ResourceId:    x.ResourceId,
				DeploymentId:  x.LastDeploymentId,
				ModuleId:      x.LastModuleDefinitionId,
				ModuleVersion: x.LastModuleDefinitionVersion,
				Edges:         x.Edges,
				Metadata:      x.Metadata,
			})
		}
		slices.SortFunc(outItems, func(a, b ActiveResourceNode) int {
			return strings.Compare(a.Id, b.Id)
		})
		return ListActiveResourceNodes200JSONResponse(ListActiveResourceNodesPage{Items: outItems}), nil
	}
}

func (s *Server) InternalCheckModuleUsage(ctx context.Context, request InternalCheckModuleUsageRequestObject) (InternalCheckModuleUsageResponseObject, error) {
	logger := hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).With(logging.ZapOrgId(request.OrgId))
	out := InternalCheckModuleUsage200JSONResponse{EnvIdsByProjectId: make(map[string][]string), Items: []InternalModuleUsageItem{}, ObservedAt: time.Now().UTC()}
	if tx, err := s.Database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true}); err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
	} else {
		defer func() {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				logger.Error("failed to rollback transaction", zap.Error(err))
			}
		}()
		usage, err := s.Database.ListModuleVersionUsage(ctx, tx, request.OrgId, request.ModuleId, opt.OfRef(request.Params.ModuleVersion))
		if err != nil {
			return nil, errors.Wrap(err, "failed to list Module Version usage")
		}
		for _, item := range usage {
			if !slices.Contains(out.EnvIdsByProjectId[item.ProjectID], item.EnvironmentID) {
				out.EnvIdsByProjectId[item.ProjectID] = append(out.EnvIdsByProjectId[item.ProjectID], item.EnvironmentID)
			}
			out.Items = append(out.Items, InternalModuleUsageItem{
				ProjectId: item.ProjectID, EnvId: item.EnvironmentID, EnvironmentUuid: item.EnvironmentUUID,
				ModuleVersion: item.ModuleVersion, DeploymentId: item.DeploymentID, ObservedAt: item.ObservedAt,
			})
		}
	}
	return out, nil
}
