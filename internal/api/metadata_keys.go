package api

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/logging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

func (s *Server) CreateMetadataKey(ctx context.Context, request CreateMetadataKeyRequestObject) (CreateMetadataKeyResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgManageAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	metadataKey := toMetadataKeyModel(*request.Body)

	if ok, err := checkIfOrganizationExists(ctx, s.ControlPlaneClient, request.OrgId); err != nil {
		return nil, errors.Wrap(err, "failed to check if organization exists")
	} else if !ok {
		return CreateMetadataKey404JSONResponse{Generate404FromModelErr(model.ErrNotFound{
			Message: fmt.Sprintf("organization %s not found", request.OrgId),
		})}, nil
	}

	createdMetadataKey, err := s.Database.CreateMetadataKey(ctx, nil, request.OrgId, &metadataKey)
	if err != nil {
		if e, ok := model.IsErrConflict(err); ok {
			return CreateMetadataKey409JSONResponse{Generate409FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to create metadata key")
	}

	return CreateMetadataKey201JSONResponse(fromMetadataKeyModel(*createdMetadataKey)), nil
}

func (s *Server) GetMetadataKey(ctx context.Context, request GetMetadataKeyRequestObject) (GetMetadataKeyResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgReadAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	metadataKey, err := s.Database.GetMetadataKey(ctx, nil, request.OrgId, request.MetadataKeyName)
	if err != nil {
		if e, ok := model.IsErrNotFound(err); ok {
			return GetMetadataKey404JSONResponse{Generate404FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to get metadata key")
	}

	return GetMetadataKey200JSONResponse(fromMetadataKeyModel(*metadataKey)), nil
}

func (s *Server) ListMetadataKeys(ctx context.Context, request ListMetadataKeysRequestObject) (ListMetadataKeysResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgReadAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	if ok, err := checkIfOrganizationExists(ctx, s.ControlPlaneClient, request.OrgId); err != nil {
		return nil, errors.Wrap(err, "failed to check if organization exists")
	} else if !ok {
		return ListMetadataKeys404JSONResponse{Generate404FromModelErr(model.ErrNotFound{
			Message: fmt.Sprintf("organization %s not found", request.OrgId),
		})}, nil
	}

	metadataKeys, npt, err := s.Database.ListMetadataKeys(ctx, nil, request.OrgId, opt.OfRef(request.Params.Page).Or(""), opt.OfRef(request.Params.PerPage).Or(defaultPaginationSize))
	if err != nil {
		return nil, errors.Wrap(err, "failed to list metadata keys")
	}

	response := make([]MetadataKey, len(metadataKeys))
	for i, key := range metadataKeys {
		response[i] = fromMetadataKeyModel(*key)
	}

	return ListMetadataKeys200JSONResponse(MetadataKeyPage{
		Items:         response,
		NextPageToken: ref.RefStringEmptyNil(npt),
	}), nil
}

func (s *Server) UpdateMetadataKey(ctx context.Context, request UpdateMetadataKeyRequestObject) (UpdateMetadataKeyResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgManageAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	logger := hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).
		With(logging.ZapOrgId(request.OrgId))

	if tx, err := s.Database.BeginTx(ctx, nil); err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
	} else {
		defer func() {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				logger.Error("failed to rollback transaction", zap.Error(err))
			}
		}()

		metadataKey, err := s.Database.GetMetadataKey(ctx, tx, request.OrgId, request.MetadataKeyName)
		if err != nil {
			if e, ok := model.IsErrNotFound(err); ok {
				return UpdateMetadataKey404JSONResponse{Generate404FromModelErr(e)}, nil
			}
			return nil, errors.Wrap(err, "failed to get metadata key")
		}

		if request.Body.Description != nil {
			metadataKey.Description = request.Body.Description
		}
		if request.Body.Schema != nil {
			if request.Body.Schema.Type != nil {
				metadataKey.Schema.Type = string(ref.DerefOr(request.Body.Schema.Type, UpdateMetadataKeySchemaTypeString))
			}
			if request.Body.Schema.Format != nil {
				metadataKey.Schema.Format = request.Body.Schema.Format
			}
			if request.Body.Schema.Pattern != nil {
				metadataKey.Schema.Pattern = request.Body.Schema.Pattern
			}
		}

		err = s.Database.UpdateMetadataKey(ctx, tx, request.OrgId, metadataKey)
		if err != nil {
			if e, ok := model.IsErrNotFound(err); ok {
				return UpdateMetadataKey404JSONResponse{Generate404FromModelErr(e)}, nil
			}
			return nil, errors.Wrap(err, "failed to update metadata key")
		} else {
			if err := tx.Commit(); err != nil {
				return nil, errors.Wrap(err, "failed to commit transaction")
			}
		}
		return UpdateMetadataKey200JSONResponse(fromMetadataKeyModel(*metadataKey)), nil
	}
}

func (s *Server) DeleteMetadataKey(ctx context.Context, request DeleteMetadataKeyRequestObject) (DeleteMetadataKeyResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgManageAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	err := s.Database.DeleteMetadataKey(ctx, nil, request.OrgId, request.MetadataKeyName)
	if err != nil {
		if e, ok := model.IsErrNotFound(err); ok {
			return DeleteMetadataKey404JSONResponse{Generate404FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to delete metadata key")
	}
	return DeleteMetadataKey204Response{}, nil
}

func toMetadataKeyModel(key MetadataKeyCreateBody) model.MetadataKey {
	return model.MetadataKey{
		Name:        key.Name,
		Description: key.Description,
		Schema: model.MetadataKeySchema{
			Type:    string(key.Schema.Type),
			Format:  key.Schema.Format,
			Pattern: key.Schema.Pattern,
		},
	}
}

func fromMetadataKeyModel(key model.MetadataKey) MetadataKey {
	return MetadataKey{
		Name:        key.Name,
		Description: key.Description,
		CreatedAt:   key.CreatedAt,
		Schema: MetadataKeySchema{
			Type:    MetadataKeySchemaType(key.Schema.Type),
			Format:  key.Schema.Format,
			Pattern: key.Schema.Pattern,
		},
	}
}
