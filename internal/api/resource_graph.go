package api

import (
	"bytes"
	"context"
	"sort"

	"github.com/pkg/errors"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func (s *Server) ListDeploymentResourceNodes(ctx context.Context, request ListDeploymentResourceNodesRequestObject) (ListDeploymentResourceNodesResponseObject, error) {
	if uid, err := GetAuthenticatedUserIdOr401(ctx); err != nil {
		return nil, err
	} else if err := s.checkOrgReadAuthorization(ctx, uid, request.OrgId); err != nil {
		return nil, err
	}

	dep, _, _, rawGraph, err := s.Database.GetDeployment(ctx, nil, request.OrgId, request.DeploymentId, model.GetModeDefault)
	if err != nil {
		if e, ok := model.IsErrNotFound(err); ok {
			return ListDeploymentResourceNodes404JSONResponse{N404NotFoundJSONResponse: Generate404FromModelErr(e)}, nil
		}
		return nil, errors.Wrap(err, "failed to get deployment")
	}

	graph, err := graphs.FromJson(bytes.NewReader(rawGraph))
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse deployment graph")
	}

	items := resolvedNodesToAPI(graphs.ResolveNodes(graph, dep.DeploymentEnvUuid), *dep)
	sort.Slice(items, func(i, j int) bool { return items[i].Id < items[j].Id })

	return ListDeploymentResourceNodes200JSONResponse(ResourceNodesPage{Items: items}), nil
}

func resolvedNodesToAPI(resolved []graphs.ResolvedNode, dep model.DeploymentSummary) []ActiveResourceNode {
	nodes := make([]ActiveResourceNode, len(resolved))
	for i, n := range resolved {
		nodes[i] = ActiveResourceNode{
			Id:            n.Hash,
			ProjectId:     dep.ProjectId,
			EnvId:         dep.EnvId,
			DeploymentId:  dep.Id,
			ResourceType:  n.ResourceType,
			ResourceClass: n.ResourceClass,
			ResourceId:    n.ResourceId,
			ModuleId:      n.DefinitionId,
			ModuleVersion: n.VersionId,
			Edges:         n.Edges,
			Metadata:      map[string]interface{}{},
		}
	}
	return nodes
}
