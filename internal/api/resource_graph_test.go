package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hecho"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/authz"
	platformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mockplatformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratoriam/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	mockmodel "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

var (
	rcPostgres = platform_orchestrator_graph.ResourceCoordinate{Type: "postgres", Class: "default", Id: "shared.pg"}
	rcNs       = platform_orchestrator_graph.ResourceCoordinate{Type: "k8s-namespace", Class: "default", Id: "shared.ns"}
)

func makeNode(defId, versionId string) platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig] {
	return platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
		ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: defId, VersionId: versionId},
	}
}

func mockOrgReadAuth(t *testing.T, s *Server, userId uuid.UUID, orgId string) {
	t.Helper()
	s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface).
		EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
		UserId: userId,
		Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanReadOrgCheck(orgId)},
	}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil)
}

func TestListDeploymentResourceNodes_unauthenticated(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	// Simulate a request where the From header was present but not a valid UUID.
	// (Completely absent userId panics in middleware before reaching the handler.)
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, "not-a-valid-uuid")

	_, err := s.ListDeploymentResourceNodes(ctx, ListDeploymentResourceNodesRequestObject{
		OrgId:        "my-org",
		DeploymentId: uuid.New(),
	})

	require.Error(t, err)
	httpErr := new(echo.HTTPError)
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, http.StatusUnauthorized, httpErr.Code)
}

func TestListDeploymentResourceNodes_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	userId := userid.NewHumanUserId()
	mockOrgReadAuth(t, s, userId, "my-org")
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	deploymentId := uuid.New()
	s.Database.(*mockmodel.MockDatabaser).EXPECT().
		GetDeployment(gomock.Any(), gomock.Any(), "my-org", deploymentId, model.GetModeDefault).
		Return(nil, nil, nil, nil, model.NewErrNotFound("deployment not found"))

	r, err := s.ListDeploymentResourceNodes(ctx, ListDeploymentResourceNodesRequestObject{
		OrgId:        "my-org",
		DeploymentId: deploymentId,
	})

	require.NoError(t, err)
	require.IsType(t, ListDeploymentResourceNodes404JSONResponse{}, r)
	assert.Equal(t, "deployment not found", r.(ListDeploymentResourceNodes404JSONResponse).Message)
}

func TestListDeploymentResourceNodes_success(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	envUuid := uuid.New()
	depId := uuid.New()
	dep := &model.DeploymentSummary{
		Id:                depId,
		ProjectId:         "my-project",
		EnvId:             "my-env",
		DeploymentEnvUuid: envUuid,
	}
	rawGraph, _ := graphs.ToJson(&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			rcPostgres: makeNode("pg-def", "v1"),
			rcNs:       makeNode("ns-def", "v2"),
		},
		Edges: map[platform_orchestrator_graph.ResourceCoordinate]map[string]platform_orchestrator_graph.ResourceCoordinate{
			rcPostgres: {"ns": rcNs},
		},
	})

	userId := userid.NewHumanUserId()
	mockOrgReadAuth(t, s, userId, "my-org")
	ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

	s.Database.(*mockmodel.MockDatabaser).EXPECT().
		GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).
		Return(dep, nil, nil, rawGraph, nil)

	r, err := s.ListDeploymentResourceNodes(ctx, ListDeploymentResourceNodesRequestObject{
		OrgId:        "my-org",
		DeploymentId: depId,
	})

	require.NoError(t, err)
	require.IsType(t, ListDeploymentResourceNodes200JSONResponse{}, r)

	page := ResourceNodesPage(r.(ListDeploymentResourceNodes200JSONResponse))
	require.Len(t, page.Items, 2)

	// items are sorted by id (hash)
	assert.Less(t, page.Items[0].Id, page.Items[1].Id)

	nsHash := util.GenerateNodeHash(envUuid, rcNs.Type, rcNs.Class, rcNs.Id)
	pgHash := util.GenerateNodeHash(envUuid, rcPostgres.Type, rcPostgres.Class, rcPostgres.Id)
	assert.ElementsMatch(t, []string{nsHash, pgHash}, []string{page.Items[0].Id, page.Items[1].Id})

	// postgres node edge points to namespace node
	for _, item := range page.Items {
		if item.ResourceType == rcPostgres.Type {
			assert.Equal(t, map[string]string{"ns": nsHash}, item.Edges)
		}
	}
}

func TestListDeploymentResourceNodes_all_modes_return_200(t *testing.T) {
	modes := []model.DeploymentMode{
		model.DeploymentModeDeploy,
		model.DeploymentModeDeployPlan,
		model.DeploymentModeRollback,
		model.DeploymentModeRollbackPlan,
		model.DeploymentModeDestroy,
	}

	rawGraph, _ := graphs.ToJson(&platform_orchestrator_graph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[platform_orchestrator_graph.ResourceCoordinate]platform_orchestrator_graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			rcNs: makeNode("ns-def", "v1"),
		},
	})

	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			_, s, fin := MockServer(t)
			defer fin()

			depId := uuid.New()
			dep := &model.DeploymentSummary{
				Id:                depId,
				Mode:              mode,
				ProjectId:         "my-project",
				EnvId:             "my-env",
				DeploymentEnvUuid: uuid.New(),
			}

			userId := userid.NewHumanUserId()
			mockOrgReadAuth(t, s, userId, "my-org")
			ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())

			s.Database.(*mockmodel.MockDatabaser).EXPECT().
				GetDeployment(gomock.Any(), gomock.Any(), "my-org", depId, model.GetModeDefault).
				Return(dep, nil, nil, rawGraph, nil)

			r, err := s.ListDeploymentResourceNodes(ctx, ListDeploymentResourceNodesRequestObject{
				OrgId:        "my-org",
				DeploymentId: depId,
			})

			require.NoError(t, err)
			require.IsType(t, ListDeploymentResourceNodes200JSONResponse{}, r, "mode %q should return 200", mode)
			page := ResourceNodesPage(r.(ListDeploymentResourceNodes200JSONResponse))
			assert.Len(t, page.Items, 1)
		})
	}
}
