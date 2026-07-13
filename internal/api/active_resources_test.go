package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	mockmodel "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestQueryModuleUsage_none(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	s.Database.(*mockmodel.MockDatabaser).EXPECT().ListLastDeploymentsByNodeProperties(gomock.Any(), gomock.Not(nil), "my-org", "", 100, model.ListLastDeploymentsByNodePropertiesParams{
		ModuleId: opt.Of("my-module"),
	}).Return([]model.DeploymentSummary{}, "", nil)

	r, err := s.InternalCheckModuleUsage(t.Context(), InternalCheckModuleUsageRequestObject{
		OrgId: "my-org", ModuleId: "my-module",
	})
	require.NoError(t, err)
	require.IsType(t, InternalCheckModuleUsage200JSONResponse{}, r)
	r200 := r.(InternalCheckModuleUsage200JSONResponse)
	require.Equal(t, map[string][]string{}, r200.EnvIdsByProjectId)
}

func TestQueryModuleUsage_some(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	s.Database.(*mockmodel.MockDatabaser).EXPECT().ListLastDeploymentsByNodeProperties(gomock.Any(), gomock.Not(nil), "my-org", "", 100, model.ListLastDeploymentsByNodePropertiesParams{
		ModuleId: opt.Of("my-module"),
	}).Return([]model.DeploymentSummary{
		{ProjectId: "a", EnvId: "a"},
		{ProjectId: "a", EnvId: "b"},
		{ProjectId: "b", EnvId: "c"},
	}, "first", nil)
	s.Database.(*mockmodel.MockDatabaser).EXPECT().ListLastDeploymentsByNodeProperties(gomock.Any(), gomock.Not(nil), "my-org", "first", 100, model.ListLastDeploymentsByNodePropertiesParams{
		ModuleId: opt.Of("my-module"),
	}).Return([]model.DeploymentSummary{}, "", nil)

	r, err := s.InternalCheckModuleUsage(t.Context(), InternalCheckModuleUsageRequestObject{
		OrgId: "my-org", ModuleId: "my-module",
	})
	require.NoError(t, err)
	require.IsType(t, InternalCheckModuleUsage200JSONResponse{}, r)
	r200 := r.(InternalCheckModuleUsage200JSONResponse)
	require.Equal(t, map[string][]string{
		"a": {"a", "b"},
		"b": {"c"},
	}, r200.EnvIdsByProjectId)
}
