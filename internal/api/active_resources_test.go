package api

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	mockmodel "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestQueryModuleUsage_none(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()

	s.Database.(*mockmodel.MockDatabaser).EXPECT().ListModuleVersionUsage(gomock.Any(), gomock.Not(nil), "my-org", "my-module", opt.Empty[string]()).
		Return([]model.ModuleVersionUsage{}, nil)

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

	now := time.Now().UTC()
	usage := []model.ModuleVersionUsage{
		{ProjectID: "a", EnvironmentID: "a", EnvironmentUUID: uuid.New(), ModuleVersion: "1.2.3", DeploymentID: uuid.New(), ObservedAt: now},
		{ProjectID: "a", EnvironmentID: "b", EnvironmentUUID: uuid.New(), ModuleVersion: "1.2.3", DeploymentID: uuid.New(), ObservedAt: now},
		{ProjectID: "b", EnvironmentID: "c", EnvironmentUUID: uuid.New(), ModuleVersion: "2.0.0", DeploymentID: uuid.New(), ObservedAt: now},
	}
	s.Database.(*mockmodel.MockDatabaser).EXPECT().ListModuleVersionUsage(gomock.Any(), gomock.Not(nil), "my-org", "my-module", opt.Empty[string]()).Return(usage, nil)

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
	require.Len(t, r200.Items, 3)
	require.Equal(t, usage[2].DeploymentID, r200.Items[2].DeploymentId)
}
