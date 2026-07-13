package api

import (
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hrabbitmq"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	mockplatformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratorcp/mocks"
	mockplatformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratoriam/mocks"
	mockmodel "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model/mocks"
	mock_vault "github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/completionhooks"
)

func MockServer(t *testing.T) (*echo.Echo, *Server, func()) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, _ := hecho.DefaultEchoServerWithValidation(&hecho.ValidatedServerConfig{
		AppName:          "test",
		Logger:           zaptest.NewLogger(t),
		OpenAPIRawSchema: MustDecodeOpenApiSpec(),
	})
	db := mockmodel.NewMockDatabaser(ctrl)
	tx := mockmodel.NewMockTxWithCommit(ctrl)
	db.EXPECT().BeginTx(gomock.Any(), gomock.Any()).Return(tx, nil).AnyTimes()
	tx.EXPECT().Rollback().Return(nil).AnyTimes()
	tx.EXPECT().Commit().Return(nil).AnyTimes()
	cpClient := mockplatformorchestratorcp.NewMockClientWithResponsesInterface(ctrl)
	vault := mock_vault.NewMockVaultClientInterface(ctrl)
	s := &Server{
		Logger:                     zaptest.NewLogger(t),
		Database:                   db,
		ControlPlaneClient:         cpClient,
		RabbitMqPublisher:          new(hrabbitmq.NoOpPublisher),
		DeploymentCompletedHooks:   new(completionhooks.CompletionHooks[completionhooks.DeploymentOrgAndId, struct{}]),
		Vault:                      vault,
		RemoteRunnerCompletedHooks: new(completionhooks.CompletionHooks[completionhooks.RunnerAndOrgId, completionhooks.RunnerMessage]),
		IamClient:                  mockplatformorchestratoriam.NewMockClientWithResponsesInterface(ctrl),
	}
	s.MapRoutes(e)
	return e, s, func() {
		ctrl.Finish()
	}
}
