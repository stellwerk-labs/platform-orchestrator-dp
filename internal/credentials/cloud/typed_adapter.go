package cloud

import (
	"context"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

//go:generate go tool mockgen -destination mocks/gcp_mock.go -package=mocks github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/cloud TypedAdapter

// TypedAdapter provides a consistent API for different cloud credentials implementations. The common API is Exchange which
// performs any temporary credential generation or exchange mechanism, and then Check which will validate it and extract
// any particular fields that we can display to the caller as verification of the final credential.
type TypedAdapter interface {
	Exchange(ctx context.Context, orgId, runnerId string, runnerConfig platformorchestratorcp.RunnerConfiguration, args ExchangeArgs) (map[string]interface{}, error)
	GetKubeconfig(ctx context.Context, orgId, runnerId string, runnerConfig platformorchestratorcp.RunnerConfiguration) (*clientcmdapi.Config, error)
	Check(ctx context.Context, outputCredential map[string]interface{}) ([]CheckCredentialsSuccess, []Warning, error)
}

type ExchangeArgs struct {
	AdditionalOAuthScopes []string
}

type Warning string

// CheckCredentialsSuccess A result from the cloud credentials check.
type CheckCredentialsSuccess struct {
	Description string `json:"description"`
	Id          string `json:"id"`
	Value       string `json:"value"`
}

// functorAdapter implements the TypedAdapter by composing two functions.
type functorAdapter struct {
	Exchanger       func(ctx context.Context, orgId, runnerId string, runnerConfig platformorchestratorcp.RunnerConfiguration, args ExchangeArgs) (map[string]interface{}, error)
	GetKubeconfiger func(ctx context.Context, orgId, runnerId string, runnerConfig platformorchestratorcp.RunnerConfiguration) (*clientcmdapi.Config, error)
	Checker         func(ctx context.Context, outputCredential map[string]interface{}) ([]CheckCredentialsSuccess, []Warning, error)
}

func (f *functorAdapter) Exchange(ctx context.Context, orgId, runnerId string, runnerConfig platformorchestratorcp.RunnerConfiguration, args ExchangeArgs) (map[string]interface{}, error) {
	return f.Exchanger(ctx, orgId, runnerId, runnerConfig, args)
}

func (f *functorAdapter) Check(ctx context.Context, outputCredential map[string]interface{}) ([]CheckCredentialsSuccess, []Warning, error) {
	return f.Checker(ctx, outputCredential)
}

func (f *functorAdapter) GetKubeconfig(ctx context.Context, orgId, runnerId string, runnerConfig platformorchestratorcp.RunnerConfiguration) (*clientcmdapi.Config, error) {
	return f.GetKubeconfiger(ctx, orgId, runnerId, runnerConfig)
}

var _ TypedAdapter = (*functorAdapter)(nil)
