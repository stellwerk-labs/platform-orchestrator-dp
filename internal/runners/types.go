//go:generate go tool mockgen -destination=mocks/runners.go -package mock_runners github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners RunnerFactory,RunnerInterface

package runners

import (
	"context"
	"time"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"

	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
)

const KubernetesAgentConnectionIssueTolerance = 120 * time.Second

var (
	// ErrKubernetesAgentNotReachableRetry is retained for local runner
	// implementations and compatibility with callers that still classify it.
	ErrKubernetesAgentNotReachableRetry = errors.New("kubernetes agent not reachable")
	// ErrRunnerCommandPublishRetry means the command has not been durably
	// accepted by JetStream and the source event should be retried.
	ErrRunnerCommandPublishRetry = errors.New("runner command was not durably published")
)

// RunnerInterface defines the common interface for all runner types
type RunnerInterface interface {
	Start(ctx context.Context) error
	IsRunning(ctx context.Context) (bool, error)
	CheckStatus(ctx context.Context) (*RunnerStatus, error)
}

// RunnerStatus represents the current status of a runner
type RunnerStatus struct {
	IsCompleted bool
	IsStuck     bool
	Message     string
}

// RunnerFactory creates RunnerInterface instances for different runner types
type RunnerFactory interface {
	CreateRunner(ctx context.Context, internalRunner platformorchestratorcp.InternalRunner, deploymentSummary *model.DeploymentSummary) (RunnerInterface, error)
}
