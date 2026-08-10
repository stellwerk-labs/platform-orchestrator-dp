package runners

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hmessaging"
	"go.uber.org/zap"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

// RemoteRunnerCommandPublisher durably submits work for one remote runner. The
// implementation is backed by a runner-specific JetStream subject. Keeping the
// broker outside RemoteKubernetesRunner makes job construction independently
// testable and prevents transport credentials from entering Kubernetes Job
// specifications.
type RemoteRunnerCommandPublisher interface {
	PublishCreateJob(
		ctx context.Context,
		orgID string,
		runnerID string,
		deploymentID string,
		deploymentRevision int,
		expiresAt time.Time,
		message hmessaging.CreateJobCommand,
	) error
}

type RemoteKubernetesRunner struct {
	runnerImage       string
	logger            *zap.Logger
	internalRunner    platformorchestratorcp.InternalRunner
	deploymentSummary *model.DeploymentSummary
	commandPublisher  RemoteRunnerCommandPublisher
	commandTTL        time.Duration
}

func NewRemoteKubernetesRunner(
	runnerImage string,
	logger *zap.Logger,
	internalRunner platformorchestratorcp.InternalRunner,
	deploymentSummary *model.DeploymentSummary,
	commandPublisher RemoteRunnerCommandPublisher,
	commandTTL time.Duration,
) *RemoteKubernetesRunner {
	return &RemoteKubernetesRunner{
		runnerImage:       runnerImage,
		logger:            logger,
		internalRunner:    internalRunner,
		deploymentSummary: deploymentSummary,
		commandPublisher:  commandPublisher,
		commandTTL:        commandTTL,
	}
}

func (r *RemoteKubernetesRunner) Start(ctx context.Context) error {
	jobCfg, err := getJobConfiguration(r.internalRunner.RunnerConfiguration)
	if err != nil {
		return errors.Wrap(err, "failed to get job configuration from runner configuration")
	}

	jobSpec, err := kubernetes.GetJobSpec(
		ctx,
		jobCfg,
		r.runnerImage,
		"",
		r.deploymentSummary,
	)
	if err != nil {
		return errors.Wrap(err, "failed to build job spec")
	}
	jobSpecJSON, err := json.Marshal(jobSpec)
	if err != nil {
		return errors.Wrap(err, "failed to marshal job spec")
	}
	var jobSpecMap map[string]interface{}
	if err := json.Unmarshal(jobSpecJSON, &jobSpecMap); err != nil {
		return errors.Wrap(err, "failed to unmarshal job spec JSON")
	}

	if r.commandPublisher == nil {
		return errors.New("remote runner command publisher is not configured")
	}

	message := hmessaging.CreateJobCommand{
		JobID:         r.deploymentSummary.Id.String(),
		Namespace:     jobCfg.Namespace,
		Configuration: jobSpecMap,
	}
	expiresAt := time.Now().UTC().Add(r.commandTTL)
	if err := r.commandPublisher.PublishCreateJob(
		ctx,
		r.deploymentSummary.OrgId,
		r.deploymentSummary.RunnerId,
		r.deploymentSummary.Id.String(),
		r.deploymentSummary.Revision,
		expiresAt,
		message,
	); err != nil {
		return errors.Wrap(ErrRunnerCommandPublishRetry, err.Error())
	}

	r.logger.Info("queued create-job command for remote runner", zap.Time("expires_at", expiresAt))
	return nil
}

func (r *RemoteKubernetesRunner) IsRunning(context.Context) (bool, error) {
	// The create command is idempotent and a redelivery is safe. The runner
	// acknowledges it only after Kubernetes reports Created or AlreadyExists.
	return false, nil
}

func (r *RemoteKubernetesRunner) CheckStatus(context.Context) (*RunnerStatus, error) {
	// Remote runners publish lifecycle events. Polling them would reintroduce a
	// second command protocol and creates unbounded stale status commands while
	// an edge is disconnected.
	return &RunnerStatus{Message: "remote runner status is event-driven"}, nil
}
