package runners

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hmessaging"
)

type NATSRemoteRunnerCommandPublisher struct {
	Publisher hmessaging.Publisher
}

func (p *NATSRemoteRunnerCommandPublisher) PublishCreateJob(
	ctx context.Context,
	orgID string,
	runnerID string,
	deploymentID string,
	deploymentRevision int,
	expiresAt time.Time,
	command hmessaging.CreateJobCommand,
) error {
	if p == nil || p.Publisher == nil {
		return errors.New("NATS command publisher is not configured")
	}
	subject, err := hmessaging.RunnerCommandSubject(orgID, runnerID)
	if err != nil {
		return errors.Wrap(err, "failed to build runner command subject")
	}
	payload, err := json.Marshal(command)
	if err != nil {
		return errors.Wrap(err, "failed to marshal create-job payload")
	}
	createdAt := time.Now().UTC()
	commandID := fmt.Sprintf("create-job:%s:%d", deploymentID, deploymentRevision)
	envelope, err := json.Marshal(hmessaging.CommandEnvelope{
		ProtocolVersion: hmessaging.ProtocolVersionV1,
		CommandID:       commandID,
		OrganizationID:  orgID,
		RunnerID:        runnerID,
		DeploymentID:    deploymentID,
		Type:            hmessaging.CommandTypeCreateJob,
		CreatedAt:       createdAt,
		ExpiresAt:       expiresAt,
		Payload:         payload,
	})
	if err != nil {
		return errors.Wrap(err, "failed to marshal create-job command envelope")
	}
	return p.Publisher.Publish(ctx, hmessaging.Message{
		ID:        commandID,
		Subject:   subject,
		Data:      envelope,
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	})
}

var _ RemoteRunnerCommandPublisher = (*NATSRemoteRunnerCommandPublisher)(nil)
