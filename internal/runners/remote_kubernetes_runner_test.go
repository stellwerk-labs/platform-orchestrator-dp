package runners

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

type recordingCommandPublisher struct {
	organizationID string
	runnerID       string
	deploymentID   string
	revision       int
	expiresAt      time.Time
	command        hmessaging.CreateJobCommand
	err            error
}

func (p *recordingCommandPublisher) PublishCreateJob(_ context.Context, organizationID, runnerID, deploymentID string, revision int, expiresAt time.Time, command hmessaging.CreateJobCommand) error {
	p.organizationID, p.runnerID, p.deploymentID = organizationID, runnerID, deploymentID
	p.revision, p.expiresAt, p.command = revision, expiresAt, command
	return p.err
}

func TestRemoteKubernetesRunnerQueuesCreateJob(t *testing.T) {
	configuration := new(platformorchestratorcp.RunnerConfiguration)
	require.NoError(t, configuration.FromK8sAgentRunnerConfiguration(platformorchestratorcp.K8sAgentRunnerConfiguration{
		Job: platformorchestratorcp.K8sRunnerJobConfig{Namespace: "runner-jobs", ServiceAccount: "runner"},
	}))
	deployment := &model.DeploymentSummary{
		OrgId: "test-org", RunnerId: "edge-1", Id: uuid.New(), Revision: 3,
		DeploymentEnvUuid: uuid.New(), Mode: model.DeploymentModeDeploy,
	}
	publisher := new(recordingCommandPublisher)
	runner := NewRemoteKubernetesRunner(
		"runner:feat-nats", zaptest.NewLogger(t),
		platformorchestratorcp.InternalRunner{Id: "edge-1", RunnerConfiguration: *configuration},
		deployment, publisher, time.Hour,
	)

	require.NoError(t, runner.Start(t.Context()))
	assert.Equal(t, "test-org", publisher.organizationID)
	assert.Equal(t, "edge-1", publisher.runnerID)
	assert.Equal(t, deployment.Id.String(), publisher.deploymentID)
	assert.Equal(t, 3, publisher.revision)
	assert.WithinDuration(t, time.Now().Add(time.Hour), publisher.expiresAt, 2*time.Second)
	assert.Equal(t, deployment.Id.String(), publisher.command.JobID)
	assert.Equal(t, "runner-jobs", publisher.command.Namespace)
	configurationJSON, err := json.Marshal(publisher.command.Configuration)
	require.NoError(t, err)
	assert.NotContains(t, string(configurationJSON), "NATS_URL", "the remote agent must inject its local broker, never the central endpoint")
}

func TestNATSRemoteRunnerCommandPublisherEnvelope(t *testing.T) {
	recorder := new(hmessaging.RecordingPublisher)
	publisher := &NATSRemoteRunnerCommandPublisher{Publisher: recorder}
	expiresAt := time.Now().UTC().Add(time.Hour)
	command := hmessaging.CreateJobCommand{JobID: "deployment", Namespace: "jobs", Configuration: map[string]interface{}{"spec": "value"}}

	require.NoError(t, publisher.PublishCreateJob(t.Context(), "test-org", "edge-1", "deployment", 4, expiresAt, command))
	messages := recorder.Messages()
	require.Len(t, messages, 1)
	assert.Equal(t, "create-job:deployment:4", messages[0].ID)
	assert.Equal(t, "po.v1.orgs.test-org.runners.edge-1.commands", messages[0].Subject)

	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(messages[0].Data, &envelope))
	assert.Equal(t, hmessaging.ProtocolVersionV1, envelope.ProtocolVersion)
	assert.Equal(t, hmessaging.CommandTypeCreateJob, envelope.Type)
	assert.Equal(t, "create-job:deployment:4", envelope.CommandID)
	assert.Equal(t, "test-org", envelope.OrganizationID)
	assert.Equal(t, "edge-1", envelope.RunnerID)
	assert.Equal(t, "deployment", envelope.DeploymentID)
	assert.Equal(t, expiresAt, envelope.ExpiresAt)
	assert.JSONEq(t, `{"job_id":"deployment","namespace":"jobs","configuration":{"spec":"value"}}`, string(envelope.Payload))
}
