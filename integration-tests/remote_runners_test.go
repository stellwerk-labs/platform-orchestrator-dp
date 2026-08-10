package integrationtests

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hnats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestRemoteRunnerCommandBuffering proves the edge invariant: a command
// published while the runner is disconnected remains available to its durable
// runner-specific consumer when it reconnects.
func TestRemoteRunnerCommandBuffering(t *testing.T) {
	connection := MustNATSConn(t)
	js, err := hnats.NewJetStream(connection)
	require.NoError(t, err)

	runnerID := fmt.Sprintf("runner-%d", time.Now().UnixNano())
	subject, err := hmessaging.RunnerCommandSubject("test-org", runnerID)
	require.NoError(t, err)
	publisher := hnats.NewPublisher(js, hmessaging.RunnerCommandsStreamName, zaptest.NewLogger(t))
	envelope := hmessaging.CommandEnvelope{
		ProtocolVersion: hmessaging.ProtocolVersionV1, CommandID: "command-1",
		OrganizationID: "test-org", RunnerID: runnerID, DeploymentID: "deployment-1",
		Type: hmessaging.CommandTypeCreateJob, CreatedAt: time.Now().UTC(),
		Payload: json.RawMessage(`{"job_id":"deployment-1","namespace":"jobs","configuration":{}}`),
	}
	data, err := json.Marshal(envelope)
	require.NoError(t, err)
	require.NoError(t, publisher.Publish(t.Context(), hmessaging.Message{ID: envelope.CommandID, Subject: subject, Data: data, CreatedAt: envelope.CreatedAt}))

	consumer, err := hnats.EnsureDurableConsumer(t.Context(), js, hnats.DurableConsumerConfig{
		Stream: hmessaging.RunnerCommandsStreamName, Durable: runnerID, FilterSubjects: []string{subject},
		MaxDeliver: 5, AckWait: time.Minute, MaxAckPending: 1,
	})
	require.NoError(t, err)
	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	require.NoError(t, err)
	messages := batch.Messages()
	message, ok := <-messages
	require.True(t, ok)
	assert.Equal(t, subject, message.Subject())
	assert.JSONEq(t, string(data), string(message.Data()))
	require.NoError(t, message.Ack())
	require.NoError(t, batch.Error())
}
