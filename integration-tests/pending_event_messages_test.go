package integrationtests

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPendingEventMessages verifies that a persisted outbox event is published
// to NATS JetStream and removed only after the server acknowledges it.
func TestPendingEventMessages(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	subject := fmt.Sprintf("io.platform-orchestrator.integration.%s", rand.Text())
	conn := MustNATSConn(t)
	subscription, err := conn.SubscribeSync(subject)
	require.NoError(t, err)
	require.NoError(t, conn.Flush())

	db := MustDatabaseConn(t)
	require.NotNil(t, db)
	messages, err := hstandardoutbox.InsertPendingEventMessages(ctx, hstandardoutbox.SQLContextAsReliableOutbox(db), []*hstandardoutbox.PendingEventMessage{
		{Subject: subject, Payload: []byte(`{"message": "hello world"}`)},
	})
	require.NoError(t, err)
	messageID := messages[0].Id
	assert.Positive(t, messageID)

	response, err := http.Post(os.Getenv("INTERNAL_DP_URL")+"/internal/actions/flush-pending-messages", "", nil) //nolint:gosec
	require.NoError(t, err)
	defer func() { assert.NoError(t, response.Body.Close()) }()
	require.Equal(t, http.StatusOK, response.StatusCode)

	received, err := subscription.NextMsgWithContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("platform-orchestrator-dp-%d", messageID), received.Header.Get(nats.MsgIdHdr))
	assert.JSONEq(t, `{"message": "hello world"}`, string(received.Data))

	for {
		pending, more, err := hstandardoutbox.SQLContextAsReliableOutbox(db).LoadPage(t.Context())
		require.NoError(t, err)
		for _, message := range pending {
			assert.NotEqual(t, messageID, message.Id)
		}
		if !more {
			break
		}
	}
}
