package integrationtests

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stellwerk-labs/golib/hrabbitmq"
	"github.com/stellwerk-labs/golib/hstandardreliableoutbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wagslane/go-rabbitmq"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
)

// TestPendingEventMessages ensures that the logic around inserting a pending message and publishing it works
// as expected.
func TestPendingEventMessages(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// First - connect to rabbitmq and listen on a random routing key with a consumer bound to this.
	rk := fmt.Sprintf("rk-%s", rand.Text())
	received := make(chan rabbitmq.Delivery)
	{
		conn := MustRabbitConn(t)
		cons, err := hrabbitmq.NewConsumerWithHandlerWaiter(
			conn,
			func(d rabbitmq.Delivery) (action rabbitmq.Action) {
				select {
				case received <- d:
				case <-ctx.Done():
					assert.Fail(t, "timeout")
				}
				return rabbitmq.Ack
			},
			rk,
			rabbitmq.WithConsumerOptionsExchangeName(events.DefaultExchange),
			rabbitmq.WithConsumerOptionsConsumerAutoAck(true),
			rabbitmq.WithConsumerOptionsQueueAutoDelete,
			rabbitmq.WithConsumerOptionsRoutingKey(rk),
		)
		require.NoError(t, err)
		go func() {
			assert.NoError(t, cons.Run())
		}()

		defer func() {
			assert.NoError(t, cons.Close(ctx))
		}()
	}

	// Wait 3 seconds until we're confident the consumer is fully ready and the queue is prepared in rabbitmq
	time.Sleep(3 * time.Second)

	// Then - connect to the database and insert a pending event message with the random routing key
	db := MustDatabaseConn(t)
	require.NotNil(t, db)
	msgs, err := hstandardreliableoutbox.InsertPendingEventMessages(ctx, hstandardreliableoutbox.SqlContextAsReliableOutbox(db), []*hstandardreliableoutbox.PendingEventMessage{
		{Exchange: events.DefaultExchange, RoutingKey: rk, Payload: []byte(`{"message": "hello world"}`)},
	})
	require.NoError(t, err)
	msgId := msgs[0].Id
	assert.Positive(t, msgId)

	// Then - to ensure the messages are published and we don't have to wait 60 seconds for the publish loop to come
	// around, we can trigger it specifically.
	res, err := http.Post(os.Getenv("INTERNAL_DP_URL")+"/internal/actions/flush-pending-messages", "", nil) //nolint:gosec
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, res.Body.Close())
	}()
	require.Equal(t, http.StatusOK, res.StatusCode)

	// And finally let's wait for our consumer to pick up the message!
	select {
	case d := <-received:
		assert.Equal(t, fmt.Sprintf("platform-orchestrator-dp-%d", msgId), d.MessageId)
		assert.JSONEq(t, `{"message": "hello world"}`, string(d.Body))
	case <-ctx.Done():
		assert.Fail(t, "timeout")
	}

	// And check that the pending message doesn't exist any-more
	for {
		p, m, err := hstandardreliableoutbox.SqlContextAsReliableOutbox(db).LoadPage(t.Context())
		require.NoError(t, err)
		for _, message := range p {
			assert.NotEqual(t, msgId, message.Id)
		}
		if !m {
			break
		}
	}
}
