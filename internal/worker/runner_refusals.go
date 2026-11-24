package worker

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hnats"
)

const runnerRefusalsConsumerDurable = "platform-orchestrator-dp-runner-refusals"

func (w *Worker) BuildRunnerRefusalsConsumer(ctx context.Context, apply hmessaging.Handler) (*hnats.Consumer, error) {
	stream, err := w.JetStream.Stream(ctx, hmessaging.DeadLettersStreamName)
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	if info.Config.Retention != jetstream.LimitsPolicy {
		return nil, fmt.Errorf("runner refusal diagnostics require retained dead-letter history")
	}
	consumer, err := w.JetStream.CreateOrUpdateConsumer(ctx, hmessaging.DeadLettersStreamName, jetstream.ConsumerConfig{
		Durable: runnerRefusalsConsumerDurable, FilterSubject: "po.dlq.po.v1.orgs.*.runners.*.commands",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: MainConsumerAckWait, MaxDeliver: -1, MaxAckPending: MainConsumerConcurrency,
	})
	if err != nil {
		return nil, err
	}
	// A failed diagnostic stays pending. Re-dead-lettering it would create a loop.
	return hnats.NewConsumer(consumer, apply, hnats.ProcessingConfig{Logger: w.Logger})
}
