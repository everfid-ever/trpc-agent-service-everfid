package relay

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"testing"
	"time"
)

type drainingWakeupQueue struct {
	wakeupQueueStub
	stopped chan struct{}
}

func (q *drainingWakeupQueue) Consume(ctx context.Context, _ WakeupConsumerOptions, handle func(context.Context, WakeupDelivery) error) error {
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-ctx.Done():
			close(q.stopped)
		case <-finished:
		}
	}()
	if err := handle(ctx, WakeupDelivery{ID: "first", Event: WakeupEvent{TenantID: "tenant", AggregateID: "request"}}); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

type drainingWakeupBroker struct {
	wakeupBrokerStub
	started, release chan struct{}
}

func (b *drainingWakeupBroker) Publish(ctx context.Context, shard broker.Shard, e runtime.ExecutionEnvelope) error {
	close(b.started)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.release:
	}
	return b.wakeupBrokerStub.Publish(ctx, shard, e)
}
func TestWakeupDrainFinishesActivationBeforeAcknowledging(t *testing.T) {
	envelope := runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session", RequestID: "request"}
	queue := &drainingWakeupQueue{stopped: make(chan struct{})}
	dispatch := &drainingWakeupBroker{started: make(chan struct{}), release: make(chan struct{})}
	store := &wakeupStoreStub{candidate: gateway.WakeupCandidate{Execution: gateway.ExecutionStatus{Envelope: envelope, Outcome: runtime.OutcomePending}, Ready: true, Version: 4}}
	signal := make(chan struct{})
	dispatcher := WakeupDispatcher{ConsumerID: "consumer", Wakeups: queue, Store: store, Dispatch: dispatch, ShardCount: 4, Drain: signal}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- dispatcher.Run(ctx) }()
	select {
	case <-dispatch.started:
	case <-ctx.Done():
		t.Fatal("activation did not start")
	}
	close(signal)
	select {
	case <-queue.stopped:
	case <-ctx.Done():
		t.Fatal("queue did not stop reading")
	}
	select {
	case err := <-done:
		t.Fatalf("active activation canceled: %v", err)
	default:
	}
	close(dispatch.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("activation did not finish")
	}
	if !store.marked || !queue.acked || dispatch.calls != 1 {
		t.Fatal("activation lost or duplicated during drain")
	}
}
