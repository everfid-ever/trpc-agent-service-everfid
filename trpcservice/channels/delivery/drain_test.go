package delivery

import (
	"context"
	"errors"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"sync/atomic"
	"testing"
	"time"
)

type drainReplyQueue struct {
	readStopped chan struct{}
	acks        atomic.Int32
}

func (q *drainReplyQueue) ConsumeReplies(ctx context.Context, d channel.ReplyDestination, _ channel.ReplyConsumerOptions, handle func(context.Context, channel.ReplyDelivery) error) error {
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-ctx.Done():
			close(q.readStopped)
		case <-finished:
		}
	}()
	if err := handle(ctx, channel.ReplyDelivery{ID: "first", Destination: d}); err != nil {
		return err
	}
	if err := handle(ctx, channel.ReplyDelivery{ID: "second", Destination: d}); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
func (q *drainReplyQueue) ReclaimReplies(context.Context, channel.ReplyDestination, channel.ReplyConsumerOptions) ([]channel.ReplyDelivery, error) {
	return nil, nil
}
func (q *drainReplyQueue) AckReply(ctx context.Context, _ channel.ReplyDestination, _ channel.ReplyDelivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.acks.Add(1)
	return nil
}

type drainDeliverer struct {
	started, release chan struct{}
	calls            atomic.Int32
}

func (d *drainDeliverer) Deliver(ctx context.Context, _ channel.ReplyEvent) error {
	if d.calls.Add(1) == 1 {
		close(d.started)
	}
	select {
	case <-d.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestSupervisorDrainsActiveDeliveryWithoutRestart(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "wecom", ChannelBindingID: "binding", ExternalAccountID: "corp"}
	queue := &drainReplyQueue{readStopped: make(chan struct{})}
	deliverer := &drainDeliverer{started: make(chan struct{}), release: make(chan struct{})}
	signal := make(chan struct{})
	var starts atomic.Int32
	supervisor := Supervisor{Catalog: &catalogStub{destinations: []channel.ReplyDestination{destination}}, RefreshInterval: time.Millisecond, Drain: signal,
		NewConsumer: func(d channel.ReplyDestination) (ConsumerRunner, error) {
			starts.Add(1)
			return Consumer{Queue: queue, Deliverer: deliverer, Destination: d, ConsumerID: "consumer", ReclaimInterval: time.Millisecond}, nil
		}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	select {
	case <-deliverer.started:
	case <-ctx.Done():
		t.Fatal("delivery never started")
	}
	close(signal)
	select {
	case <-queue.readStopped:
	case <-ctx.Done():
		t.Fatal("read loop did not stop")
	}
	select {
	case err := <-done:
		t.Fatalf("canceled active send: %v", err)
	default:
	}
	close(deliverer.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("drain did not finish")
	}
	if starts.Load() != 1 || deliverer.calls.Load() != 1 || queue.acks.Load() != 1 {
		t.Fatalf("starts=%d calls=%d acks=%d", starts.Load(), deliverer.calls.Load(), queue.acks.Load())
	}
}
func TestConsumerDrainDeadlineLeavesUnfinishedSendUnacknowledged(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "wecom", ChannelBindingID: "binding", ExternalAccountID: "corp"}
	queue := &drainReplyQueue{readStopped: make(chan struct{})}
	deliverer := &drainDeliverer{started: make(chan struct{}), release: make(chan struct{})}
	signal := make(chan struct{})
	consumer := Consumer{Queue: queue, Deliverer: deliverer, Destination: destination, ConsumerID: "consumer", Drain: signal, DrainTimeout: 20 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	select {
	case <-deliverer.started:
	case <-ctx.Done():
		t.Fatal("send not started")
	}
	close(signal)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("deadline failed")
	}
	if queue.acks.Load() != 0 || deliverer.calls.Load() != 1 {
		t.Fatal("acknowledged or repeated incomplete delivery")
	}
}
