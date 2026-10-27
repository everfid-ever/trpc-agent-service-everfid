package delivery

import (
	"context"
	"errors"
	"sync"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reliability/drain"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type EventDeliverer interface {
	Deliver(context.Context, channel.ReplyEvent) error
}

// Consumer owns one tenant/binding/account Reply Stream. Provider errors leave
// the Redis entry pending; the durable Delivery Ledger decides whether a
// reclaimed delivery is sent, deferred, reconciled, or ACKed as already done.
type Consumer struct {
	Queue           channel.ReplyQueue
	Deliverer       EventDeliverer
	Destination     channel.ReplyDestination
	ConsumerID      string
	ReclaimInterval time.Duration
	ReclaimLimit    int
	OnDeliveryError func(channel.ReplyDelivery, error)
	OnPoll          func(error)
	Telemetry       telemetry.Provider
	Drain           <-chan struct{}
	DrainTimeout    time.Duration
}

// WithPollObserver lets the supervisor wait for real queue access rather than
// considering a newly launched account consumer ready immediately.
func (c Consumer) WithPollObserver(observe func(error)) ConsumerRunner {
	previous := c.OnPoll
	c.OnPoll = func(err error) {
		if previous != nil {
			previous(err)
		}
		observe(err)
	}
	return c
}

func (c Consumer) WithDrain(signal <-chan struct{}) ConsumerRunner { c.Drain = signal; return c }

func (c Consumer) Run(ctx context.Context) error {
	if err := c.validate(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if drain.Signaled(c.Drain) {
		return nil
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	runCtx, cancel := context.WithCancel(workCtx)
	defer cancel()
	gate := drain.New()
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-finished:
			return
		case <-ctx.Done():
			return
		case <-c.Drain:
			gate.Begin()
			cancel()
		}
		timeout := c.DrainTimeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-finished:
		case <-ctx.Done():
		case <-timer.C:
			cancelWork()
		}
	}()
	process := func(_ context.Context, delivery channel.ReplyDelivery) error {
		leave, ok := gate.Enter()
		if !ok {
			return context.Canceled
		}
		defer leave()
		if drain.Signaled(c.Drain) {
			return context.Canceled
		}
		return c.process(workCtx, delivery)
	}
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		errCh <- c.Queue.ConsumeReplies(runCtx, c.Destination, channel.ReplyConsumerOptions{ConsumerID: c.ConsumerID}, process)
	}()
	go func() {
		defer wg.Done()
		errCh <- c.reclaimLoop(runCtx, process)
	}()
	err := <-errCh
	cancel()
	if !drain.Signaled(c.Drain) {
		cancelWork()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if drain.Signaled(c.Drain) {
		return workCtx.Err()
	}
	return err
}

func (c Consumer) ReclaimOnce(ctx context.Context) (int, error) {
	return c.reclaimOnce(ctx, c.process)
}

func (c Consumer) reclaimOnce(ctx context.Context, process func(context.Context, channel.ReplyDelivery) error) (int, error) {
	if err := c.validate(); err != nil {
		return 0, err
	}
	deliveries, err := c.Queue.ReclaimReplies(ctx, c.Destination, channel.ReplyConsumerOptions{ConsumerID: c.ConsumerID, Limit: c.ReclaimLimit})
	if c.OnPoll != nil {
		c.OnPoll(err)
	}
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, delivery := range deliveries {
		if ctx.Err() != nil || drain.Signaled(c.Drain) {
			return processed, context.Canceled
		}
		if err := process(ctx, delivery); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func (c Consumer) reclaimLoop(ctx context.Context, process func(context.Context, channel.ReplyDelivery) error) error {
	interval := c.ReclaimInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := c.reclaimOnce(ctx, process); err != nil {
				return err
			}
		}
	}
}

func (c Consumer) process(ctx context.Context, delivery channel.ReplyDelivery) (resultErr error) {
	ctx, finish := telemetry.StartOperation(ctx, c.Telemetry, delivery.Event.TraceParent, telemetry.OperationChannelDeliver,
		telemetry.ComponentAttribute(telemetry.ComponentChannelDelivery))
	defer func() { finish(resultErr) }()
	if delivery.Destination != c.Destination {
		return runtime.ErrTenantScope
	}
	if err := c.Deliverer.Deliver(ctx, delivery.Event); err != nil {
		if c.OnDeliveryError != nil {
			c.OnDeliveryError(delivery, err)
		}
		var terminal TerminalError
		if errors.As(err, &terminal) {
			return c.Queue.AckReply(ctx, c.Destination, delivery)
		}
		// Retryable/ambiguous failures are durable state. Leave the entry
		// pending and continue consuming unrelated account replies.
		return nil
	}
	return c.Queue.AckReply(ctx, c.Destination, delivery)
}

func (c Consumer) validate() error {
	if c.Queue == nil || c.Deliverer == nil || c.ConsumerID == "" || c.Destination.TenantID == "" ||
		c.Destination.Channel == "" || c.Destination.ChannelBindingID == "" || c.Destination.ExternalAccountID == "" {
		return runtime.ErrInvariantViolation
	}
	return nil
}
