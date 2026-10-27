package relay

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reliability/drain"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type WakeupDelivery struct {
	ID    string
	Event WakeupEvent
}

type WakeupConsumerOptions struct{ ConsumerID string }
type WakeupReclaimOptions struct {
	ConsumerID string
	Limit      int
}

type WakeupQueue interface {
	Consume(context.Context, WakeupConsumerOptions, func(context.Context, WakeupDelivery) error) error
	AckWakeup(context.Context, WakeupDelivery) error
	ReclaimWakeups(context.Context, WakeupReclaimOptions) ([]WakeupDelivery, error)
}

type WakeupActivationDisposition string

const (
	WakeupActivated WakeupActivationDisposition = "activated"
	WakeupNotReady  WakeupActivationDisposition = "not_ready"
	WakeupObsolete  WakeupActivationDisposition = "obsolete"
	WakeupBlocked   WakeupActivationDisposition = "blocked"
)

// WakeupActivator is the single typed pending->queued transition used by the
// Redis consumer and the PostgreSQL reconciliation path.
type WakeupActivator struct {
	Store      gateway.WakeupStore
	Dispatch   broker.Broker
	ShardCount uint32
}

func (a WakeupActivator) Activate(ctx context.Context, key gateway.ExecutionKey) (WakeupActivationDisposition, error) {
	if a.Store == nil || a.Dispatch == nil || a.ShardCount == 0 || key.TenantID == "" || key.RequestID == "" {
		return "", runtime.ErrInvariantViolation
	}
	candidate, err := a.Store.InspectWakeup(ctx, key)
	if errors.Is(err, runtime.ErrNotFound) {
		return "", runtime.ErrInvariantViolation
	}
	if err != nil {
		return "", err
	}
	if candidate.Execution.Outcome == runtime.OutcomeBlocked || candidate.Blocked {
		return WakeupBlocked, nil
	}
	if candidate.Execution.Outcome != runtime.OutcomePending {
		return WakeupObsolete, nil
	}
	if !candidate.Ready {
		return WakeupNotReady, nil
	}
	shard, err := broker.ShardForSession(candidate.Execution.Envelope.TenantID,
		candidate.Execution.Envelope.AgentAppID, candidate.Execution.Envelope.SessionID, a.ShardCount)
	if err != nil {
		return "", err
	}
	if err := a.Dispatch.Publish(ctx, shard, candidate.Execution.Envelope); err != nil {
		return "", err
	}
	if err := a.Store.MarkWoken(ctx, key, candidate.Version); err != nil {
		if !errors.Is(err, runtime.ErrVersionConflict) {
			return "", err
		}
		current, readErr := a.Store.InspectWakeup(ctx, key)
		if readErr != nil {
			return "", err
		}
		if current.Execution.Outcome == runtime.OutcomePending {
			return "", err
		}
	}
	return WakeupActivated, nil
}

// WakeupDispatcher converts durable wakeup hints back into ordinary dispatch
// deliveries. PostgreSQL remains authoritative; Redis entries may be repeated.
type WakeupDispatcher struct {
	ConsumerID      string
	Wakeups         WakeupQueue
	Store           gateway.WakeupStore
	Dispatch        broker.Broker
	ShardCount      uint32
	ReclaimInterval time.Duration
	ReclaimLimit    int
	OnError         func(context.Context, WakeupDelivery, error)
	OnBlocked       func(context.Context, gateway.ExecutionKey)
	OnPoll          func(error)
	Drain           <-chan struct{}
}

func (d WakeupDispatcher) Run(ctx context.Context) error {
	if drain.Signaled(d.Drain) {
		return nil
	}
	if d.ConsumerID == "" || d.Wakeups == nil || d.Store == nil || d.Dispatch == nil || d.ShardCount == 0 {
		return runtime.ErrInvariantViolation
	}
	if d.ReclaimInterval <= 0 {
		d.ReclaimInterval = time.Second
	}
	if d.ReclaimLimit <= 0 {
		d.ReclaimLimit = 100
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
		case <-ctx.Done():
		case <-d.Drain:
			gate.Begin()
			cancel()
		}
	}()
	handle := func(delivery WakeupDelivery) error {
		leave, ok := gate.Enter()
		if !ok {
			return context.Canceled
		}
		defer leave()
		if drain.Signaled(d.Drain) {
			return context.Canceled
		}
		return d.handle(workCtx, delivery)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(d.ReclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				deliveries, err := d.Wakeups.ReclaimWakeups(runCtx, WakeupReclaimOptions{ConsumerID: d.ConsumerID, Limit: d.ReclaimLimit})
				if d.OnPoll != nil {
					d.OnPoll(err)
				}
				if err != nil {
					d.report(runCtx, WakeupDelivery{}, err)
					continue
				}
				for _, delivery := range deliveries {
					if runCtx.Err() != nil {
						return
					}
					if err := handle(delivery); err != nil && workCtx.Err() == nil {
						d.report(runCtx, delivery, err)
					}
				}
			}
		}
	}()
	err := d.Wakeups.Consume(runCtx, WakeupConsumerOptions{ConsumerID: d.ConsumerID}, func(_ context.Context, delivery WakeupDelivery) error {
		if err := handle(delivery); err != nil {
			d.report(workCtx, delivery, err)
		}
		// Failed handling deliberately leaves the entry pending for reclaim.
		return nil
	})
	cancel()
	if !drain.Signaled(d.Drain) {
		cancelWork()
	}
	<-done
	if drain.Signaled(d.Drain) {
		return workCtx.Err()
	}
	return err
}

func (d WakeupDispatcher) handle(ctx context.Context, delivery WakeupDelivery) error {
	event := delivery.Event
	if delivery.ID == "" || event.TenantID == "" || event.AggregateID == "" {
		return runtime.ErrInvalidEnvelope
	}
	key := gateway.ExecutionKey{TenantID: event.TenantID, RequestID: event.AggregateID}
	disposition, err := (WakeupActivator{Store: d.Store, Dispatch: d.Dispatch, ShardCount: d.ShardCount}).Activate(ctx, key)
	if err != nil {
		return err
	}
	if disposition == WakeupNotReady {
		return runtime.ErrInputNotReady
	}
	if disposition == WakeupBlocked && d.OnBlocked != nil {
		d.OnBlocked(ctx, key)
	}
	return d.Wakeups.AckWakeup(ctx, delivery)
}

func (d WakeupDispatcher) report(ctx context.Context, delivery WakeupDelivery, err error) {
	if err != nil && d.OnError != nil {
		d.OnError(ctx, delivery, err)
	}
}
