package delivery

import (
	"context"
	"sort"
	"sync"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type DestinationCatalog interface {
	ListDeliveryDestinations(context.Context) ([]channel.ReplyDestination, error)
}

type ConsumerRunner interface{ Run(context.Context) error }
type ConsumerFactory func(channel.ReplyDestination) (ConsumerRunner, error)

type Supervisor struct {
	Catalog         DestinationCatalog
	NewConsumer     ConsumerFactory
	RefreshInterval time.Duration
	OnError         func(error)
	// OnHealth reports successful catalog synchronization and consumer exits.
	OnHealth func(error)
}

type runningConsumer struct {
	cancel     context.CancelFunc
	generation uint64
	ready      bool
}

type consumerHealth struct {
	key        string
	generation uint64
	err        error
}

type observableConsumer interface {
	WithPollObserver(func(error)) ConsumerRunner
}

type consumerExit struct {
	key        string
	generation uint64
	err        error
}

func (s Supervisor) Run(ctx context.Context) error {
	if ctx == nil || s.Catalog == nil || s.NewConsumer == nil || s.RefreshInterval <= 0 {
		return runtime.ErrInvariantViolation
	}
	runCtx, cancel := context.WithCancel(ctx)
	var consumers sync.WaitGroup
	defer func() { cancel(); consumers.Wait() }()
	active := make(map[string]runningConsumer)
	exits := make(chan consumerExit)
	observations := make(chan consumerHealth)
	var generation uint64
	syncCatalog := func() error {
		destinations, err := s.Catalog.ListDeliveryDestinations(runCtx)
		if err != nil {
			return err
		}
		desired := make(map[string]channel.ReplyDestination, len(destinations))
		for _, destination := range destinations {
			key, err := deliveryDestinationKey(destination)
			if err != nil {
				return err
			}
			if _, exists := desired[key]; exists {
				return runtime.ErrIdempotencyCollision
			}
			desired[key] = destination
		}
		for key, current := range active {
			if _, keep := desired[key]; !keep {
				current.cancel()
				delete(active, key)
			}
		}
		keys := make([]string, 0, len(desired))
		for key := range desired {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, exists := active[key]; exists {
				continue
			}
			runner, err := s.NewConsumer(desired[key])
			if err != nil {
				return err
			}
			generation++
			consumerCtx, consumerCancel := context.WithCancel(runCtx)
			current := runningConsumer{cancel: consumerCancel, generation: generation}
			if observable, ok := runner.(observableConsumer); ok {
				currentGeneration := generation
				runner = observable.WithPollObserver(func(err error) {
					select {
					case observations <- consumerHealth{key: key, generation: currentGeneration, err: err}:
					case <-consumerCtx.Done():
					}
				})
			} else {
				current.ready = true
			}
			active[key] = current
			consumers.Add(1)
			go func(key string, generation uint64) {
				defer consumers.Done()
				err := runner.Run(consumerCtx)
				select {
				case exits <- consumerExit{key: key, generation: generation, err: err}:
				case <-runCtx.Done():
				}
			}(key, generation)
		}
		return nil
	}
	syncHealth := func() error {
		err := syncCatalog()
		if s.OnHealth != nil {
			healthErr := err
			for _, current := range active {
				if !current.ready {
					healthErr = runtime.ErrBackendUnavailable
				}
			}
			if len(active) == 0 {
				healthErr = runtime.ErrBackendUnavailable
			}
			s.OnHealth(healthErr)
		}
		return err
	}
	if err := syncHealth(); err != nil {
		return err
	}
	ticker := time.NewTicker(s.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			for _, current := range active {
				current.cancel()
			}
			return runCtx.Err()
		case exited := <-exits:
			if current, ok := active[exited.key]; ok && current.generation == exited.generation {
				delete(active, exited.key)
				if s.OnHealth != nil {
					s.OnHealth(runtime.ErrBackendUnavailable)
				}
				if exited.err != nil && s.OnError != nil {
					s.OnError(exited.err)
				}
			}
		case observation := <-observations:
			if current, ok := active[observation.key]; ok && current.generation == observation.generation {
				current.ready = observation.err == nil
				active[observation.key] = current
				// Only a successful catalog refresh can restore aggregate health.
				if observation.err != nil && s.OnHealth != nil {
					s.OnHealth(observation.err)
				}
			}
		case <-ticker.C:
			if err := syncHealth(); err != nil && s.OnError != nil {
				s.OnError(err)
			}
		}
	}
}

func deliveryDestinationKey(value channel.ReplyDestination) (string, error) {
	if value.TenantID == "" || value.Channel == "" || value.ChannelBindingID == "" || value.ExternalAccountID == "" || value.ConfigVersion != 0 {
		return "", runtime.ErrInvalidEnvelope
	}
	return value.TenantID + "\x00" + value.Channel + "\x00" + value.ChannelBindingID + "\x00" + value.ExternalAccountID, nil
}
