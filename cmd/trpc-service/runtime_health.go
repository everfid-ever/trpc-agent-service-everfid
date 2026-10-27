package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

// runtimeHealth records actual component observations. Starting a goroutine is
// not evidence of readiness; each component must complete a successful poll.
// An idle poll counts as progress, independently of model execution duration.
type runtimeHealth struct {
	mu         sync.RWMutex
	components map[string]componentHealth
	stopping   bool
}

type componentHealth struct {
	Owner       string    `json:"owner"`
	State       string    `json:"state"`
	LastSuccess time.Time `json:"last_success,omitempty"`
	LastFailure time.Time `json:"last_failure,omitempty"`
	Failures    uint64    `json:"failures"`
	Reason      string    `json:"reason,omitempty"`
}

func newRuntimeHealth(owners map[string]string) *runtimeHealth {
	h := &runtimeHealth{components: make(map[string]componentHealth, len(owners))}
	for name, owner := range owners {
		h.components[name] = componentHealth{Owner: owner, State: "starting"}
	}
	return h
}

func (h *runtimeHealth) observe(name string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.components[name]
	if !ok || h.stopping {
		return
	}
	if err == nil {
		c.State, c.Reason, c.LastSuccess = "ready", "", time.Now().UTC()
	} else {
		// Do not expose raw dependency errors, which may contain credentials.
		c.State, c.Reason, c.LastFailure = "degraded", "operation_failed", time.Now().UTC()
		c.Failures++
	}
	h.components[name] = c
}

func (h *runtimeHealth) snapshot() (bool, map[string]componentHealth) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ready := !h.stopping && len(h.components) > 0
	result := make(map[string]componentHealth, len(h.components))
	for name, c := range h.components {
		result[name] = c
		ready = ready && c.State == "ready"
	}
	return ready, result
}

func (h *runtimeHealth) stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopping = true
	for name, c := range h.components {
		c.State, c.Reason = "stopped", "shutdown"
		h.components[name] = c
	}
}

func (h *runtimeHealth) beginDrain() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopping = true
	for name, c := range h.components {
		c.State, c.Reason = "draining", "planned_stop"
		h.components[name] = c
	}
}

func (h *runtimeHealth) run(name string, operation func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		err := operation(ctx)
		if err == nil {
			err = errors.New("component stopped unexpectedly")
		}
		h.observe(name, err)
		return err
	}
}

func (h *runtimeHealth) poll(name string, operation func(context.Context) (int, error)) func(context.Context, int) (int, error) {
	return func(ctx context.Context, _ int) (int, error) {
		n, err := operation(ctx)
		h.observe(name, err)
		return n, err
	}
}
