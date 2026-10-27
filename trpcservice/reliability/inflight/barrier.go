// Package inflight provides opt-in, in-process fault-injection barriers for
// deterministic takeover tests. It is never armed by normal runtime paths.
package inflight

import (
	"context"
	"sync"
)

// Point identifies a durable-work boundary. P2 is intentionally after model
// and tool execution but before terminal result persistence and commit.
type Point string

const (
	PointP1BeforeExecution           Point = "p1_persisted_before_execution"
	PointP2BeforeTerminalCommit      Point = "p2_executed_before_commit"
	PointP3BeforeReplyPublish        Point = "p3_result_committed_before_reply_publish"
	PointP4BeforeProviderDelivery    Point = "p4_delivery_claimed_before_provider_send"
	PointP2DuringModelResponse       Point = "p2_model_accepted_before_result"
	PointP4AfterProviderAccepted     Point = "p4_provider_accepted_before_confirmation"
	PointToolEffectBeforeCommit      Point = "tool_effect_completed_before_result_commit"
	PointResultPersistedBeforeCommit Point = "result_persisted_before_terminal_commit"
)

type scope struct{ TenantID, RequestID, Evidence string }
type scopeKey struct{}
type barrierKey struct{}

func WithScope(ctx context.Context, tenantID, requestID, evidence string) context.Context {
	return context.WithValue(ctx, scopeKey{}, scope{tenantID, requestID, evidence})
}
func WithBarrier(ctx context.Context, barrier Barrier) context.Context {
	return context.WithValue(ctx, barrierKey{}, barrier)
}
func Wait(ctx context.Context, point Point) error {
	if barrier, ok := ctx.Value(barrierKey{}).(Barrier); ok && barrier != nil {
		return barrier.Wait(ctx, point)
	}
	return nil
}

// Barrier is injected only by test compositions. Production callers may keep
// it nil, which has no effect on execution.
type Barrier interface {
	Wait(context.Context, Point) error
}

type Snapshot struct {
	Point     Point  `json:"point"`
	Armed     bool   `json:"armed"`
	Hit       bool   `json:"hit"`
	Released  bool   `json:"released"`
	TenantID  string `json:"tenant_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	Evidence  string `json:"evidence,omitempty"`
}

type gate struct {
	hit      chan struct{}
	release  chan struct{}
	hitOnce  sync.Once
	relOnce  sync.Once
	filter   scope
	hitScope scope
}

// Controller owns one optional barrier per point. Arm returns a channel which
// closes when a worker reaches the point; Release unblocks that worker.
type Controller struct {
	mu    sync.Mutex
	gates map[Point]*gate
}

func (c *Controller) Arm(point Point) <-chan struct{} {
	return c.ArmFor(point, "", "")
}
func (c *Controller) ArmFor(point Point, tenantID, requestID string) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gates == nil {
		c.gates = make(map[Point]*gate)
	}
	if existing := c.gates[point]; existing != nil {
		select {
		case <-existing.release:
			// A released run is complete and may be armed again.
		default:
			// Do not strand a worker already waiting at this point.
			return existing.hit
		}
	}
	g := &gate{hit: make(chan struct{}), release: make(chan struct{}), filter: scope{TenantID: tenantID, RequestID: requestID}}
	c.gates[point] = g
	return g.hit
}

func (c *Controller) Wait(ctx context.Context, point Point) error {
	c.mu.Lock()
	g := c.gates[point]
	identity, _ := ctx.Value(scopeKey{}).(scope)
	if g == nil || (g.filter.TenantID != "" && g.filter.TenantID != identity.TenantID) || (g.filter.RequestID != "" && g.filter.RequestID != identity.RequestID) {
		c.mu.Unlock()
		return nil
	}
	g.hitOnce.Do(func() { g.hitScope = identity; close(g.hit) })
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.release:
		return nil
	}
}

func (c *Controller) Release(point Point) bool {
	c.mu.Lock()
	g := c.gates[point]
	c.mu.Unlock()
	if g == nil {
		return false
	}
	g.relOnce.Do(func() { close(g.release) })
	return true
}

func (c *Controller) Snapshot(point Point) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.gates[point]
	if g == nil {
		return Snapshot{Point: point}
	}
	s := Snapshot{Point: point, Armed: true, TenantID: g.hitScope.TenantID, RequestID: g.hitScope.RequestID, Evidence: g.hitScope.Evidence}
	select {
	case <-g.hit:
		s.Hit = true
	default:
	}
	select {
	case <-g.release:
		s.Released = true
	default:
	}
	return s
}
