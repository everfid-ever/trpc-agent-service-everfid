// Package inflight provides opt-in, in-process fault-injection barriers for
// deterministic takeover tests. It is never armed by normal runtime paths.
package inflight

import (
	"context"
	"sync"
)

// Point identifies a durable-work boundary. P2 is intentionally after model
// execution and result persistence, but before the fenced terminal commit.
type Point string

const PointP2BeforeTerminalCommit Point = "p2_before_terminal_commit"

// Barrier is injected only by test compositions. Production callers may keep
// it nil, which has no effect on execution.
type Barrier interface {
	Wait(context.Context, Point) error
}

type Snapshot struct {
	Point    Point
	Armed    bool
	Hit      bool
	Released bool
}

type gate struct {
	hit     chan struct{}
	release chan struct{}
	hitOnce sync.Once
	relOnce sync.Once
}

// Controller owns one optional barrier per point. Arm returns a channel which
// closes when a worker reaches the point; Release unblocks that worker.
type Controller struct {
	mu    sync.Mutex
	gates map[Point]*gate
}

func (c *Controller) Arm(point Point) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gates == nil {
		c.gates = make(map[Point]*gate)
	}
	g := &gate{hit: make(chan struct{}), release: make(chan struct{})}
	c.gates[point] = g
	return g.hit
}

func (c *Controller) Wait(ctx context.Context, point Point) error {
	c.mu.Lock()
	g := c.gates[point]
	c.mu.Unlock()
	if g == nil {
		return nil
	}
	g.hitOnce.Do(func() { close(g.hit) })
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
	g := c.gates[point]
	c.mu.Unlock()
	if g == nil {
		return Snapshot{Point: point}
	}
	s := Snapshot{Point: point, Armed: true}
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
