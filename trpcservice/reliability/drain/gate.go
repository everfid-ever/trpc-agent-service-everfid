// Package drain separates stopping admission from canceling active work.
package drain

import "sync"

type Gate struct {
	mu           sync.Mutex
	stopping     bool
	active       int
	signal, done chan struct{}
}

func New() *Gate                        { return &Gate{signal: make(chan struct{}), done: make(chan struct{})} }
func (g *Gate) Signal() <-chan struct{} { return g.signal }
func (g *Gate) Done() <-chan struct{}   { return g.done }
func (g *Gate) Draining() bool          { g.mu.Lock(); defer g.mu.Unlock(); return g.stopping }
func (g *Gate) Begin() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping {
		return
	}
	g.stopping = true
	close(g.signal)
	if g.active == 0 {
		close(g.done)
	}
}
func (g *Gate) Enter() (func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping {
		return nil, false
	}
	g.active++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			g.active--
			if g.stopping && g.active == 0 {
				close(g.done)
			}
		})
	}, true
}

func Signaled(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return true
	default:
		return false
	}
}
