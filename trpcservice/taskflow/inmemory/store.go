// Package inmemory provides the taskflow contract for isolated tests. It is
// not a distributed or durable deployment backend.
package inmemory

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/taskflow"
)

type Store struct {
	mu      sync.Mutex
	plans   map[taskflow.Key]taskflow.Plan
	history map[taskflow.Key][]taskflow.Plan
}

func New() *Store {
	return &Store{plans: map[taskflow.Key]taskflow.Plan{}, history: map[taskflow.Key][]taskflow.Plan{}}
}
func clone(p taskflow.Plan) taskflow.Plan {
	b, _ := json.Marshal(p)
	var out taskflow.Plan
	_ = json.Unmarshal(b, &out)
	out.Scope = p.Scope
	out.KeyVersion = p.KeyVersion
	return out
}
func (s *Store) Open(_ context.Context, scope taskflow.Scope, digest string, v int64) (taskflow.Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if scope.TenantID == "" || scope.RequestID == "" || scope.Fence == 0 || len(digest) != 64 || v < 1 {
		return taskflow.Plan{}, runtime.ErrInvalidEnvelope
	}
	p, ok := s.plans[scope.Key]
	if ok {
		if p.Fence > scope.Fence {
			return p, runtime.ErrStaleFence
		}
		if p.InputDigest != digest || p.KeyVersion != v {
			return p, runtime.ErrVersionMismatch
		}
		p.Fence = scope.Fence
	} else {
		p = taskflow.Plan{Scope: scope, Version: 1, InputDigest: digest, KeyVersion: v, Phase: "open", Steps: []taskflow.Step{}, Contract: taskflow.DefaultContract()}
	}
	s.plans[scope.Key] = clone(p)
	return clone(p), nil
}
func (s *Store) Get(_ context.Context, k taskflow.Key) (taskflow.Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.plans[k]
	if !ok {
		return p, runtime.ErrNotFound
	}
	return clone(p), nil
}
func (s *Store) Save(_ context.Context, scope taskflow.Scope, version int64, p taskflow.Plan) (taskflow.Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.plans[scope.Key]
	if !ok || old.Fence != scope.Fence || old.Version != version || (old.Phase != "open" && !(old.Phase == "sealed" && p.Phase == "failed")) || p.Key != scope.Key || p.Fence != scope.Fence || p.InputDigest != old.InputDigest || p.KeyVersion != old.KeyVersion {
		return p, runtime.ErrVersionConflict
	}
	p.Version = version + 1
	s.plans[scope.Key] = clone(p)
	s.history[scope.Key] = append(s.history[scope.Key], clone(p))
	return clone(p), nil
}
func (s *Store) History(k taskflow.Key) []taskflow.Plan {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]taskflow.Plan, 0, len(s.history[k]))
	for _, p := range s.history[k] {
		out = append(out, clone(p))
	}
	return out
}
