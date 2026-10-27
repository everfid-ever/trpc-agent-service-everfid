package tool

import (
	"context"
	"errors"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancememory "github.com/liuzengh/trpc-agent-service/trpcservice/governance/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	toolexecution "github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
	"sync"
	"testing"
	"time"
)

type durableMemory struct {
	mu        sync.Mutex
	records   map[toolexecution.Key]toolexecution.Record
	outputs   map[toolexecution.Key][]byte
	loseLease bool
}

func newDurableMemory() *durableMemory {
	return &durableMemory{records: map[toolexecution.Key]toolexecution.Record{}, outputs: map[toolexecution.Key][]byte{}}
}
func (s *durableMemory) Get(_ context.Context, k toolexecution.Key) (toolexecution.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[k]
	if !ok {
		return r, runtime.ErrNotFound
	}
	return r, nil
}
func (s *durableMemory) Claim(_ context.Context, in toolexecution.Record, c toolexecution.Claim) (toolexecution.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.records[in.Key]
	if ok && (old.State == toolexecution.Succeeded || old.State == toolexecution.Running && old.LeaseUntil.After(time.Now())) {
		return old, false, nil
	}
	in.Attempt = old.Attempt + 1
	in.Fence = old.Fence + 1
	in.State = toolexecution.Running
	in.LeaseOwner = c.Owner
	in.LeaseUntil = time.Now().Add(c.TTL)
	s.records[in.Key] = in
	return in, true, nil
}
func (s *durableMemory) Renew(_ context.Context, in toolexecution.Record, _ time.Duration) (toolexecution.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loseLease {
		return in, runtime.ErrLeaseLost
	}
	return in, nil
}
func (s *durableMemory) Finish(_ context.Context, in toolexecution.Record, state toolexecution.State, ref, lastError string) (toolexecution.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in.State, in.ResultRef, in.LastError = state, ref, lastError
	s.records[in.Key] = in
	return in, nil
}
func (s *durableMemory) Complete(_ context.Context, in toolexecution.Record, data []byte, _ int64) (toolexecution.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loseLease {
		return in, runtime.ErrLeaseLost
	}
	in.State = toolexecution.Succeeded
	in.ResultRef = "result"
	s.records[in.Key] = in
	s.outputs[in.Key] = append([]byte(nil), data...)
	return in, nil
}
func (s *durableMemory) LoadResult(_ context.Context, k toolexecution.Key) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.outputs[k]...), nil
}

type recoveringTool struct {
	callable
	policy  RecoveryPolicy
	block   bool
	queries int
}

func (t *recoveringTool) RecoveryPolicy() RecoveryPolicy { return t.policy }
func (t *recoveringTool) RecoveryKey(ctx context.Context) (string, error) {
	execution, _ := runtime.ExecutionContextFrom(ctx)
	return execution.ToolCallID, nil
}
func (t *recoveringTool) QueryRecovery(context.Context, string) (any, bool, error) {
	t.queries++
	return "external-result", true, nil
}
func (t *recoveringTool) Call(ctx context.Context, _ []byte) (any, error) {
	t.calls++
	if t.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return "ok", nil
}

func durableGuard(t *testing.T, inner *recoveringTool, store *durableMemory) (GuardedCallable, context.Context) {
	t.Helper()
	policies := governancememory.New(0, 0)
	value := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled, Tools: []governance.ToolRule{{ToolID: "safe", Version: 1}}}
	digest, _, _ := governance.PolicyDigest(value)
	if err := policies.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 1, SchemaVersion: 1, Policy: value, ContentDigest: digest, PublishedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	guard := GuardedCallable{Inner: tenantBoundCallable{tenantID: "tenant", id: "safe", inner: inner}, Policies: policies, Tool: governance.VersionedRef{ID: "safe", Version: 1}, Executions: store}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant", RequestID: "request", SubjectID: "user", PolicyVersion: 1, PayloadKeyVersion: 1})
	return guard, ctx
}
func TestDurableAllowReusesResultAcrossRegeneratedCallIDs(t *testing.T) {
	store := newDurableMemory()
	inner := &recoveringTool{policy: RecoveryIdempotentKey}
	guard, ctx := durableGuard(t, inner, store)
	if _, err := guard.Call(ctx, []byte(`{"a":1,"b":2}`)); err != nil {
		t.Fatal(err)
	}
	execution, _ := runtime.ExecutionContextFrom(ctx)
	execution.ToolCallID = "new-provider-call"
	ctx = runtime.WithExecutionContext(ctx, execution)
	if result, err := guard.Call(ctx, []byte(`{"b":2,"a":1}`)); err != nil || result != "ok" {
		t.Fatalf("%v %v", result, err)
	}
	if inner.calls != 1 || len(store.records) != 1 {
		t.Fatalf("calls=%d records=%d", inner.calls, len(store.records))
	}
}
func TestDurableAllowManualTakeoverStopsAndQueryableRecovers(t *testing.T) {
	for _, policy := range []RecoveryPolicy{RecoveryManual, RecoveryQueryable, RecoveryIdempotentKey} {
		store := newDurableMemory()
		inner := &recoveringTool{policy: policy}
		guard, ctx := durableGuard(t, inner, store)
		// First attempt completes only in the external system; lose its local
		// output to model a crash before the fenced completion transaction.
		if _, err := guard.Call(ctx, []byte(`{"a":1}`)); err != nil {
			t.Fatal(err)
		}
		for key, r := range store.records {
			r.State = toolexecution.Running
			r.LeaseUntil = time.Now().Add(-time.Second)
			store.records[key] = r
			delete(store.outputs, key)
		}
		before := inner.calls
		result, err := guard.Call(ctx, []byte(`{"a":1}`))
		if policy == RecoveryManual {
			if !errors.Is(err, runtime.ErrEffectUnknown) || inner.calls != before {
				t.Fatalf("manual calls=%d err=%v", inner.calls, err)
			}
		} else if err != nil {
			t.Fatal(err)
		} else if policy == RecoveryQueryable && (result != "external-result" || inner.calls != before) {
			t.Fatal("query recovery repeated effect")
		}
	}
}
func TestDurableToolCancelsAndCannotCompleteAfterLeaseLoss(t *testing.T) {
	store := newDurableMemory()
	store.loseLease = true
	inner := &recoveringTool{policy: RecoveryIdempotentKey, block: true}
	guard, ctx := durableGuard(t, inner, store)
	guard.LeaseTTL = 100 * time.Millisecond
	guard.RenewInterval = 5 * time.Millisecond
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err := guard.Call(ctx, []byte(`{}`)); !errors.Is(err, runtime.ErrLeaseLost) {
		t.Fatal(err)
	}
	if len(store.outputs) != 0 {
		t.Fatal("stale owner persisted output")
	}
}
