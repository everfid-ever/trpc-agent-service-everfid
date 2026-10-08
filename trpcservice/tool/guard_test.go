package tool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancememory "github.com/liuzengh/trpc-agent-service/trpcservice/governance/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
	toolexecution "github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type callable struct{ calls int }

func (c *callable) Declaration() *agenttool.Declaration       { return &agenttool.Declaration{Name: "safe"} }
func (c *callable) Call(context.Context, []byte) (any, error) { c.calls++; return "ok", nil }

type queryableCallable struct {
	callable
	queries int
}

func (*queryableCallable) RecoveryPolicy() RecoveryPolicy { return RecoveryQueryable }
func (*queryableCallable) RecoveryKey(context.Context) (string, error) {
	return "provider-operation-1", nil
}
func (c *queryableCallable) QueryRecovery(context.Context, string) (any, bool, error) {
	c.queries++
	return "already-completed", true, nil
}

type executionStore struct {
	claim    toolexecution.Record
	claimed  bool
	finished toolexecution.Record
}

func (s *executionStore) Claim(_ context.Context, record toolexecution.Record, claim toolexecution.Claim) (toolexecution.Record, bool, error) {
	s.claim = record
	if !s.claimed {
		return record, false, nil
	}
	record.State = toolexecution.Running
	record.Attempt = 1
	record.Fence = 1
	record.LeaseOwner = claim.Owner
	return record, true, nil
}
func (s *executionStore) Renew(_ context.Context, record toolexecution.Record, _ time.Duration) (toolexecution.Record, error) {
	return record, nil
}
func (s *executionStore) Finish(_ context.Context, record toolexecution.Record, state toolexecution.State, resultRef, lastError string) (toolexecution.Record, error) {
	record.State, record.ResultRef, record.LastError = state, resultRef, lastError
	s.finished = record
	return record, nil
}
func (*executionStore) Get(context.Context, toolexecution.Key) (toolexecution.Record, error) {
	return toolexecution.Record{}, runtime.ErrNotFound
}
func TestGuardedCallableRequiresTrustedContextAndDeniesDangerousTools(t *testing.T) {
	store := governancememory.New(0, 0)
	value := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled, Tools: []governance.ToolRule{{ToolID: "safe", Version: 1}}}
	digest, _, _ := governance.PolicyDigest(value)
	_ = store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 1, SchemaVersion: 1, Policy: value, ContentDigest: digest, PublishedAt: time.Now()})
	inner := &callable{}
	guard := GuardedCallable{Inner: inner, Policies: store, Tool: governance.VersionedRef{ID: "safe", Version: 1}}
	if _, err := guard.Call(context.Background(), nil); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("untrusted=%v", err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant", RequestID: "request", SubjectID: "user", PolicyVersion: 1})
	if _, err := guard.Call(ctx, nil); err != nil || inner.calls != 1 {
		t.Fatalf("call=%d err=%v", inner.calls, err)
	}
	value.Tools[0].Dangerous = true
	digest, _, _ = governance.PolicyDigest(value)
	_ = store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 2, SchemaVersion: 1, Policy: value, ContentDigest: digest, PublishedAt: time.Now()})
	ctx = runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant", RequestID: "request", SubjectID: "user", PolicyVersion: 2})
	if _, err := guard.Call(ctx, nil); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("dangerous=%v", err)
	}
	if inner.calls != 1 {
		t.Fatalf("dangerous tool executed %d", inner.calls)
	}
}

type grantCoordinator struct {
	governance.ConfirmationCoordinator
	consumed bool
	attempt  governance.ToolAttempt
}

func (c *grantCoordinator) ConsumeGrant(_ context.Context, in governance.GrantClaim) (governance.Grant, error) {
	if c.consumed {
		return governance.Grant{}, runtime.ErrVersionConflict
	}
	c.consumed = true
	return governance.Grant{GrantID: in.GrantID, TenantID: in.TenantID, RequestID: in.RequestID, Tool: in.Tool, ToolCallID: in.ToolCallID, ArgsDigest: in.ArgsDigest, PolicyVersion: in.PolicyVersion, Version: in.ExpectedVersion + 1}, nil
}

func (c *grantCoordinator) FinishToolAttempt(_ context.Context, in governance.FinishToolAttemptRequest) (governance.ToolAttempt, error) {
	c.attempt = governance.ToolAttempt{TenantID: in.TenantID, GrantID: in.GrantID, State: in.State, ResultRef: in.ResultRef}
	return c.attempt, nil
}

func (c *grantCoordinator) GetToolAttempt(_ context.Context, tenantID, grantID string) (governance.ToolAttempt, error) {
	if c.attempt.TenantID != tenantID || c.attempt.GrantID != grantID {
		return governance.ToolAttempt{}, runtime.ErrNotFound
	}
	return c.attempt, nil
}

func TestGuardedCallableConsumesConfirmationOnceAndPersistsResult(t *testing.T) {
	store := governancememory.New(0, 0)
	value := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled,
		Tools: []governance.ToolRule{{ToolID: "safe", Version: 1, Dangerous: true, ConfirmationSupported: true}}}
	digest, _, _ := governance.PolicyDigest(value)
	_ = store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 1, SchemaVersion: 1, Policy: value, ContentDigest: digest, PublishedAt: time.Now()})
	coordinator := &grantCoordinator{}
	results := messagingmemory.New()
	inner := &callable{}
	guard := GuardedCallable{Inner: inner, Policies: store, Tool: governance.VersionedRef{ID: "safe", Version: 1}, Grants: coordinator, Results: results}
	args := []byte(`{"b":2,"a":1}`)
	_, argsDigest, err := governance.CanonicalArguments(args)
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant", RequestID: "request", SubjectID: "user", PolicyVersion: 1,
		GrantID: "grant-1", GrantVersion: 1, ToolCallID: "call-1", ArgsDigest: argsDigest, PayloadKeyVersion: 3})
	if result, err := guard.Call(ctx, args); err != nil || result != "ok" {
		t.Fatalf("result=%v err=%v", result, err)
	}
	if inner.calls != 1 || coordinator.attempt.State != governance.ToolAttemptSucceeded {
		t.Fatalf("calls=%d attempt=%#v", inner.calls, coordinator.attempt)
	}
	stored, err := results.GetToolResult(context.Background(), "tenant", "grant-1")
	if err != nil || string(stored.Content) != `"ok"` || stored.ResultRef != coordinator.attempt.ResultRef {
		t.Fatalf("stored=%#v err=%v", stored, err)
	}
	if _, err := guard.Call(ctx, args); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("replay err=%v", err)
	}
	if inner.calls != 1 {
		t.Fatalf("tool replayed calls=%d", inner.calls)
	}
}

func TestGuardedCallableUsesQueryableRecoveryWithoutRepeatingExternalEffect(t *testing.T) {
	store := governancememory.New(0, 0)
	value := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled,
		Tools: []governance.ToolRule{{ToolID: "safe", Version: 1, Dangerous: true, ConfirmationSupported: true}}}
	digest, _, _ := governance.PolicyDigest(value)
	_ = store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 1, SchemaVersion: 1, Policy: value, ContentDigest: digest, PublishedAt: time.Now()})
	coordinator := &grantCoordinator{}
	results := messagingmemory.New()
	inner := &queryableCallable{}
	executions := &executionStore{claimed: true}
	guard := GuardedCallable{Inner: inner, Policies: store, Tool: governance.VersionedRef{ID: "safe", Version: 1}, Grants: coordinator, Results: results, Executions: executions}
	args := []byte(`{"a":1}`)
	_, argsDigest, err := governance.CanonicalArguments(args)
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant", RequestID: "request", SubjectID: "user", PolicyVersion: 1,
		GrantID: "grant-1", GrantVersion: 1, ToolCallID: "call-1", ArgsDigest: argsDigest, PayloadKeyVersion: 3})
	result, err := guard.Call(ctx, args)
	if err != nil || result != "already-completed" {
		t.Fatalf("result=%v err=%v", result, err)
	}
	if inner.calls != 0 || inner.queries != 1 {
		t.Fatalf("tool calls=%d queries=%d", inner.calls, inner.queries)
	}
	if executions.claim.Policy != string(RecoveryQueryable) || executions.claim.IdempotencyKey != "provider-operation-1" || executions.finished.State != toolexecution.Succeeded {
		t.Fatalf("claim=%#v finished=%#v", executions.claim, executions.finished)
	}
	if !coordinator.consumed || coordinator.attempt.State != governance.ToolAttemptSucceeded {
		t.Fatalf("grant=%v attempt=%#v", coordinator.consumed, coordinator.attempt)
	}
}

func TestGuardedCallableDoesNotConsumeGrantWhenAnotherNodeOwnsExecution(t *testing.T) {
	store := governancememory.New(0, 0)
	value := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled,
		Tools: []governance.ToolRule{{ToolID: "safe", Version: 1, Dangerous: true, ConfirmationSupported: true}}}
	digest, _, _ := governance.PolicyDigest(value)
	_ = store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 1, SchemaVersion: 1, Policy: value, ContentDigest: digest, PublishedAt: time.Now()})
	coordinator := &grantCoordinator{}
	inner := &callable{}
	guard := GuardedCallable{Inner: inner, Policies: store, Tool: governance.VersionedRef{ID: "safe", Version: 1}, Grants: coordinator, Results: messagingmemory.New(), Executions: &executionStore{}}
	args := []byte(`{"a":1}`)
	_, argsDigest, err := governance.CanonicalArguments(args)
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant", RequestID: "request", SubjectID: "user", PolicyVersion: 1,
		GrantID: "grant-1", GrantVersion: 1, ToolCallID: "call-1", ArgsDigest: argsDigest, PayloadKeyVersion: 3})
	if _, err := guard.Call(ctx, args); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("err=%v", err)
	}
	if coordinator.consumed || inner.calls != 0 {
		t.Fatalf("grant=%v calls=%d", coordinator.consumed, inner.calls)
	}
}

func TestGuardedCallableRecoversConsumedUncertainAttempt(t *testing.T) {
	store := governancememory.New(0, 0)
	value := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled,
		Tools: []governance.ToolRule{{ToolID: "safe", Version: 1, Dangerous: true, ConfirmationSupported: true}}}
	digest, _, _ := governance.PolicyDigest(value)
	_ = store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 1, SchemaVersion: 1, Policy: value, ContentDigest: digest, PublishedAt: time.Now()})
	coordinator := &grantCoordinator{consumed: true, attempt: governance.ToolAttempt{TenantID: "tenant", GrantID: "grant-1", RequestID: "request", ToolCallID: "call-1", State: governance.ToolAttemptEffectUnknown}}
	inner := &queryableCallable{}
	executions := &executionStore{claimed: true}
	guard := GuardedCallable{Inner: inner, Policies: store, Tool: governance.VersionedRef{ID: "safe", Version: 1}, Grants: coordinator, Results: messagingmemory.New(), Executions: executions}
	args := []byte(`{"a":1}`)
	_, argsDigest, err := governance.CanonicalArguments(args)
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant", RequestID: "request", SubjectID: "user", PolicyVersion: 1,
		GrantID: "grant-1", GrantVersion: 2, ToolCallID: "call-1", ArgsDigest: argsDigest, PayloadKeyVersion: 3})
	result, err := guard.Call(ctx, args)
	if err != nil || result != "already-completed" {
		t.Fatalf("result=%v err=%v", result, err)
	}
	if inner.calls != 0 || inner.queries != 1 || executions.finished.State != toolexecution.Succeeded {
		t.Fatalf("calls=%d queries=%d finished=%#v", inner.calls, inner.queries, executions.finished)
	}
}

func TestGuardedCallableNeverReplaysManualRecoveryAfterGrantConsumption(t *testing.T) {
	store := governancememory.New(0, 0)
	value := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled,
		Tools: []governance.ToolRule{{ToolID: "safe", Version: 1, Dangerous: true, ConfirmationSupported: true}}}
	digest, _, _ := governance.PolicyDigest(value)
	_ = store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 1, SchemaVersion: 1, Policy: value, ContentDigest: digest, PublishedAt: time.Now()})
	coordinator := &grantCoordinator{consumed: true, attempt: governance.ToolAttempt{TenantID: "tenant", GrantID: "grant-1", RequestID: "request", ToolCallID: "call-1", State: governance.ToolAttemptEffectUnknown}}
	inner := &callable{}
	executions := &executionStore{claimed: true}
	guard := GuardedCallable{Inner: inner, Policies: store, Tool: governance.VersionedRef{ID: "safe", Version: 1}, Grants: coordinator, Results: messagingmemory.New(), Executions: executions}
	args := []byte(`{"a":1}`)
	_, argsDigest, err := governance.CanonicalArguments(args)
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant", RequestID: "request", SubjectID: "user", PolicyVersion: 1,
		GrantID: "grant-1", GrantVersion: 2, ToolCallID: "call-1", ArgsDigest: argsDigest, PayloadKeyVersion: 3})
	if _, err := guard.Call(ctx, args); !errors.Is(err, runtime.ErrEffectUnknown) {
		t.Fatalf("err=%v", err)
	}
	if inner.calls != 0 || executions.finished.State != toolexecution.EffectUnknown {
		t.Fatalf("calls=%d finished=%#v", inner.calls, executions.finished)
	}
}
