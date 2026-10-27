package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	toolexecution "github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// GuardedCallable is the non-bypassable policy boundary around a raw Tool.
// Distributed runtimes provide Executions with encrypted DurableResults for
// BOTH allow and ask. The nil-store path supports standalone legacy callers;
// it does not claim durable recovery guarantees.
type GuardedCallable struct {
	Inner         agenttool.CallableTool
	Policies      governance.Repository
	Tool          governance.VersionedRef
	Grants        governance.ConfirmationCoordinator
	Results       messaging.ToolResultStore
	Executions    toolexecution.Store
	LeaseTTL      time.Duration
	RenewInterval time.Duration
}

func (g GuardedCallable) Declaration() *agenttool.Declaration {
	if g.Inner == nil {
		return nil
	}
	return g.Inner.Declaration()
}
func (g GuardedCallable) GovernanceToolRef() governance.VersionedRef { return g.Tool }
func (g GuardedCallable) Call(ctx context.Context, args []byte) (any, error) {
	execution, ok := runtime.ExecutionContextFrom(ctx)
	if !ok || execution.SubjectID == "" || execution.PolicyVersion < 1 || g.Inner == nil || g.Policies == nil || g.Tool.ID == "" || g.Tool.Version < 1 {
		return nil, runtime.ErrTenantScope
	}
	policy, err := g.Policies.GetPolicy(ctx, execution.TenantID, execution.PolicyVersion)
	if err != nil {
		return nil, err
	}
	decision := governance.ToolDecision(policy, g.Tool)
	if decision.Action != governance.ActionAllow && decision.Action != governance.ActionAsk {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if g.Executions != nil {
		results, ok := g.Executions.(toolexecution.DurableResults)
		if !ok {
			return nil, runtime.ErrCapabilityUnsupported
		}
		return g.callDurable(ctx, args, decision.Action == governance.ActionAsk, results)
	}
	if decision.Action == governance.ActionAllow {
		return g.Inner.Call(ctx, args)
	}
	_, digest, err := governance.CanonicalArguments(args)
	if err != nil || g.Grants == nil || g.Results == nil || execution.GrantID == "" || execution.GrantVersion < 1 || execution.ToolCallID == "" || execution.ArgsDigest != digest || execution.PayloadKeyVersion < 1 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	_, err = g.Grants.ConsumeGrant(ctx, governance.GrantClaim{TenantID: execution.TenantID, GrantID: execution.GrantID, RequestID: execution.RequestID, SubjectID: execution.SubjectID, Tool: g.Tool, ToolCallID: execution.ToolCallID, ArgsDigest: digest, PolicyVersion: execution.PolicyVersion, ExpectedVersion: execution.GrantVersion})
	if err != nil {
		return nil, err
	}
	result, err := g.Inner.Call(ctx, args)
	if err != nil {
		_, _ = g.Grants.FinishToolAttempt(ctx, governance.FinishToolAttemptRequest{TenantID: execution.TenantID, GrantID: execution.GrantID, State: governance.ToolAttemptFailed})
		return nil, err
	}
	content, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(content)
	if err = g.mirrorConfirmedResult(ctx, execution, content, "tool-result://sha256/"+hex.EncodeToString(sum[:])); err != nil {
		return nil, err
	}
	return result, nil
}

func GuardCallables(policies governance.Repository, refs []governance.VersionedRef, values []agenttool.Tool) ([]agenttool.Tool, error) {
	return GuardCallablesWithGrants(policies, nil, refs, values)
}
func GuardCallablesWithGrants(policies governance.Repository, grants governance.ConfirmationCoordinator, refs []governance.VersionedRef, values []agenttool.Tool) ([]agenttool.Tool, error) {
	return GuardCallablesWithConfirmation(policies, grants, nil, refs, values)
}
func GuardCallablesWithConfirmation(policies governance.Repository, grants governance.ConfirmationCoordinator, results messaging.ToolResultStore, refs []governance.VersionedRef, values []agenttool.Tool) ([]agenttool.Tool, error) {
	return GuardCallablesWithExecution(policies, grants, results, nil, refs, values)
}
func GuardCallablesWithExecution(policies governance.Repository, grants governance.ConfirmationCoordinator, results messaging.ToolResultStore, executions toolexecution.Store, refs []governance.VersionedRef, values []agenttool.Tool) ([]agenttool.Tool, error) {
	if policies == nil || len(refs) != len(values) {
		return nil, runtime.ErrCapabilityUnsupported
	}
	result := make([]agenttool.Tool, len(values))
	for i, value := range values {
		callable, ok := value.(agenttool.CallableTool)
		if !ok || value == nil || value.Declaration() == nil || value.Declaration().Name != refs[i].ID {
			return nil, runtime.ErrCapabilityUnsupported
		}
		result[i] = GuardedCallable{Inner: callable, Policies: policies, Tool: refs[i], Grants: grants, Results: results, Executions: executions}
	}
	return result, nil
}

var _ agenttool.CallableTool = GuardedCallable{}
var _ governance.VersionedTool = GuardedCallable{}
