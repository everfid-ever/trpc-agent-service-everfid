package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	toolexecution "github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// GuardedCallable is the non-bypassable outer wrapper around a raw Tool. Raw
// tools stay private to the resolver; every actual call re-resolves the exact
// policy carried by trusted execution context. Dangerous ask paths require an
// exact, one-use durable grant and an encrypted result store.
type GuardedCallable struct {
	Inner      agenttool.CallableTool
	Policies   governance.Repository
	Tool       governance.VersionedRef
	Grants     governance.ConfirmationCoordinator
	Results    messaging.ToolResultStore
	Executions toolexecution.Store
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
	if decision.Action == governance.ActionAsk {
		_, digest, digestErr := governance.CanonicalArguments(args)
		if digestErr != nil || g.Grants == nil || g.Results == nil || execution.GrantID == "" || execution.GrantVersion < 1 || execution.ToolCallID == "" || execution.ArgsDigest != digest || execution.PayloadKeyVersion < 1 {
			return nil, runtime.ErrCapabilityUnsupported
		}
		var running toolexecution.Record
		var stopRenew func()
		if g.Executions != nil {
			policy, key := string(RecoveryManual), ""
			if recoverable, ok := g.Inner.(RecoveryDescriptor); ok {
				policy = string(recoverable.RecoveryPolicy())
				key, err = recoverable.RecoveryKey(ctx)
				if err != nil {
					return nil, err
				}
			}
			if key == "" {
				key = execution.TenantID + ":" + execution.RequestID + ":" + execution.ToolCallID
			}
			var claimed bool
			running, claimed, err = g.Executions.Claim(ctx, toolexecution.Record{Key: toolexecution.Key{TenantID: execution.TenantID, RequestID: execution.RequestID, ToolCallID: execution.ToolCallID}, ToolID: g.Tool.ID, ToolVersion: g.Tool.Version, ArgsDigest: digest, Policy: policy, IdempotencyKey: key}, toolexecution.Claim{Owner: execution.RequestID + ":tool", TTL: 30 * time.Second})
			if err != nil {
				return nil, err
			}
			if !claimed {
				return nil, runtime.ErrVersionConflict
			}
			if running.ToolID != g.Tool.ID || running.ToolVersion != g.Tool.Version || running.ArgsDigest != digest || running.Policy != policy || running.IdempotencyKey != key {
				return nil, runtime.ErrVersionMismatch
			}
			renewCtx, cancelRenew := context.WithCancel(context.Background())
			doneRenew := make(chan struct{})
			stopRenew = func() { cancelRenew(); <-doneRenew }
			go func() {
				defer close(doneRenew)
				ticker := time.NewTicker(10 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-renewCtx.Done():
						return
					case <-ticker.C:
						if _, renewErr := g.Executions.Renew(renewCtx, running, 30*time.Second); renewErr != nil {
							cancelRenew()
							return
						}
					}
				}
			}()
		}
		if stopRenew != nil {
			defer stopRenew()
		}
		var recovered any
		var recoveredComplete bool
		if queryable, ok := g.Inner.(RecoveryQuerier); ok && queryable.RecoveryPolicy() == RecoveryQueryable {
			recoveryKey, keyErr := queryable.RecoveryKey(ctx)
			if keyErr != nil {
				return nil, keyErr
			}
			recovered, recoveredComplete, err = queryable.QueryRecovery(ctx, recoveryKey)
			if err != nil {
				return nil, err
			}
		}
		_, err = g.Grants.ConsumeGrant(ctx, governance.GrantClaim{TenantID: execution.TenantID, GrantID: execution.GrantID,
			RequestID: execution.RequestID, SubjectID: execution.SubjectID, Tool: g.Tool, ToolCallID: execution.ToolCallID,
			ArgsDigest: digest, PolicyVersion: execution.PolicyVersion, ExpectedVersion: execution.GrantVersion})
		if err != nil {
			// A previous owner may have consumed the one-use grant immediately
			// before it died. That leaves the companion tool attempt in
			// effect_unknown. A fenced successor that has acquired the durable
			// execution lease is allowed to recover that exact attempt; it must
			// never consume the grant a second time.
			resumingConsumedAttempt := false
			if g.Executions != nil && errors.Is(err, runtime.ErrVersionConflict) {
				attempt, attemptErr := g.Grants.GetToolAttempt(ctx, execution.TenantID, execution.GrantID)
				resumingConsumedAttempt = attemptErr == nil && attempt.State == governance.ToolAttemptEffectUnknown &&
					attempt.RequestID == execution.RequestID && attempt.ToolCallID == execution.ToolCallID
			}
			if !resumingConsumedAttempt {
				if g.Executions != nil {
					_, _ = g.Executions.Finish(ctx, running, toolexecution.EffectUnknown, "", "grant consumption failed")
				}
				return nil, err
			}
			if RecoveryPolicy(running.Policy) == RecoveryManual {
				_, _ = g.Executions.Finish(ctx, running, toolexecution.EffectUnknown, "", "manual recovery required")
				return nil, runtime.ErrEffectUnknown
			}
		}
		result := recovered
		var callErr error
		if !recoveredComplete {
			result, callErr = g.Inner.Call(ctx, args)
		}
		if callErr != nil {
			if g.Executions != nil {
				_, _ = g.Executions.Finish(ctx, running, toolexecution.Failed, "", callErr.Error())
			}
			_, _ = g.Grants.FinishToolAttempt(ctx, governance.FinishToolAttemptRequest{TenantID: execution.TenantID, GrantID: execution.GrantID, State: governance.ToolAttemptFailed})
			return nil, callErr
		}
		encoded, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			return nil, runtime.ErrInvariantViolation
		}
		sum := sha256.Sum256(encoded)
		resultRef := "tool-result://sha256/" + hex.EncodeToString(sum[:])
		if err = g.Results.PutToolResult(ctx, messaging.ToolResultRecord{TenantID: execution.TenantID, GrantID: execution.GrantID,
			RequestID: execution.RequestID, ResultRef: resultRef, ContentDigest: hex.EncodeToString(sum[:]), Content: encoded,
			KeyVersion: execution.PayloadKeyVersion}); err != nil {
			return nil, err
		}
		if _, err = g.Grants.FinishToolAttempt(ctx, governance.FinishToolAttemptRequest{TenantID: execution.TenantID, GrantID: execution.GrantID,
			State: governance.ToolAttemptSucceeded, ResultRef: resultRef}); err != nil {
			return nil, err
		}
		if g.Executions != nil {
			if _, err = g.Executions.Finish(ctx, running, toolexecution.Succeeded, resultRef, ""); err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	if decision.Action != governance.ActionAllow {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return g.Inner.Call(ctx, args)
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
	for index, value := range values {
		callable, ok := value.(agenttool.CallableTool)
		if !ok || value == nil || value.Declaration() == nil || value.Declaration().Name != refs[index].ID {
			return nil, runtime.ErrCapabilityUnsupported
		}
		result[index] = GuardedCallable{Inner: callable, Policies: policies, Tool: refs[index], Grants: grants, Results: results, Executions: executions}
	}
	return result, nil
}

var _ agenttool.CallableTool = GuardedCallable{}
var _ governance.VersionedTool = GuardedCallable{}
