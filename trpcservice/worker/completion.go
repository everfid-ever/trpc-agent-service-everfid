package worker

import (
	"context"
	"encoding/json"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reliability/inflight"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/taskflow"
)

type completionCandidate struct {
	SchemaVersion int
	Envelope      runtime.ExecutionEnvelope
	InputDigest   string
	Permit        *governance.RunPermit
	Prepared      *governance.Finalization
	Usage         []governance.ModelUsage
	Output        OutboundContent
}

func (w RunnerExecutor) completionStore() messaging.CompletionStore {
	if w.Completions != nil {
		return w.Completions
	}
	s, _ := w.Payloads.(messaging.CompletionStore)
	return s
}
func envelopeDigest(e runtime.ExecutionEnvelope) string {
	b, _ := json.Marshal(e)
	return taskflow.Digest(b)
}
func (w RunnerExecutor) saveCompletion(ctx context.Context, e runtime.ExecutionEnvelope, fence uint64, before func(context.Context) error, payload messaging.PayloadRecord, permit governance.RunPermit, result runnerResult, out OutboundContent) (messaging.CompletionRecord, error) {
	s := w.completionStore()
	if s == nil {
		return messaging.CompletionRecord{}, runtime.ErrCapabilityUnsupported
	}
	c := completionCandidate{SchemaVersion: 1, Envelope: e, InputDigest: taskflow.Digest(payload.Content), Usage: runnerModelUsages(permit.Model, result), Output: out}
	if w.Governance != nil {
		c.Permit = &permit
		if guard, ok := w.Governance.(governance.PreparedRunGuard); ok {
			p, err := guard.PrepareFinalization(ctx, permit, c.Usage, out.Content)
			if err != nil {
				return messaging.CompletionRecord{}, err
			}
			c.Prepared = &p
		}
	}
	b, err := json.Marshal(c)
	if err != nil {
		return messaging.CompletionRecord{}, err
	}
	r := messaging.CompletionRecord{TenantID: e.TenantID, RequestID: e.RequestID, EnvelopeDigest: envelopeDigest(e), ContentDigest: taskflow.Digest(b), Content: b, KeyVersion: payload.KeyVersion, Fence: fence}
	if before != nil {
		if err = before(ctx); err != nil {
			return r, err
		}
	}
	if err = s.PutCompletion(ctx, r); err != nil {
		return r, err
	}
	return r, nil
}
func (w RunnerExecutor) resumeCompletion(ctx context.Context, turn *sessionstore.BufferedTurn, e runtime.ExecutionEnvelope, head sessionstore.SessionHead, fence uint64, before func(context.Context) error, payload messaging.PayloadRecord, r messaging.CompletionRecord) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.TenantID != e.TenantID || r.RequestID != e.RequestID || r.KeyVersion != payload.KeyVersion || r.EnvelopeDigest != envelopeDigest(e) || r.Fence > fence {
		return runtime.ErrVersionMismatch
	}
	var c completionCandidate
	if err := json.Unmarshal(r.Content, &c); err != nil {
		return runtime.ErrInvariantViolation
	}
	contentType, typeErr := messaging.NormalizeContentType(c.Output.ContentType)
	if c.SchemaVersion != 1 || c.InputDigest != taskflow.Digest(payload.Content) || verifyAuthoritativeEnvelope(c.Envelope, e) != nil || len(c.Output.Content) == 0 || typeErr != nil || contentType != c.Output.ContentType {
		return runtime.ErrVersionMismatch
	}
	if task := taskflow.FromContext(ctx); task != nil {
		if _, err := task.ValidateCompletion(ctx); err != nil {
			return err
		}
	}
	latest, err := w.Tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: e.TenantID, RequestID: e.RequestID})
	if err != nil {
		return err
	}
	if verifyAuthoritativeEnvelope(latest.Envelope, e) != nil {
		return runtime.ErrVersionMismatch
	}
	if latest.CancelRequested {
		return runtime.ErrCancelRequested
	}
	if before != nil {
		if err = before(ctx); err != nil {
			return err
		}
	}
	if w.Governance != nil {
		if c.Permit == nil || c.Permit.Policy.TenantID != e.TenantID || c.Permit.Policy.Version != e.PolicyVersion || c.Permit.Reservation.TenantID != e.TenantID || c.Permit.Reservation.RequestID != e.RequestID || c.Permit.Decision.Action != governance.ActionAllow {
			return runtime.ErrVersionMismatch
		}
		var decision governance.Decision
		if c.Prepared != nil {
			guard, ok := w.Governance.(governance.PreparedRunGuard)
			if !ok || c.Prepared.OutputDigest != taskflow.Digest(c.Output.Content) || c.Prepared.Decision.TenantID != e.TenantID || c.Prepared.Decision.RequestID != e.RequestID || c.Prepared.Decision.PolicyVersion != e.PolicyVersion || c.Prepared.Decision.ReservationID != c.Permit.Reservation.ReservationID || c.Prepared.Settlement.ExpectedVersion != c.Permit.Reservation.Version {
				return runtime.ErrVersionMismatch
			}
			decision, err = guard.CommitFinalization(ctx, *c.Prepared)
		} else {
			if _, ok := w.Governance.(governance.PreparedRunGuard); ok {
				return runtime.ErrVersionMismatch
			}
			if multi, ok := w.Governance.(governance.MultiModelRunGuard); ok {
				decision, err = multi.FinishModels(ctx, *c.Permit, c.Usage, c.Output.Content)
			} else if len(c.Usage) == 1 {
				decision, err = w.Governance.Finish(ctx, *c.Permit, c.Usage[0].Usage, c.Output.Content)
			} else {
				return runtime.ErrCapabilityUnsupported
			}
		}
		if err != nil {
			return err
		}
		if decision.Action != governance.ActionAllow {
			if task := taskflow.FromContext(ctx); task != nil {
				return w.commitTaskFailure(ctx, task, turn, e, head, fence, before, c.Permit.Model, payload.KeyVersion, "task_output_rejected")
			}
			return w.commitGovernanceTerminal(ctx, turn, e, head, fence, before, runtime.OutcomeDenied, decision)
		}
	} else if c.Permit != nil || c.Prepared != nil {
		return runtime.ErrVersionMismatch
	}
	if w.Barrier != nil {
		if err = w.Barrier.Wait(ctx, inflight.PointP2BeforeTerminalCommit); err != nil {
			return err
		}
	}
	ref, err := encodeResultRef(ctx, w.EncodeResult, e, string(c.Output.Content))
	if err != nil {
		return err
	}
	results, ok := w.Payloads.(messaging.ResultStore)
	if !ok {
		return runtime.ErrCapabilityUnsupported
	}
	if err = results.PutResult(ctx, messaging.ResultRecord{TenantID: e.TenantID, RequestID: e.RequestID, ResultRef: ref, ContentDigest: taskflow.Digest(c.Output.Content), Content: c.Output.Content, ContentType: c.Output.ContentType, KeyVersion: payload.KeyVersion}); err != nil {
		return err
	}
	if w.Barrier != nil {
		if err = w.Barrier.Wait(ctx, inflight.PointResultPersistedBeforeCommit); err != nil {
			return err
		}
	}
	return w.commitFrozenSuccess(ctx, turn, e, head, fence, before, ref)
}
