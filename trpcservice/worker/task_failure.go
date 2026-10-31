package worker

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/taskflow"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

// The stopped plan precedes the failure result and terminal. Each boundary
// is replayable under the original request; a restart cannot restart effects.
func (w RunnerExecutor) commitTaskFailure(ctx context.Context, task *taskflow.Session, turn *sessionstore.BufferedTurn, e runtime.ExecutionEnvelope, head sessionstore.SessionHead, fence uint64, before func(context.Context) error, modelRef governance.VersionedRef, keyVersion int64, reason string) error {
	if _, err := task.Fail(ctx, reason); err != nil {
		return err
	}
	if w.Governance != nil {
		if err := w.Governance.Abort(ctx, e, modelRef, reason); err != nil && !errors.Is(err, runtime.ErrNotFound) {
			return err
		}
		decision := governance.Decision{DecisionID: governance.StableDecisionID(e.TenantID, e.RequestID, reason, e.PolicyVersion), TenantID: e.TenantID, RequestID: e.RequestID, Stage: "task_failure", Action: governance.ActionDeny, ReasonCode: reason, PolicyVersion: e.PolicyVersion}
		if err := w.Governance.Record(ctx, decision); err != nil {
			return err
		}
	}
	content := []byte("任务未能完成，已安全停止并保存执行记录。原因：" + reason + "。请核查记录后再决定后续处理，避免重复执行外部操作。")
	ref, err := encodeResultRef(ctx, w.EncodeResult, e, string(content))
	if err != nil {
		return err
	}
	results, ok := w.Payloads.(messaging.ResultStore)
	if !ok {
		return runtime.ErrCapabilityUnsupported
	}
	if err = results.PutResult(ctx, messaging.ResultRecord{TenantID: e.TenantID, RequestID: e.RequestID, ResultRef: ref, ContentDigest: taskflow.Digest(content), Content: content, ContentType: messaging.ContentTypeText, KeyVersion: keyVersion}); err != nil {
		return err
	}
	if turn != nil {
		_ = turn.Rollback(ctx)
	}
	if before != nil {
		if err = before(ctx); err != nil {
			return err
		}
	}
	replyID, err := messaging.StableReplyID(messaging.ReplyCoordinate{TenantID: e.TenantID, RequestID: e.RequestID, InputSeq: e.InputSeq, Stage: "terminal", Ordinal: 0})
	if err != nil {
		return err
	}
	_, err = w.Sessions.CommitTurn(ctx, sessionstore.CommitTurnRequest{SessionKey: head.SessionKey, RequestID: e.RequestID, CommitID: e.RequestID + ":task-failure", Stage: "terminal", InputSeq: e.InputSeq, Fence: fence, ExpectedVersion: head.Version, Outcome: runtime.OutcomeFailed, ResultRef: ref, ReplyCursor: e.RequestID + ":1", Outbox: []sessionstore.OutboxEvent{
		{Kind: "reply", IdempotencyKey: replyID, PayloadRef: ref, EventSeq: 1, TraceParent: telemetry.EffectiveTraceParent(ctx, e.TraceParent)}, terminalAuditOutbox(ctx, e, runtime.OutcomeFailed),
	}})
	if errors.Is(err, runtime.ErrAlreadyTerminal) {
		return nil
	}
	return err
}
