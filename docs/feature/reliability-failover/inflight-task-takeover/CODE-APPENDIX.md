# 在途任务接管与幂等投递 — 实现代码附录

> 本文直接摘录 P1–P4 测试控制面和关键生产链路，并逐段解释状态变化与故障语义。所有标识符与真实实现逐字一致（仓库对应位置，可选核对：`path`）。完整接口、DDL、CAS SQL、配置表见 [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)。

---

## 1. barrier 的边界和最小接口

```go
// 仓库对应位置，可选核对：trpcservice/reliability/inflight/barrier.go:15
type Point string
const (
    PointP1BeforeExecution        Point = "p1_persisted_before_execution"
    PointP2BeforeTerminalCommit   Point = "p2_executed_before_commit"
    PointP3BeforeReplyPublish     Point = "p3_result_committed_before_reply_publish"
    PointP4BeforeProviderDelivery Point = "p4_delivery_claimed_before_provider_send"
)
type Barrier interface { Wait(context.Context, Point) error }
type Snapshot struct { Point Point; Armed, Hit, Released bool }
```

**语义：** 业务组件只依赖 `Barrier`，不依赖 HTTP、Docker 或测试框架。正常运行时不注入它，生产任务不会暂停。`Snapshot` 只说明某个 point 是否 arm/hit/released，不含 tenant、request 或正文。

```go
// trpcservice/reliability/inflight/barrier.go
func (c *Controller) Arm(point Point) <-chan struct{}
func (c *Controller) Wait(context.Context, Point) error
func (c *Controller) Release(point Point) bool
func (c *Controller) Snapshot(point Point) Snapshot
```

---

## 2. Controller 不会替系统重试或修改状态

```go
// trpcservice/reliability/inflight/barrier.go
func (c *Controller) Wait(ctx context.Context, point Point) error {
    c.mu.Lock(); g := c.gates[point]; c.mu.Unlock()
    if g == nil { return nil }             // only explicitly armed points pause
    g.hitOnce.Do(func() { close(g.hit) })
    select { case <-ctx.Done(): return ctx.Err(); case <-g.release: return nil }
}
```

**故障语义：** 只有被 arm 的 point 停住。脚本轮询 point-scoped `status`，命中后强杀 node-a；controller 连同内存消失，但 `inbox`、任务 pending、lease、ledger 都未被它改变。`Release(point)` 只用于局部调试，**不重新入队、不删租约、不补发回复、不变更 ledger**。

---

## 3. P1：持久化 job 在 dispatch 前暂停

```go
// trpcservice/preprocess/worker.go: RunOnce
for _, job := range jobs {
    if w.Barrier != nil {
        if err := w.Barrier.Wait(ctx, inflight.PointP1BeforeExecution); err != nil { return processed, err }
    }
    // process(job) performs preprocess -> dispatch -> MarkDispatched
}
func (w Worker) dispatch(ctx context.Context, job Job) error {
    payloadRef := job.PayloadRef
    if job.PreparedPayloadRef != "" { payloadRef = job.PreparedPayloadRef }
    _, err := w.Dispatcher.Dispatch(ctx, gateway.DispatchRequest{
        Tenant: tenant.Context{TenantID: job.TenantID, TenantVersion: job.TenantVersion,
            AgentAppID: job.AgentAppID, SubjectID: job.UserID, Channel: job.Channel,
            TrustedSource: "channel_binding:" + job.ChannelBindingID},
        RequestID: job.RequestID, SessionID: job.SessionID, UserID: job.UserID,
        PayloadRef: payloadRef, ConfigVersion: job.ConfigVersion,
    })
    if err != nil { return err }
    _, err = w.Store.MarkDispatched(ctx, job, w.now())
    return err
}
```

**状态变化：** 命中时 `preprocess_job` 已是 `ready`（F1 完成），`execution_record` 尚未创建（F2 未发生）。节点被杀后，另一节点的 `RunOnce → ClaimReadyForDispatch` 扫描 `state='ready' AND dispatched_at IS NULL` 的 job 自动接力。`TrustedSource: "channel_binding:" + job.ChannelBindingID` 保留可信 binding 来源，保证转入异步链路后 tenant 归属不丢失。`MarkDispatched` 把 `dispatched_at` 写回，幂等。`dispatch` 内部 `prepare_dispatch` 对 `dispatch_ready` 返回既有 `input_seq` + `accepted=true`，重复调用收敛。

---

## 4. P2：模型/工具执行后、终态提交前暂停

```go
// 仓库对应位置，可选核对：trpcservice/worker/runner.go:618
outbound, err := renderOutbound(ctx, w.OutputRenderer, envelope, content)
if err != nil { return fmt.Errorf("render outbound result: %w", err) }
if w.Barrier != nil {
    if err := w.Barrier.Wait(ctx, inflight.PointP2BeforeTerminalCommit); err != nil {
        return err
    }
}
resultRef, err := encodeResultRef(ctx, w.EncodeResult, envelope, string(outbound.Content))
if err != nil { return err }
resultStore.PutResult(ctx, messaging.ResultRecord{TenantID: envelope.TenantID,
    RequestID: envelope.RequestID, ResultRef: resultRef, ContentDigest: hex.EncodeToString(resultDigest[:]),
    Content: outbound.Content, ContentType: outbound.ContentType, KeyVersion: payload.KeyVersion})
if beforeCommit != nil { if err := beforeCommit(ctx); err != nil { return err } }
replyID, _ := messaging.StableReplyID(messaging.ReplyCoordinate{
    TenantID: envelope.TenantID, RequestID: envelope.RequestID,
    InputSeq: envelope.InputSeq, Stage: "terminal", Ordinal: 0})
_, err = turn.Commit(ctx, sessionstore.CommitTurnRequest{
    SessionKey: sessionKey, RequestID: envelope.RequestID,
    CommitID: envelope.RequestID + ":terminal:0", Stage: "terminal",
    InputSeq: envelope.InputSeq, Fence: fence, ExpectedVersion: head.Version,
    Outcome: runtime.OutcomeSucceeded, ResultRef: resultRef,
    ReplyCursor: envelope.RequestID + ":1",
    Outbox: []sessionstore.OutboxEvent{
        {Kind: "reply", IdempotencyKey: replyID, PayloadRef: resultRef, EventSeq: 1,
            TraceParent: telemetry.EffectiveTraceParent(ctx, envelope.TraceParent)},
        terminalAuditOutbox(ctx, envelope, runtime.OutcomeSucceeded),
    },
})
if errors.Is(err, runtime.ErrAlreadyTerminal) { return nil }
```

**状态变化：** 命中时 `result_payload` 已写、`renderOutbound` 已生成终态内容，但 `commit_turn`（F3）尚未执行，`execution_record.outcome` 仍是 `running`。节点被杀后，存活 worker `Reclaim` 同 entry、取更高 fence、`ExecuteWithLease` 重新跑模型（**模型调用次数 +1 是允许的**），再 `commit_turn`。若旧 owner 晚到提交，`commit_turn` 中 `p_fence < v_head.last_fence` → `stale fence`（40001）→ `ErrVersionConflict`，不改变任何状态。`ErrAlreadyTerminal` 视为成功返回（已终态）。

**broker/ACK（consumer.go:300）：**

```go
// 仓库对应位置，可选核对：trpcservice/worker/consumer.go:300
if executeErr != nil { return executeErr }     // 不 ACK，保留 pending 供 reclaim
return w.Broker.Ack(ctx, delivery)            // 仅在 executeErr==nil 时 ACK
```

这是不变量 I5：`ACK 必须在 terminal commit 之后`。

---

## 5. P3：Reply Relay 发布前；P4：Delivery Ledger 的发送与不确定状态

P3 不在 Delivery Ledger。`trpcservice/relay/reply.go` 在从 durable result 和冻结 `ReplyRoute` 构造完 `ReplyEvent` 后执行 `r.Barrier.Wait(ctx, inflight.PointP3BeforeReplyPublish)`，再调用 `r.Replies.PublishReply(...)`。因此 P3 被杀只会留下可重领的 reply outbox claim；接管者重发同一 reply event，既不运行模型，也不调用 provider。

```go
// 仓库对应位置，可选核对：trpcservice/channels/delivery/service.go:109
record, acquired, err := s.Ledger.ClaimDelivery(ctx, key, plan,
    messaging.DeliveryClaim{Owner: s.Owner, TTL: claimTTL})
if err != nil { return err }
if !acquired {
    switch record.State {
    case messaging.DeliverySent, messaging.DeliveryFailed:
        return nil
    case messaging.DeliveryAmbiguous:
        return s.reconcile(ctx, event, record)
    case messaging.DeliveryPending, messaging.DeliverySending, messaging.DeliveryRetryWait:
        return DeferredError{NotBefore: record.NotBefore}
    }
}
if s.Barrier != nil {
    if err := s.Barrier.Wait(ctx, inflight.PointP4BeforeProviderDelivery); err != nil {
        return err
    }
}
record, resultDelivery, deliverErr := s.deliverWithClaimRenewal(ctx, adapter,
    channel.DeliveryRequest{Event: event, ClientRequestID: record.ClientRequestID,
        Target: event.Target, Content: append([]byte(nil), content...),
        ContentDigest: contentDigest, ContentType: contentType}, record, claimTTL)
if deliverErr != nil {
    var ambiguous AmbiguousError
    if errors.As(deliverErr, &ambiguous) {
        record.State, record.LastErrorClass = messaging.DeliveryAmbiguous, "response_lost"
        _, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
        return errors.Join(deliverErr, finishErr)
    }
    var permanent PermanentError
    if errors.As(deliverErr, &permanent) {
        return s.finishFailed(ctx, record, deliverErr, permanent.Class, false)
    }
    var retryAfter RetryAfterError
    if errors.As(deliverErr, &retryAfter) {
        return s.finishRetry(ctx, record, deliverErr, retryAfter.RetryAfter)
    }
    return s.finishRetry(ctx, record, deliverErr, 0)
}
if !resultDelivery.Delivered {
    return s.finishRetry(ctx, record, runtime.ErrBackendUnavailable, 0)
}
if resultDelivery.ProviderMessageID == "" {
    record.State, record.LastErrorClass = messaging.DeliveryAmbiguous, "missing_provider_message_id"
    _, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
    return errors.Join(AmbiguousError{Err: runtime.ErrInvariantViolation}, finishErr)
}
record.State = messaging.DeliverySent
record.ProviderMessageID = resultDelivery.ProviderMessageID
record.LastErrorClass = ""
_, err = s.Ledger.FinishDelivery(ctx, record, record.Version)
return err
```

**状态变化（P4）：** `ClaimDelivery` 成功后 `delivery_ledger.state='sending'`，`attempt` 递增，但 provider 尚未调用；节点若在此刻被杀，claim 到期后存活节点重新处理同一稳定 `client_request_id`。provider 调用后的网络中断仍可能形成 `ambiguous`：此时按 `client_request_id` 去重/对账收敛；下游不支持时保留 `ambiguous`，绝不伪造 receipt。

---

## 6. Ledger 状态分派（finish 家族）

```go
// 仓库对应位置，可选核对：trpcservice/channels/delivery/service.go:287
func (s Service) finishRetry(ctx context.Context, record messaging.DeliveryRecord, cause error, delay time.Duration) error {
    if record.Attempt >= s.maxAttempts() {           // 默认 MaxAttempts=8
        return s.finishFailed(ctx, record, cause, "retry_exhausted", false)
    }
    delay = s.backoff(record.Attempt, delay)
    record.State = messaging.DeliveryRetryWait
    record.NotBefore = time.Now().Add(delay)
    record.LastErrorClass = "retryable"
    _, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
    return errors.Join(cause, finishErr)
}
```

`backoff`：指数退避 `1<<(attempt-1)`（上限 `MaxRetryDelay` 默认 1min），`DefaultRetryDelay` 默认 1s。

`reconcile`：

```go
// 仓库对应位置，可选核对：trpcservice/channels/delivery/service.go:243
func (s Service) reconcile(ctx context.Context, event channel.ReplyEvent, record messaging.DeliveryRecord) error {
    if record.NotBefore.After(time.Now()) { return DeferredError{NotBefore: record.NotBefore} }
    adapter, err := s.resolveAdapter(ctx, event)
    if err != nil { return s.deferReconciliation(ctx, record, err) }
    reconciler, ok := adapter.(channel.DeliveryReconciler)
    if !ok { return s.deferReconciliation(ctx, record, runtime.ErrCapabilityUnsupported) }
    result, err := reconciler.ReconcileDelivery(ctx, channel.ReconciliationRequest{
        Event: event, ClientRequestID: record.ClientRequestID})
    if err != nil { return s.deferReconciliation(ctx, record, err) }
    switch result.Status {
    case channel.ReconciliationDelivered:            // 下游确认已接受
        record.State, record.ProviderMessageID, record.LastErrorClass =
            messaging.DeliverySent, result.ProviderMessageID, ""
        _, err = s.Ledger.ReconcileDelivery(ctx, record, record.Version); return err
    case channel.ReconciliationNotDelivered:        // 下游确认未接受
        record.State, record.NotBefore, record.LastErrorClass =
            messaging.DeliveryRetryWait, time.Now().UTC(), "reconciled_not_delivered"
        if _, err = s.Ledger.ReconcileDelivery(ctx, record, record.Version); err != nil { return err }
        return DeferredError{NotBefore: record.NotBefore}
    case channel.ReconciliationUnknown:             // 下游无法确定
        return s.deferReconciliation(ctx, record, runtime.ErrCommitConflict)
    default:
        return s.deferReconciliation(ctx, record, runtime.ErrInvariantViolation)
    }
}
```

`deferReconciliation`：超过 `MaxReconcileAttempts`(默认 8) → `finishFailed(..., 'reconcile_exhausted', reconcile=true)`；否则 `reconcile_attempt+1` + 退避 + `last_error_class='reconcile_retryable'`，返回 `DeferredError`。

---

## 7. retry 分类与错误类型

```go
// 仓库对应位置，可选核对：trpcservice/channels/delivery/service.go:31
type AmbiguousError = channel.AmbiguousDeliveryError
type RetryAfterError = channel.RetryableDeliveryError
type PermanentError = channel.PermanentDeliveryError
type DeferredError struct{ NotBefore time.Time }   // 让 account-queue 保持 pending
type TerminalError struct{ Err error }             // 已持久化 failed，可 ACK
```

| 错误 | 处理 | ledger 终态 | 是否可 ACK |
|---|---|---|---|
| `AmbiguousError` | `FinishDelivery` + 进入 reconcile | `ambiguous`/`response_lost` | 否（待对账） |
| `PermanentError` | `finishFailed` | `failed` | 是（TerminalError） |
| `RetryAfterError` | `finishRetry(delay)` | `retry_wait` | 否 |
| 其它 | `finishRetry(0)` | `retry_wait` | 否 |
| `Delivered=false` | `finishRetry(ErrBackendUnavailable)` | `retry_wait` | 否 |
| `ProviderMessageID==""` | `ambiguous` | `ambiguous`/`missing_provider_message_id` | 否 |

---

## 8. 启用契约（测试控制面开关）

```text
TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED=true       # 仅 webui-multinode 演练 profile
```

未启用 `TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED` 时 controller 不创建。启用后，以带 `X-TRPC-Local-Token` 的 `/test/failover/{p1|p2|p3|p4}/{arm,status,release}` 控制**单个 point**；状态只含 `point/armed/hit/released`，不含 tenant/request。标准 E2E arm node-a、命中后杀死 node-a，不向 survivor release。

**代码结论（四个断言）：**
- P1 证明可靠接收（F1 已 durable，未成为 execution）；
- P2 证明可接管执行（新 fence owner 完成原 request，旧 fence 被拒）；
- P3 证明已提交结果的 reply event 在发布前崩溃时由 relay 重放，模型不重跑；
- P4 证明 delivery claim 后、provider 调用前崩溃时由 ledger 接管；provider 调用结果不确定时仍以去重/对账收敛，否则 `ambiguous`。

---

## 9. worker consumer 的 handle 全貌

```go
// 仓库对应位置，可选核对：trpcservice/worker/consumer.go:181
func (w Consumer) handle(ctx context.Context, delivery broker.Delivery) error {
    key := coordination.SessionKey{TenantID: delivery.Envelope.TenantID,
        AgentAppID: delivery.Envelope.AgentAppID, SessionID: delivery.Envelope.SessionID}
    persistedFence, err := w.Sessions.ReadLastFence(ctx, sessionstore.SessionKey(key))
    if err != nil { return err }
    if err := w.Leases.EnsureFenceAtLeast(ctx, key, persistedFence); err != nil { return err }
    lease, err := w.acquire(ctx, key)                 // 冲突按 RetryWait 重试
    if err != nil { return err }
    defer func() { releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second); defer cancel(); _ = w.Leases.Release(releaseCtx, lease) }()
    executionCtx, cancelExecution := context.WithCancel(ctx)
    leaseLost := make(chan struct{})
    markLeaseLost := func() { leaseLostOnce.Do(func() { close(leaseLost); cancelExecution() }) }
    go func() { /* 续租 ticker: Renew 失败 → markLeaseLost */ }()
    beforeCommit := func(commitCtx context.Context) error {
        select { case <-leaseLost: return runtime.ErrLeaseLost; default: }
        if _, renewErr := w.Leases.Renew(commitCtx, lease, w.LeaseTTL); renewErr != nil {
            markLeaseLost(); return runtime.ErrLeaseLost
        }
        return nil
    }
    for {
        executeErr = w.Executor.ExecuteWithLease(executionCtx, delivery.Envelope, lease.Fence, beforeCommit)
        if !errors.Is(executeErr, runtime.ErrInputNotReady) { break }
        parked, parkErr := w.Parker.ParkInput(executionCtx, gateway.ParkRequest{...})
        switch parked.Disposition {
        case gateway.ParkInputReady: continue
        case gateway.ParkedInput, gateway.ParkInputTerminal: executeErr = nil
        case gateway.ParkInputBlocked: w.report(...); executeErr = nil
        default: executeErr = runtime.ErrInvariantViolation
        }
        break
    }
    if executeErr != nil { return executeErr }          // 不 ACK
    return w.Broker.Ack(ctx, delivery)                 // 仅 executeErr==nil
}
```

**故障语义：** 续租失败即 `markLeaseLost` 取消执行 context；`executeErr != nil` 时直接 return，原 stream entry 保持 pending，由存活节点 `Reclaim` 接管。这正是 I5（ACK 必在 terminal commit 后）的代码落点。

---

## 10. preprocess worker 的扫描与 dispatch 入口

```go
// 仓库对应位置，可选核对：trpcservice/preprocess/worker.go:76
func (w Worker) RunOnce(ctx context.Context, limit int) (int, error) {
    if w.LeaseTTL <= 0 { ttl = 30 * time.Second }
    jobs, err := w.Store.ClaimJobs(ctx, ClaimOptions{Owner: w.Owner, Now: now, TTL: ttl, Limit: limit})
    for _, job := range jobs { w.preprocess(ctx, job) }      // 校验 payload / 媒体预处理
    ready, err := w.Store.ClaimReadyForDispatch(ctx, ClaimOptions{...})
    for _, job := range ready { w.dispatch(ctx, job) }        // ← P1 挂点位于此
    return processed, nil
}
```

**状态变化：** `ClaimJobs` 取 `state='pending'` job 做 `preprocess`（产出 `ready` 或 `rejected`）；`ClaimReadyForDispatch` 取 `state='ready' AND dispatched_at IS NULL` 的 job 进入 `dispatch`。节点在 P1 被强杀后，`RunOnce` 周期扫描自然接力，无需人工入队。

---

## 11. delivery 的 reconcile / defer 实现

```go
// 仓库对应位置，可选核对：trpcservice/channels/delivery/service.go:348
func (s Service) deferReconciliation(ctx context.Context, record messaging.DeliveryRecord, cause error) error {
    nextAttempt := record.ReconcileAttempt + 1
    if nextAttempt >= s.maxReconcileAttempts() {           // 默认 8
        return s.finishFailed(ctx, record, cause, "reconcile_exhausted", true)
    }
    record.ReconcileAttempt = nextAttempt
    record.NotBefore = time.Now().Add(s.backoff(nextAttempt, 0))
    record.LastErrorClass = "reconcile_retryable"
    updated, err := s.Ledger.DeferDeliveryReconciliation(ctx, record, record.Version)
    if err != nil { return errors.Join(cause, err) }
    return DeferredError{NotBefore: updated.NotBefore}
}
```

`DeferDeliveryReconciliation` 的 SQL `WHERE ... AND state='ambiguous' AND client_request_id=$8 AND reconcile_attempt=$4-1` 用版本 + 重试计数双重 CAS，避免并发重入对账。`finishFailed(..., reconcile=true)` 走 `ReconcileDelivery` 而非 `FinishDelivery`。

---

## 12. 会话存储的 SQLSTATE → 错误映射

```go
// 仓库对应位置，可选核对：trpcservice/storage/session/postgres/store.go（translate）
// 40001 且消息含 "stale fence" → ErrStaleFence，否则 ErrVersionConflict
// 23505 → ErrCommitConflict（idempotency collision）
// XX001 → ErrInvariantViolation（terminal invariant missing）
// 55000 → ErrInputNotReady
// P0902 → ErrCancelRequested
// 42501 → ErrTenantScope
// sql.ErrNoRows → ErrNotFound
```

`OpenForRun` 的关键判断：`input_seq < next_input_seq` → `ErrAlreadyTerminal`；`>` → `ErrInputNotReady`；`fence < last_fence` → `ErrStaleFence`。这三条是 P2 接管「旧 owner 迟到提交被拒」与「前序未终态不得提交」的数据库层断言。

---

## 13. 租约 Redis Lua 的精确语义（摘录）

```lua
-- acquire：仅当 lease 不存在才成功，并 INCR fence
if redis.call('EXISTS', KEYS[1]) == 1 then return {0, '0'} end
redis.call('INCR', KEYS[2])
local fence = redis.call('GET', KEYS[2])
redis.call('PSETEX', KEYS[1], ARGV[3], ARGV[1]..'|'..ARGV[2]..'|'..fence)
return {1, fence}

-- ensureFence：current < minimum 时 SET minimum（十进制字符串比较）
local function decimal_less(left, right)
  left = normalize(left); right = normalize(right)
  if string.len(left) ~= string.len(right) then return string.len(left) < string.len(right) end
  return left < right
end
local current = redis.call('GET', KEYS[1]) or '0'
if decimal_less(current, ARGV[1]) then redis.call('SET', KEYS[1], ARGV[1]) end
return 1
```

**故障语义：** `acquire` 冲突返回 `runtime.ErrVersionConflict`；`EnsureFenceAtLeast` 拒绝 `minimum >= math.MaxInt64`（Redis INCR 是 signed 64-bit，须留一个可用值）→ `ErrInvariantViolation`。`workerID` 不得为空、不得含 `|`；`ttl` 必须 >0 且 ≥1ms。`Renew`/`Release` 必须三者（owner、leaseID、fence）整体比对（值串比较），防止误删他人 lease。

---

## 14. 错误的语义分类（delivery 内）

```go
type AmbiguousError = channel.AmbiguousDeliveryError   // 下游接受状态未知
type RetryAfterError = channel.RetryableDeliveryError  // 可重试（带 delay）
type PermanentError  = channel.PermanentDeliveryError  // 永久失败
type DeferredError   struct{ NotBefore time.Time }     // 保持 account-queue pending
type TerminalError   struct{ Err error }                // 已持久化 failed，可 ACK
```

`TerminalError` 表示已持久化 `failed`，account-queue entry 可 ACK；`DeferredError` 让 entry 保持 pending，等另一 delivery 尝试或 `not_before` 到期。两者都不能被误判为「成功」，也不能被误判为「需立即重发」。

## 当前实现增量：代码证据索引

| 证据 | 位置 | 说明 |
|---|---|---|
| 恢复契约 | `trpcservice/tool/recovery.go` | 四种 recovery policy 与 queryable 接口 |
| 工具 lease | `trpcservice/tool/execution/postgres/store.go` | Claim/Renew/Finish 的 owner+fence CAS |
| Guard | `trpcservice/tool/guard.go` | 先 Claim、一次 Grant 消费、恢复已消费的不确定 attempt |
| 续办 | `trpcservice/worker/runner.go` | consumed confirmation 先复用结果，再进入受保护恢复 |
| 数据迁移 | `migrations/000002_tool_execution_recovery.up.sql` | `tool_execution` 表、索引和约束 |
