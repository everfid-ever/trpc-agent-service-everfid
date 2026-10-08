# 在途任务接管与幂等投递 — 实现走读

> 本文逐模块说明接管的恢复路径与真实标识符。P1–P4 controller、四个挂点、Delivery Ledger 的直接代码见 [CODE-APPENDIX.md](./CODE-APPENDIX.md)；完整接口与 DDL 见 [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)。
> 文中（仓库对应位置，可选核对：`path`）仅作索引，不作为唯一说明。

---

## 一、barrier 接口：测试的「确定性暂停」

包 `inflight` 提供 opt-in、process-local 的 barrier。它不是调度器，不 reclaim 工作，只用于在达到 durable boundary 时暂停，供测试人员强杀该进程。

```go
// 仓库对应位置，可选核对：trpcservice/reliability/inflight/barrier.go
type Point string
const (
    PointP1BeforeExecution        Point = "p1_persisted_before_execution"
    PointP2BeforeTerminalCommit   Point = "p2_executed_before_commit"
    PointP3BeforeReplyPublish     Point = "p3_result_committed_before_reply_publish"
    PointP4BeforeProviderDelivery Point = "p4_delivery_claimed_before_provider_send"
)
type Barrier interface{ Wait(context.Context, Point) error }
type Snapshot struct { Point Point; Armed, Hit, Released bool }
```

`Controller.Wait` 在未 arm 的 point 上直接返回；命中已 arm 的 point 后置 `hit` 并阻塞至 `Release(point)` 或 `ctx.Done()`。它没有 tenant/request 过滤功能，也不把状态暴露到健康接口。测试组合仅通过 `TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED=true` 创建 controller，并用带 `X-TRPC-Local-Token` 的本地控制面分别 arm/status/release 某一个 point：`/test/failover/{p1|p2|p3|p4}/{arm,status,release}`。

**为什么这么设计？** 测试只负责精确制造故障；自动接管仍由持久化状态、队列 reclaim、lease/fence、Delivery Ledger 完成。barrier 不创建任务、不删租约、不变更 Ledger、不补发回复。

---

## 二、P1 挂点：preprocess dispatch 之前

入口：`preprocess/worker.go` 的 `Worker.RunOnce()`；它在已领取 preprocess job、任何预处理和 dispatch 之前暂停。

```go
// trpcservice/preprocess/worker.go
for _, job := range jobs { // jobs were durably claimed
    if w.Barrier != nil {
        if err := w.Barrier.Wait(ctx, inflight.PointP1BeforeExecution); err != nil {
            return processed, err
        }
    }
    // process(job): preprocess, Dispatcher.Dispatch, then MarkDispatched
}
```

**恢复路径（不依赖 barrier，生产代码）：**

1. `RunOnce` → `ClaimJobs`（取 `state='pending'` 的 job 做 `preprocess` 校验）→ `ClaimReadyForDispatch`（取 `state='ready' AND dispatched_at IS NULL` 的 job）。
2. 节点死亡后，另一节点的 `RunOnce` 周期扫描自然接力；`dispatch` 只需重跑，`Dispatcher.Dispatch` 内部 `prepare_dispatch` 幂等收敛到同一 `input_seq`。

**P1 关键事实：** job 已是 durable preprocess 事实，但尚未成为 execution。节点被杀后，恢复者必须从未完成 job 自动继续 dispatch；用户无需重发，测试人员也不得手工重新入队。`TrustedSource` 保留可信 binding 来源，保证转入异步链路后 tenant 归属不丢失。

---

## 三、P2 挂点：模型/工具执行后、终态提交前

入口：`worker/runner.go` 的 `ExecuteWithLease` 提交前。

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
// ... PutResult ...
if beforeCommit != nil { if err := beforeCommit(ctx); err != nil { return err } }
// ... turn.Commit(ctx, CommitTurnRequest{..., Fence: fence, ExpectedVersion: head.Version,
//     Outcome: runtime.OutcomeSucceeded, ResultRef: resultRef, ...}) ...
```

**恢复路径（consumer.go 的 `handle()`）：**

```go
// 仓库对应位置，可选核对：trpcservice/worker/consumer.go:181
key := coordination.SessionKey{TenantID:..., AgentAppID:..., SessionID:...}
persistedFence, _ := w.Sessions.ReadLastFence(ctx, sessionstore.SessionKey(key))
w.Leases.EnsureFenceAtLeast(ctx, key, persistedFence)   // 校准 Redis fence
lease, _ := w.acquire(ctx, key)                          // Acquire，冲突按 RetryWait 重试
defer w.Leases.Release(...)
// 续租 goroutine：Renew 失败 → markLeaseLost() → cancelExecution
beforeCommit := func(c context.Context) error {
    select { case <-leaseLost: return runtime.ErrLeaseLost; default: }
    if _, e := w.Leases.Renew(c, lease, w.LeaseTTL); e != nil { markLeaseLost(); return runtime.ErrLeaseLost }
    return nil
}
executeErr = w.Executor.ExecuteWithLease(executionCtx, envelope, lease.Fence, beforeCommit)
// ErrInputNotReady → Parker.ParkInput（Ready 重试 / Terminal 视为成功 / Blocked 上报）
// 执行成功 → Broker.Ack；executeErr != nil → 直接 return（不 ACK，保留 pending 供 reclaim）
```

**P2 关键事实：** 模型调用允许大于一次（模型内部状态不可跨节点恢复）。系统追求**一次有效 terminal commit**，靠 `commit_turn` 的 `p_fence < v_head.last_fence` 拒绝旧 owner。`AcquireLeaseAfterCalibratingFromSession` 用 durable `last_fence` 修正 Redis fence，避免 Redis 数据丢失后 fence 倒退。`LeaseStillValid`（beforeCommit 最后一道检查）是数据库 fence 前的快速闸。

---

## 四、P3 挂点：Reply Relay 构造 reply event 后、发布前

入口：`relay/reply.go` 的 `ReplyRelay.handle()`。此时 terminal result、reply outbox 和冻结的 reply route 都已经 durable；尚未向 reply queue 发布事件。

```go
// trpcservice/relay/reply.go
event := channel.ReplyEvent{TenantID: record.TenantID, RequestID: record.AggregateID,
    ChannelBindingID: route.ChannelBindingID, DeliveryKey: record.IdempotencyKey,
    ContentRef: result.ResultRef, Target: frozenTarget, Final: true}
if r.Barrier != nil {
    if err := r.Barrier.Wait(ctx, inflight.PointP3BeforeReplyPublish); err != nil { return err }
}
return r.Replies.PublishReply(ctx, destination, event)
```

**恢复路径：** Relay 的 outbox claim 到期后由另一个节点重新领取、重新构造相同 reply event 并发布；之后 delivery 消费该事件。模型执行与回复发送严格分离：**P3 故障只恢复 relay publish，不触发 `Execute`，也尚未 claim Delivery Ledger**。

**P3 关键事实：** 结果已保存于 `result_payload`，恢复使用冻结路由发布同一 reply event；其后的 delivery ledger 才负责发送端去重。

---

## 五、P4 挂点：Delivery Ledger 已领取、Provider 调用前

入口为 `channels/delivery/service.go` 的 `deliverSegment()`，在 `ClaimDelivery` 成功后、`deliverWithClaimRenewal` 调用 adapter 之前：

```go
// trpcservice/channels/delivery/service.go
record, acquired, err := s.Ledger.ClaimDelivery(ctx, key, plan, claim)
if !acquired { return deferredOrReconcile(record) }
if s.Barrier != nil {
    if err := s.Barrier.Wait(ctx, inflight.PointP4BeforeProviderDelivery); err != nil { return err }
}
record, resultDelivery, err := s.deliverWithClaimRenewal(ctx, adapter, request, record, claimTTL)
// only after the provider outcome is known: FinishDelivery(sent/retry_wait/ambiguous)
```

**错误分类（同一函数内）：**

| 错误类型 | 处理 | ledger 状态 |
|---|---|---|
| `AmbiguousError` | `FinishDelivery` + reconcile | `ambiguous`, `last_error_class='response_lost'` |
| `PermanentError` | `finishFailed(..., reconcile=false)` | `failed` |
| `RetryAfterError` | `finishRetry(请求的 delay)` | `retry_wait` |
| 其它 | `finishRetry(0)` | `retry_wait` |
| `Delivered=false` | `finishRetry(ErrBackendUnavailable)` | `retry_wait` |
| `ProviderMessageID==""` | `ambiguous` | `ambiguous`, `last_error_class='missing_provider_message_id'` |

新 owner 对 `ambiguous` 记录调用 `ReconcileDelivery`：只有能得到可信「已接受/未接受」证据时才自动闭环；否则保留状态并触发告警，防止把用户体验写成 exactly-once。

---

## 六、relay：Outbox 到 account queue 的桥

relay 负责把 `outbox`（`kind='reply'`, `state='pending'`）至少一次发布到 account queue：

- `ClaimOutbox(kind, state='pending')` → 发布到对应 channel 的 account queue → `MarkPublished`（CAS：`state='claimed' AND version=$3`）。
- 发布失败 → `MarkRetry`（`state='retry_wait'`, `next_attempt_at`），由扫描补发。
- `outbox_id` 格式 `format('%s:%s', kind, idempotency_key)`，唯一约束 `outbox_tenant_id_kind_idempotency_key_key`，保证 replay 收敛。

`reconcile()`（delivery 内）：`NotBefore` 未到 → Deferred；adapter 必须实现 `DeliveryReconciler`（否则 `deferReconciliation`）；`ReconciliationDelivered` → sent；`ReconciliationNotDelivered` → `retry_wait` + `reconciled_not_delivered`；`ReconciliationUnknown` → 继续 defer；超过 `MaxReconcileAttempts`(默认 8) → `finishFailed(..., 'reconcile_exhausted', reconcile=true)`。

---

## 七、端到端恢复时序（无 barrier 的生产路径）

```text
callback → claim_inbox(F1) ───────────────────────────────▶ inbox durable
preprocess: ClaimReadyForDispatch → dispatch → prepare_dispatch(F2: execution_record, dispatch_ready)
worker: Reclaim/Consume → acquire lease(fence) → ExecuteWithLease(模型/工具, 可 >1 次)
        → PutResult → commit_turn(F3: head+commit+reply outbox 单事务；events=null，转写由官方会话后端持有)
relay: scan reply outbox → account queue
delivery: Deliver → 分片 → ClaimDelivery(F4) → adapter.Deliver → FinishDelivery(sent, F5)
```

任一节点在任意箭头之间死亡，存活节点都从**该箭头之前最后一个 durable 状态**接力，不重做已完成步骤（尤其是 P3 不重跑模型，P4 不盲重发）。这正是五个持久化事实分段落库的价值。

---

## 八、实现要点清单（照抄校验）

- [ ] `claim_inbox` 唯一键 `(tenant_id, channel, external_account_id, external_message_id)` + digest 比对防碰撞。
- [ ] `prepare_dispatch` 在单事务分配 `input_seq` 并写 `dispatch` outbox（`idempotency_key='dispatch:<request_id>'`）。
- [ ] `commit_turn` 单事务写 head/commit/outbox（`p_events` 生产为 null），`p_fence < last_fence` 拒绝旧 owner。
- [ ] worker `handle` 只在 `executeErr==nil` 时 `Broker.Ack`（I5）。
- [ ] `park_execution` 处理 `ErrInputNotReady`，`p_max_attempts` ≤ 64。
- [ ] `ClaimDelivery` 先回收超时 sending claim 再插入/更新，`client_request_id` 由 `StableDeliveryRequestID` 推导。
- [ ] `FinishDelivery`/`ReconcileDelivery` 均校验 `version + state + claim_owner + client_request_id` 三重条件。
- [ ] 四个 barrier 挂点位置与 P1–P4 语义一一对应，且仅在配置时启用。

---

## 九、worker consumer 的 reclaim 与 ACK 时序

`Run()`（仓库对应位置，可选核对：`trpcservice/worker/consumer.go:47`）的骨架：

```go
reclaim goroutine: 每 ReclaimInterval(装配值 5s，代码兜底 1s) → Broker.Reclaim(consumerID, limit=ReclaimLimit(100))
    → 每条走 process → handle
Consume 回调: 执行失败【故意返回 nil】→ 让空闲 delivery 可被 reclaim，worker 不退出
process(delivery): for { err := handle(...); if !Is(ErrCommitConflict|ErrVersionConflict) { return err }; wait(RetryWait) }
```

`handle()` 的租约/提交闭环（逐段）：

```go
key := coordination.SessionKey{TenantID, AgentAppID, SessionID}
persistedFence, _ := w.Sessions.ReadLastFence(ctx, sessionstore.SessionKey(key))
w.Leases.EnsureFenceAtLeast(ctx, key, persistedFence)      // 校准 Redis fence
lease, _ := w.acquire(ctx, key)                            // Acquire，冲突按 RetryWait 重试
defer w.Leases.Release(releaseCtx, lease)                  // 1s 超时 context
// 续租 goroutine: Renew 失败 → markLeaseLost() → cancelExecution
beforeCommit := func(c context.Context) error {
    select { case <-leaseLost: return runtime.ErrLeaseLost; default: }
    if _, e := w.Leases.Renew(c, lease, w.LeaseTTL); e != nil { markLeaseLost(); return runtime.ErrLeaseLost }
    return nil
}
executeErr = w.Executor.ExecuteWithLease(executionCtx, envelope, lease.Fence, beforeCommit)
// ErrInputNotReady → Parker.ParkInput（Ready 重试 / Terminal 视为成功 / Blocked 上报）
// 取消监听: cancelRequested → CancelWithLease
if executeErr != nil { return executeErr }                 // 不 ACK，保留 pending 供 reclaim
return w.Broker.Ack(ctx, delivery)                         // 仅 executeErr==nil 时 ACK
```

**铁律（I5）**：`ACK 必须在 terminal commit 之后`。`executeErr==nil` 意味着 `CommitTurn` 已成功，此时才 ACK；否则原队列消息保持 pending，存活节点通过 `Reclaim` 接管。

---

## 十、runner 的 terminal 提交路径

`ExecuteWithLease`（仓库对应位置，可选核对：`trpcservice/worker/runner.go:103`）在 `commitGovernanceTerminal` / `commitBudgetTerminal` 等最终都会走到同一 `CommitTurn` 调用：

```go
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
        {Kind: "reply", IdempotencyKey: replyID, PayloadRef: resultRef, EventSeq: 1, TraceParent: ...},
        terminalAuditOutbox(ctx, envelope, runtime.OutcomeSucceeded),
    },
})
if errors.Is(err, runtime.ErrAlreadyTerminal) { return nil }
```

`CommitTurn` 内部调用 `sessionstore.ValidateCommit` + `CommitDigest`，随后 `SELECT ... FROM commit_turn($1..$17)`；`already_terminal=true` 时返回 `ErrAlreadyTerminal`。这是「一次 terminal commit」的代码落点：无论哪个 owner 提交，`commit_turn` 的 `p_fence < last_fence` 与 `session_commit_terminal_input_idx` 唯一索引保证只一个终态。

---

## 十一、状态字段变化对照（每阶段）

| 阶段 | 变更前 | 变更后 | 责任组件 |
|---|---|---|---|
| P1 命中 | `preprocess_job.state='ready'` | 不变（暂停） | preprocess worker |
| P1 恢复 | `preprocess_job.state='ready'` | `execution_record` 创建 + `inbox`→`dispatch_ready` | prepare_dispatch |
| P2 命中 | `execution_record.outcome='running'` | 不变（暂停） | worker runner |
| P2 提交 | `execution_record.outcome='running'` | `session_head.next_input_seq+1` + `session_commit` 终态 + `outbox` reply pending | commit_turn |
| P3 命中 | `outbox(kind='reply').state='claimed'` | 不变（reply event 尚未发布） | Reply Relay |
| P3 恢复 | `outbox.state='claimed'` 的 claim 到期 | 重新发布同一个 `ReplyEvent` | Reply Relay |
| P4 命中 | `delivery_ledger.state='sending'` | 不变（provider 尚未调用） | delivery |
| P4 收敛 | `delivery_ledger.state='sending'/'ambiguous'` | `state='sent'`，或保留 `ambiguous` + 告警 | FinishDelivery/ReconcileDelivery |

---

## 十二、preprocess 校验链（保 F1 质量）

`preprocess(ctx, job)`（仓库对应位置，可选核对：`trpcservice/preprocess/worker.go:108`）在 `dispatch` 之前保证入队数据合法：

```go
payload, _ := w.Payloads.GetPayload(ctx, job.TenantID, job.RequestID)
sum := sha256.Sum256(payload.Content)
if payload.PayloadRef != job.PayloadRef || payload.ContentDigest != hex.EncodeToString(sum[:]) {
    return w.Store.FinishRejected(ctx, job, "payload_integrity")   // 完整性失败
}
// 解析 NormalizedInput，校验必填字段与 message_type（text/image/file）
if !validNormalizedInput(normalized) { return w.Store.FinishRejected(ctx, job, "invalid_text_payload") }
if len(normalized.MediaRefs) > 0 {
    return w.prepareMedia(ctx, job, ...)   // 媒体：Stage → PutPreparedPayload → FinishReady
}
return w.Store.FinishReady(ctx, job)        // 文本：直接 ready
```

**恢复语义：** `FinishReady` 后 `preprocess_job.state='ready'`，进入 `ClaimReadyForDispatch` 的扫描范围；`FinishRejected` 写 `reject_reason` 终态，不再 dispatch。这保证 P1 接管时接手的是**已校验**的 job，而非脏数据。

---

## 十三、delivery 分片与 adapter 解析（P4 前的准备）

`Deliver()`（仓库对应位置，可选核对：`trpcservice/channels/delivery/service.go:72`）在 `deliverSegment` 前完成：

```go
result, _ := messaging.ResolveReplyContent(ctx, s.Results, event.TenantID, event.RequestID, event.ContentRef)
if result.ResultRef != event.ContentRef { return runtime.ErrVersionMismatch }   // 内容必须对应
adapter, _ := s.resolveAdapter(ctx, event)
if adapter == nil || adapter.ID() != event.Target.Channel { return runtime.ErrTenantScope }  // 租户/渠道一致
if contentType == messaging.ContentTypeText { segments = splitText(result.Content, maxTextBytes(adapter)) }  // 按 MaxTextBytes 分片
plan := messaging.DeliveryPlan{RendererVersion:..., FormatVersion:..., ContentDigest: result.ContentDigest, SegmentCount: len(segments)}
for segmentNo, content := range segments { s.deliverSegment(ctx, event, adapter, plan, segmentNo, content, contentType) }
```

每个 segment 独立 `deliverSegment` → 独立 `delivery_ledger` 行（key = `(tenant_id, delivery_key, segment_no)`），互不影响；一个 segment 的 `ambiguous` 不会拖垮其它 segment。

## 当前实现增量：确认工具被接管时的运行时顺序

1. Runner 发现 confirmation 已 `consumed`，先读取 `tool_result_payload`；有结果则直接续办模型。
2. 无结果且 `tool_attempt=effect_unknown` 时，重新解析同一固定版本工具。
3. Guarded callable 先 Claim `tool_execution`，再检测 Grant 已消费；不会第二次消费 Grant。
4. 按 `queryable`/`idempotent_key`/`replay_safe`/`manual` 决定查询、重试、重放或安全终止。
5. 成功后按顺序写工具结果、完成 tool attempt、完成 tool execution，再把结果作为同一 tool call 的 continuation 输入。
