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
    PointP1 Point = "p1_persisted_before_dispatch"
    PointP2 Point = "p2_executed_before_commit"
    PointP3 Point = "p3_committed_before_send"
    PointP4 Point = "p4_accepted_before_confirmation"
)
type Observation struct{ Point Point; TenantID, RequestID, Owner string; At time.Time }
type Barrier interface{ Wait(context.Context, Observation) error }
type Status struct{ Enabled bool `json:"enabled"`; Point Point `json:"point,omitempty"`; Hit, Released bool }
```

`Controller.Wait`：point/tenant 不匹配直接返回 nil；匹配则 `hit=true` 并阻塞至 `Release()` 或 `ctx.Done()`。`Status` 有意不返回 tenant/request（健康接口不得成为路由元数据泄露面）。nil controller 安全（直接返回 nil / 零值）。启用条件：同时配置 `TRPC_INFLIGHT_TEST_BARRIER` 及（可选）`INSTANCE_ID`/`TENANT_ID`，否则不创建 Controller。

**为什么这么设计？** 测试只负责精确制造故障；自动接管仍由持久化状态、队列 reclaim、lease/fence、Delivery Ledger 完成。barrier 不创建任务、不删租约、不变更 Ledger、不补发回复。

---

## 二、P1 挂点：preprocess dispatch 之前

入口：`preprocess/worker.go` 的 `Worker.dispatch()`。

```go
// 仓库对应位置，可选核对：trpcservice/preprocess/worker.go:251
func (w Worker) dispatch(ctx context.Context, job Job) error {
    if w.InflightBarrier != nil {
        if err := w.InflightBarrier.Wait(ctx, inflight.Observation{
            Point: inflight.PointP1, TenantID: job.TenantID,
            RequestID: job.RequestID, Owner: w.Owner, At: w.now()}); err != nil {
            return err
        }
    }
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
if w.InflightBarrier != nil {
    if err := w.InflightBarrier.Wait(ctx, inflight.Observation{
        Point: inflight.PointP2, TenantID: envelope.TenantID,
        RequestID: envelope.RequestID, Owner: w.Owner, At: time.Now().UTC()}); err != nil {
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

## 四、P3 挂点：Delivery Ledger claim 后、Adapter 调用前

入口：`channels/delivery/service.go` 的 `deliverSegment()`。

```go
// 仓库对应位置，可选核对：trpcservice/channels/delivery/service.go:109
record, acquired, err := s.Ledger.ClaimDelivery(ctx, key, plan, messaging.DeliveryClaim{Owner: s.Owner, TTL: claimTTL})
if !acquired {
    switch record.State {
    case messaging.DeliverySent, messaging.DeliveryFailed: return nil
    case messaging.DeliveryAmbiguous: return s.reconcile(ctx, event, record)
    case messaging.DeliveryPending, messaging.DeliverySending, messaging.DeliveryRetryWait:
        return DeferredError{NotBefore: record.NotBefore}
    }
}
if s.InflightBarrier != nil {
    if err := s.InflightBarrier.Wait(ctx, inflight.Observation{
        Point: inflight.PointP3, TenantID: event.TenantID,
        RequestID: event.RequestID, Owner: s.Owner, At: time.Now().UTC()}); err != nil {
        return err
    }
}
record, resultDelivery, deliverErr := s.deliverWithClaimRenewal(ctx, adapter, channel.DeliveryRequest{
    Event: event, ClientRequestID: record.ClientRequestID, Target: event.Target,
    Content: append([]byte(nil), content...), ContentDigest: contentDigest, ContentType: contentType,
}, record, claimTTL)
```

**恢复路径：** relay 至少一次发布 reply outbox → delivery 消费 → `Deliver()` 校验 schema（`ResultRef == event.ContentRef` 否则 `ErrVersionMismatch`）、解析版本化 adapter（`adapter.ID() == event.Target.Channel` 否则 `ErrTenantScope`）、按 `MaxTextBytes()` 分片 → 每个 segment `deliverSegment`。模型执行与回复发送严格分离：**P3 故障只恢复 relay/delivery，不触发 `Execute`**。

**P3 关键事实：** 结果已保存于 `result_payload`，恢复只发送已保存内容。`deliverWithClaimRenewal` 在后台按 `claimTTL/3` 续租 claim，失败即取消调用 context——防止 owner 死亡后 claim 悬挂导致双发。

---

## 五、P4 挂点：Provider 接受证据后、持久化 sent 前

同一 `deliverSegment` 内，紧接 P3 之后：

```go
// 仓库对应位置，可选核对：trpcservice/channels/delivery/service.go:163
if !resultDelivery.Delivered {
    return s.finishRetry(ctx, record, runtime.ErrBackendUnavailable, 0)
}
if resultDelivery.ProviderMessageID == "" {
    record.State, record.LastErrorClass = messaging.DeliveryAmbiguous, "missing_provider_message_id"
    _, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
    return errors.Join(AmbiguousError{Err: runtime.ErrInvariantViolation}, finishErr)
}
if s.InflightBarrier != nil {
    if err := s.InflightBarrier.Wait(ctx, inflight.Observation{
        Point: inflight.PointP4, TenantID: event.TenantID,
        RequestID: event.RequestID, Owner: s.Owner, At: time.Now().UTC()}); err != nil {
        return err
    }
}
record.State = messaging.DeliverySent
record.ProviderMessageID = resultDelivery.ProviderMessageID
record.LastErrorClass = ""
_, err = s.Ledger.FinishDelivery(ctx, record, record.Version)
return err
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
| P3 命中 | `delivery_ledger.state='sending'` | 不变（暂停） | delivery |
| P3 发送 | `delivery_ledger.state='sending'` | `state='sent'` + `provider_message_id`（或 `ambiguous`） | FinishDelivery |
| P4 命中 | `delivery_ledger.state='sending'` | 不变（暂停，sent 未持久化） | delivery |
| P4 收敛 | `delivery_ledger.state='sending'/'ambiguous'` | `state='sent'` 或保留 `ambiguous` + 告警 | FinishDelivery/ReconcileDelivery |

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

## 十三、delivery 分片与 adapter 解析（P3/P4 前的准备）

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
