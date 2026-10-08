# 故障恢复后继续对话 — 实现走读

> 本文按**请求生命周期顺序**逐模块走读完整调用链：启动装配 → 可信入口 → Inbox → Dispatch → Worker 接管 → 会话存储 → 原子提交 → Relay → Delivery → 故障接管。
> 每一步都回答两个问题：**它的 durable truth 在哪里？此刻进程被杀会怎样？**
> 直接可复制的代码摘录见 `CODE-APPENDIX.md`；从零复现材料见 `REFERENCE-IMPLEMENTATION.md`。真实 WeCom 回调与手机端会话属外部验收边界（`testing-and-acceptance.md` §9）。

---

## 一、启动阶段：装配身份与就绪

### 1.1 配置与身份

1. 读取 `TRPC_WEBUI_LOCAL_INSTANCE_ID`：**部署槽位**身份（N1/N2 必须不同）。
2. 生成 `ProcessStartID`：进程启动时 `crypto/rand` 取 8 字节 → hex（16 字符）。**不可配置、每次启动不同**。
3. 加载两个 active WeCom binding，校验 tenant、secret scope 与 config version 完整。
4. 第二 Bot 的强校验（缺一即启动失败）：
   - `TRPC_WECOM_SECONDARY_LOCAL_ENABLED=true` 时，`WECOM_SECONDARY_AGENT_ID` 必须可解析为正整数；
   - `WECOM_SECONDARY_CORP_ID`、`WECOM_SECONDARY_APP_SECRET`、`WECOM_SECONDARY_CALLBACK_TOKEN`、`WECOM_SECONDARY_ENCODING_AES_KEY` 必须非空；
   - 否则报错 `secondary WeCom local configuration is incomplete`；
   - 且 `(Corp ID, Agent ID)` 元组必须与主 Bot **不同**，否则报错 `secondary WeCom local configuration is incompatible`。

### 1.2 owner 命名公式

```go
func (value webUILocalConfig) instanceName(component string) string {
	return "webui-local-" + component + "-" + value.InstanceID + "-" + value.ProcessStartID
}
```

装配到以下七个组件（owner 用于 lease、queue consumer 和日志关联，**不经 `/statusz` 暴露**）：

| 组件 | 角色 | **实际装配参数** |
|---|---|---|
| `worker` | 消费 Work Stream、持有 session lease、执行、提交 | `LeaseTTL=30s`、`RenewInterval=10s`、`RetryWait=250ms`、`ReclaimInterval=5s`、`ReclaimLimit=100`、`DrainTimeout=30s`；Shards `{0,1,2,3}` |
| `preprocess` | 认领 `preprocess_job`、准备输入、dispatch | `LeaseTTL=30s`、`RetryDelay=1s`、`MaxAttempts=8`、`ArtifactRetention=24h` |
| `dispatch-relay` | 把 dispatch outbox 发布为执行任务 | `ClaimTTL=30s`、`ClaimRenewInterval=10s`、`PollInterval=100ms`、`ShardCount=4` |
| `reply-relay` | 把 reply outbox 发布到 Reply Stream | `ClaimTTL=30s`、`ClaimRenewInterval=10s`、`PollInterval=100ms` |
| `wakeup-relay` | 发布 wakeup 事件 | `ClaimTTL=30s`、`ClaimRenewInterval=10s`、`PollInterval=100ms` |
| `wakeup` | 消费 wakeup 队列 | `ReclaimInterval=5s`、`ReclaimLimit=100`；队列 group `webui-wakeup`、ReadBlock 250ms、ReclaimIdle 30s |
| `delivery` | 领取 Delivery Ledger、调用渠道、置终态 | `ClaimTTL=30s`、`ClaimRenewInterval=10s`、`DefaultRetryDelay=1s`、`MaxRetryDelay=1min`、`MaxAttempts=8`、`MaxReconcileAttempts=8`；consumer `ReclaimInterval=5s`、`ReclaimLimit=100`、队列 group `webui-delivery`、ReadBlock 250ms、ReclaimIdle 30s |

> ⚠️ **区分"装配值"与"代码级兜底默认值"。** 上表是 `webui-local` 角色显式传入的装配值（部署实际生效值）。组件内部另有兜底默认，仅在调用方未设置时生效，例如 `worker.Consumer` 的 `LeaseTTL<=0` → 5s、`RenewInterval<=0` → `LeaseTTL/3`、`ReclaimInterval<=0` → 1s、`RetryWait<=0` → 10ms。**文档与告警阈值必须引用装配值**；把兜底值当成生产值会得出错误的故障检测时限。

**为什么 formula 里必须有 `ProcessStartID`：** `InstanceID` 只能标识"这是哪个部署槽位"，无法区分同一槽位里换过的进程。容器重建后 `InstanceID` 不变，若 owner 名不含 `ProcessStartID`，新进程会被误认为仍持有旧 lease / 旧 consumer 位置 / 旧 delivery claim（不变量 I9）。

### 1.3 HTTP 面

| 路径 | 语义 |
|---|---|
| `/callbacks/wecom` | 渠道回调入口（存在 WeCom endpoint 时注册） |
| `/livez` | 恒 200。**只表示进程没退出** |
| `/readyz` | `db.PingContext` 与 `redis.Ping` 与 malware 探针任一失败 → 503。这才表示"能安全处理业务" |
| `/statusz` | 返回 `{instance_id, process_start_id}`。**不返回** tenant、会话、任务或 Secret；barrier 状态通过 point-scoped test endpoint 查询。 |
| `/test/failover/{p1|p2|p3|p4}/{arm,status,release}` | 仅启用 `TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED=true` 时注册；所有操作要求 `X-TRPC-Local-Token`，未鉴权为 403。标准 E2E arm node-a 后强杀它，不向 survivor release。 |

### 1.4 编排顺序

```text
wecom-ha-bootstrap（webui-local-bootstrap，一次性初始化共享 tenant/config fixture）
   └─ depends_on postgres/redis/qdrant healthy
wecom-ha-node-a / wecom-ha-node-b（wecom-local，restart:"no"）
   └─ depends_on wecom-ha-bootstrap(service_completed_successfully), clamav, otel-collector
wecom-ha-entry（wecom-ha-entry，restart:"no"）
   └─ TRPC_WECOM_HA_ENTRY_BACKENDS=http://wecom-ha-node-a:8080,http://wecom-ha-node-b:8080
      TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL=1s
```

两个 Bot 共用同一 public origin，仅以 `route_key=local-wecom` 与 `route_key=local-wecom-secondary` 区分。外部 HTTPS tunnel 只暴露入口端口（默认 58087）；节点观测端口（58088/58089）**不得**登记为企业微信回调地址。

**此阶段的 durable truth：** 无（纯装配）。此刻被杀：进程不提供服务，入口 readiness 探测会摘除它，无数据损失。

---

## 二、可信入口：候选（candidate）一次性验签

外部请求**不能**自行指定 tenant。实现上分为四个阶段，全部围绕一个**一次性候选令牌**：

```text
ResolveCandidate      按 route key 找 binding（不暴露 tenant 字段）
  → IssueCandidate    state='issued'，写入 candidate_token_digest + expires_at
AcquireVerifier       原子取得验证权 state='verifier_acquired'，解析该 binding 范围内的验签密钥
  → verifierHandle.Verify   在 binding 凭据范围内验签；成功后 state='verified' + receipt
PromoteVerified       提升为 VerifiedBinding（携带 tenant_id / agent_app_id / channel_binding_id / 凭据引用）
```

### 2.1 `ResolveCandidate`

- 入参校验：`hint.Channel`、`hint.RouteKeyDigest`、`hint.IngressAttemptID` 缺一即 `ErrInvariantViolation`。
- `Store.ResolveBindingRoute(channel, routeKeyDigest)` 返回 `BindingRoute`（受信控制面数据），公开侧只得到不透明的 route 信息。
- `route.Enabled == false` → `ErrVersionMismatch`（禁用路由不得进入验证）。
- 生成随机候选令牌（默认 32 字节 → `base64.RawURLEncoding`），TTL 默认 30s；写 `CandidateRecord{State: 'issued'}`。
- **关键：** 此阶段**不返回** tenant / agent / 凭据，因此路由泄露不构成租户泄露。

### 2.2 `AcquireVerifier`

- `Store.AcquireCandidate(digest(token), bindingVersion, now)` 原子取得验证权。
- 交叉核验七项（channel、route_key_digest、purpose、binding_version、issued_at、expires_at 到微秒），任一不符 → `BurnCandidate` + `ErrVersionMismatch`。**候选只能用一次**。
- 通过 `Secrets.Resolve(Scope{TenantID, Subject: channelBindingID, Purpose: channel_verify, ResourceID, ResourceVersion}, route.SecretRef)` 解析验签密钥，并校验 `secret.Version == route.SecretRef.Version` 且非空；失败同样 `BurnCandidate`。

### 2.3 `verifierHandle.Verify`

- 单次使用：加锁 → 取出密钥副本 → 调用 `ProtocolVerifier` → **立即把副本与原密钥全部清零并置 nil**，同时 `closed = true`。后续调用返回 `ErrVersionConflict`。
- 验签失败或 `ProtocolIdentityDigest` 为空 → `BurnCandidate` 返回错误。
- 成功 → 生成 receipt token，`MarkCandidateVerified` 置 `state='verified'`，returns `(VerifiedCallback, VerificationReceipt)`。
- `Close()` 在未完成验证时 `BurnCandidate`——保证不留下孤儿候选。

### 2.4 `PromoteVerified`

- 校验 receipt 的 `CandidateToken`、`Purpose`、`ProtocolIdentityDigest` 非空、`ReceiptToken` 非空、`VerifiedAt` 非零。
- `Store.PromoteCandidate(...)` 置 `state='promoted'`；再校验 `route.Enabled` 与 `BindingVersion` 一致。
- 返回 `VerifiedBinding{TenantID, AgentAppID, ChannelBindingID, Channel, ExternalAccountID, TenantVersion, BindingVersion, IdentitySecretRef, SessionSecretRef}`。

**候选状态机：**

```text
issued ──AcquireVerifier──▶ verifier_acquired ──Verify 成功──▶ verified ──PromoteVerified──▶ promoted
   │                              │                              │
   └────── BurnCandidate ─────────┴──────────────────────────────┴──────▶ burned
        （过期 / 校验不符 / 密钥解析失败 / 验签失败 / 未完成即 Close）
```

**durable truth：** `channel_ingress_candidate` 行（含 `candidate_token_digest`、`receipt_token_digest`、`protocol_identity_digest`、`state`、`expires_at`）。
**此刻被杀：** 候选停留在 `issued` 或 `verifier_acquired`，由过期清理回收；平台重传会产生新的候选。**不会**产生跨租户路由，因为 tenant 只在 `promoted` 之后才被带出。

---

## 三、Inbox：把"收到"变成 durable 事实

```text
claim_inbox(tenant, channel, external_account_id, external_message_id, request_id,
            agent_app_id, session_id, payload_ref, payload_digest, key_version, initial_state)
  → INSERT ... ON CONFLICT (tenant_id, channel, external_account_id, external_message_id) DO NOTHING
  → SELECT * ... WHERE <同键> FOR UPDATE
  → 若 payload_digest / payload_ref / agent_app_id / session_id 不一致 → RAISE 'inbox idempotency collision' (23505)
  → RETURN NEXT v_row
```

语义要点：

- 唯一键是**四元组** `(tenant_id, channel, external_account_id, external_message_id)`：tenant 参与主键，因此两个租户即使收到相同 provider 消息 ID 也不会互相覆盖（不变量 I1 + I2）。
- `ON CONFLICT DO NOTHING` 后 `FOR UPDATE` 读回，使并发重传收敛到同一行。
- 属主/摘要不一致 → 23505 冲突。这防止"同名消息 ID 但内容不同"被误认为幂等重复。
- `payload_digest` 必须是 64 位小写 hex（`^[0-9a-f]{64}$`）；`initial_state` 只能是 `preprocess_pending` 或 `dispatch_pending`。
- 只有当 `claim_inbox` + 必要 dispatch 信息在同一事务提交后，才向企业微信确认接收。

**durable truth：** `inbox` 行。
**此刻被杀：** 事务未提交 → 平台重传，重新走一遍；事务已提交 → 重传命中唯一键收敛，不会产生第二条任务。

---

## 四、Dispatch：分配 input_seq 与写 Outbox

`prepare_dispatch(...)` 在一个事务内完成（详见 `REFERENCE-IMPLEMENTATION.md` §3.2）：

1. 锁 `tenant`，锁 `inbox`（按 `request_id`）。
2. 若 `inbox.state='dispatch_ready'`：按八元组（`agent_app_id, session_id, payload_ref, agent_app_version, agent_app_revision, agent_content_digest, config_version, policy_version`）反查 `execution_record`；查不到 → 23505（幂等冲突）；查到 → 直接返回既有 `input_seq` + `accepted=true`。**重复调用不会重复分配序号。**
3. 若 `inbox.state='terminal'`：返回 `accepted=false` + `terminal_reason`。
4. 状态/作用域不匹配（含 `prepared_payload` 链不成立）→ 42501。
5. tenant 非 active → `inbox` 置 `terminal` + 写 `dispatch-denied` 的 reply / audit outbox（`ON CONFLICT DO NOTHING`）→ `accepted=false`。
6. tenant 版本或 `active_config_version` 不匹配 → 40001。
7. `agent_app` 必须 active、version/revision 匹配、revision 必须 `published` 且 `content_digest` 匹配 → 否则 40001；`config_snapshot` 的 `policy_version` 必须匹配 → 否则 40001。
8. `INSERT INTO session_head(...) ON CONFLICT DO NOTHING`；`SELECT last_allocated_input_seq+1 ... FOR UPDATE`；更新 `last_allocated_input_seq`。
9. 插入 `execution_record`（`outcome` 默认 `queued`）。
10. `inbox` → `dispatch_ready` + `input_seq`。
11. 写 dispatch outbox（`idempotency_key = 'dispatch:<request_id>'`）。

**为什么 input_seq 分配必须与 outbox 同事务：** 若先分配序号、再写 outbox，中间崩溃会留下"已占号但无任务"的空洞，导致 `next_input_seq` 永久卡住（后续输入全部 `ErrInputNotReady`）。

**durable truth：** `session_head.last_allocated_input_seq`、`execution_record` 行、`inbox.state='dispatch_ready'`、`outbox` 的 dispatch 行。
**此刻被杀：** 事务回滚 → 重传重新分配；事务提交后由 `dispatch-relay` 继续发布。

---

## 五、Worker：从 Stream 到 terminal commit

### 5.1 消费与回收（`Consumer.Run`）

- 代码级兜底默认（仅在调用方未设置时生效）：`LeaseTTL<=0` → 5s、`RenewInterval<=0` → `LeaseTTL/3`、`RetryWait<=0` → 10ms、`ReclaimInterval<=0` → 1s、`ReclaimLimit<=0` → 100、`DrainTimeout<=0` → 30s。**`webui-local` 实际装配为 `LeaseTTL=30s`、`RenewInterval=10s`、`RetryWait=250ms`、`ReclaimInterval=5s`（见 §1.2 表）。**
- 每秒 `Broker.Reclaim(consumerID, limit)` 拉取超时未确认的 entry，逐条走 `process`。
- `process` 对 `ErrCommitConflict` / `ErrVersionConflict` 按 `RetryWait` 重试（这些是**可重试的竞争**，不是失败）。
- Consume 回调中执行失败**故意返回 nil**：不让 worker 退出，使空闲 delivery 保持在 pending、可被自己或他人 reclaim。
- Drain 语义：收到 drain 信号后 `cancelConsume`（停止接收新工作），`DrainTimeout` 后 `cancelWork`。

### 5.2 `handle` 的 13 步（关键顺序）

```text
1  SessionKey{TenantID, AgentAppID, SessionID} ← delivery.Envelope
2  Sessions.ReadLastFence(key)              ← 持久化 last fence
3  Leases.EnsureFenceAtLeast(key, persisted) ← 校准 Redis 计数
4  acquire(): 循环 Acquire，ErrVersionConflict 时按 RetryWait 重试
5  defer Release(lease)（1s 超时 context）
6  启动 取消监听 goroutine + 续租 goroutine（ticker=RenewInterval）
     续租失败 → markLeaseLost()（sync.Once：close(leaseLost) + cancelExecution()）
7  beforeCommit 闭包：先查 leaseLost，再 Renew 一次；失败 → ErrLeaseLost
8  Executor.ExecuteWithLease(executionCtx, envelope, lease.Fence, beforeCommit)
9  若 ErrInputNotReady → Parker.ParkInput：
     ParkInputReady → 重试；ParkedInput / ParkInputTerminal → 视为完成；ParkInputBlocked → 上报 ErrInputBlocked
10 停止监听与续租；若取消被请求 → CancelWithLease(beforeCommit)
11 再次检查 leaseLost → ErrLeaseLost
12 executeErr != nil → 直接 return（不 ACK！）
13 否则 Broker.Ack(ctx, delivery)
```

**第 3 步为什么必要：** Redis 的 fence 计数是易失的（重启/主从切换可能回退）。若不先把它校准到数据库的 `last_fence`，新 lease 可能拿到一个**小于**已持久化 fence 的值，于是合法接管者反而会被数据库拒绝。校准后新 lease 的 fence 必然 > 数据库值。

**第 12 步为什么不 ACK：** 未 ACK 的 entry 会留在 pending，可被存活节点 reclaim。反过来，若先 ACK 再提交，进程在 ACK 后崩溃会导致该消息既不会重投、也无结果——永久丢失（不变量 I5）。

### 5.3 执行与提交（`ExecuteWithLease`）

- 读取本租户已提交历史（`session_head` + `session_event`，按 `session_seq` 顺序）构造模型上下文。
- 执行模型/工具；渲染出站内容；写入 `result_payload` 得到 `result_ref`。
- 调用 `beforeCommit`（续租检查）后执行 `commit_turn`。
- 提交内容与语义见 §六。

**durable truth：** `result_payload` 行、`session_event` / `session_head` / `session_commit` / `outbox` 行（同一事务）。
**此刻被杀：**
- 提交前 → 无 terminal commit，entry 保持 pending，由存活节点 reclaim 后**重跑**（模型调用次数允许 >1）。
- 提交后、ACK 前 → entry 仍 pending，存活节点 reclaim 后发现已终态，`commit_turn` 返回 `already_terminal=true` → 直接 ACK，**不重跑模型**。

---

## 六、会话存储：OpenForRun / LoadSession / CommitTurn

### 6.1 `OpenForRun`

```sql
SELECT h.version, h.last_fence, h.last_session_seq, h.next_input_seq, h.state_json
FROM session_head h
JOIN execution_record e
  ON e.tenant_id = h.tenant_id AND e.agent_app_id = h.agent_app_id AND e.session_id = h.session_id
WHERE h.tenant_id = $1 AND h.agent_app_id = $2 AND h.session_id = $3
  AND e.request_id = $4 AND e.input_seq = $5
```

判定顺序（顺序重要）：

| 条件 | 返回 |
|---|---|
| `input_seq < next_input_seq` | `ErrAlreadyTerminal`（该输入已终态） |
| `input_seq > next_input_seq` | `ErrInputNotReady`（前序未完成，需 park） |
| `fence < last_fence` | `ErrStaleFence`（旧 owner 迟到） |
| 其他 | 返回 head，可执行 |

注意 JOIN `execution_record`：**必须能证明这次执行的 request/input_seq 属于该会话**，不能只凭 `session_id` 就开跑。

### 6.2 `ReadLastFence` / `LoadSession`

- `ReadLastFence`：`SELECT last_fence FROM session_head WHERE tenant/app/session`。用于 Worker 的第 2 步。
- `LoadSession`：读 head（version/last_fence/last_session_seq/next_input_seq/state_json）+ 按 `session_seq` 升序读 `session_event.event_payload`。**只读已提交事件**，因此半生成内容不会污染下一轮上下文。

### 6.3 SQLSTATE → 错误映射（必须实现，否则上层的重试/降级语义会错）

| SQLSTATE | 含义 | 映射到 |
|---|---|---|
| `40001` 且消息含 `stale fence` | 迟到写被拒 | `ErrStaleFence` |
| `40001` 其他 | 版本冲突（乐观锁） | `ErrVersionConflict` |
| `23505` | 唯一键冲突 | `ErrCommitConflict` |
| `XX001` | 不变量缺失（终态丢失） | `ErrInvariantViolation` |
| `55000` | 前序未就绪 | `ErrInputNotReady` |
| `P0902` | 取消被请求 | `ErrCancelRequested` |
| `42501` | 作用域/租户不匹配 | `ErrTenantScope` |
| `no rows` | 不存在 | `ErrNotFound` |

`ErrVersionConflict` / `ErrCommitConflict` 由上层当作**可重试竞争**处理（`Consumer.process` 按 `RetryWait` 重试），而 `ErrInvariantViolation` 必须当作故障向上抛——区分错会导致"数据不一致被当成竞争而无限重试"。

---

## 七、原子提交：`commit_turn`

`CommitTurn` 先做**请求级校验**（`ValidateCommit`）并计算**提交摘要**（`CommitDigest`），然后调用数据库函数 `commit_turn`（17 个入参）。函数内的判定顺序：

```text
1  锁 session_head；不存在 → P0002
2  锁 execution_record（按 request_id）；scope 不一致 → 42501
3  按 commit_id 查 session_commit：
      已存在 + digest 不同 → 23505（commit id 冲突）
      已存在 + digest 相同 → 直接返回既有结果，already_terminal=false（幂等重放）
4  input_seq < next_input_seq：必须存在对应终态 session_commit，否则 XX001；
      返回该终态 + already_terminal=true
5  input_seq > next_input_seq → 55000
6  expected_version <> head.version → 40001
7  p_fence < head.last_fence → RAISE 'stale fence' (40001)   ← 迟到写被拒
8  version+1；last_session_seq += jsonb_array_length(events)；逐条 INSERT session_event
9  UPDATE session_head：version / last_fence=GREATEST(last_fence, p_fence) /
      last_session_seq / next_input_seq += (终态 ? 1 : 0) / state_json || state_delta
10 （可选）写 session_summary
11 INSERT session_commit（含 fence、session_version、reply_cursor、result_ref）
12 UPDATE execution_record SET outcome, result_ref, version=version+1
13 逐条 INSERT outbox ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING
      outbox_id = format('%s:%s', kind, idempotency_key)
```

**四个字段共同界定"一次有效提交"：** `request_id`（哪次输入）+ `input_seq`（第几轮）+ `expected_version`（会话乐观锁）+ `fence`（所有权栅栏）。缺任何一个都会留下漏洞：

| 缺少 | 会发生的错误 |
|---|---|
| `request_id` | 同一轮被不同请求重复提交 |
| `input_seq` | 轮次错位，历史顺序错乱 |
| `expected_version` | 并发提交互相覆盖（lost update） |
| `fence` | 网络隔离后的旧 owner 覆盖新结果 |

`reply_cursor`（形如 `<request_id>:1`）与 `StableReplyID({TenantID, RequestID, InputSeq, Stage:"terminal", Ordinal:0})` 共同保证：**同一 tenant/request/轮次的待发回复标识稳定不变**，relay 的至少一次发布不会产生第二条逻辑回复。

终态集合（原文）：`('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout')`。

**durable truth：** 上述同一事务内的所有行。
**此刻被杀：** 事务整体回滚或整体提交；不存在"结果有了但待发回复丢了"的中间态（不变量 I4）。这正是 P3 窗口可恢复的根基。

---

## 八、Relay：把 Outbox 变成可重放工作

| Relay | 输入 | 输出 | 特性 |
|---|---|---|---|
| `dispatch-relay` | `outbox` 中 `kind='dispatch'` 的行 | Work Stream 的执行任务 | `Outbox` + `Tasks` + `Broker`；owner 含实例身份 |
| `reply-relay` | `outbox` 中 `kind='reply'` 的行 | Reply Stream 的回复事件 | `ClaimTTL=30s`、`ClaimRenewInterval=10s`、`PollInterval=100ms`；按 claim 续租防止双发 |
| `wakeup-relay` / `wakeup` | `kind='wakeup'` 行 | wakeup 队列 | 用于停放输入的唤醒 |

**为什么用 Outbox 而不是直接发 Stream：** 发布是外部副作用，不能放进 `commit_turn` 事务里；但发布失败也不能让已提交结果消失。Outbox 把两者解耦：事务只写"有待发事件"这一事实，relay 负责至少一次发布，幂等键（`outbox_id` / `idempotency_key` / 唯一约束 `outbox_tenant_id_kind_idempotency_key_key`）保证重复发布收敛。

**durable truth：** `outbox` 行（`state` 从 `pending` → `claimed`/`retry_wait` → `published`，`attempt` 计数）。
**此刻被杀：** claim 超时后由存活节点重发；已 `published` 的行不会二次发布。

### 8.1 Outbox 发布生命周期（领取 → 发布 → 重试，逐字语义）

relay 对每条待发事件走四步，全部以**条件更新**收口：

**① 领取（claim）** —— 一次 `SELECT ... FOR UPDATE SKIP LOCKED` + 一次 `UPDATE`：

```sql
SELECT ... FROM outbox o
WHERE kind = $1
  AND ((state IN ('pending','retry_wait') AND next_attempt_at <= now())
       OR (state = 'claimed' AND claim_until < now()))
ORDER BY next_attempt_at, created_at
FOR UPDATE SKIP LOCKED LIMIT $2;

UPDATE outbox o SET state='claimed', claim_owner=$3, claim_until=$4,
       version = o.version + 1, attempt = o.attempt + 1
...
```

| 细节 | 作用 |
|---|---|
| `FOR UPDATE SKIP LOCKED` | 两个 relay 并发扫描**同一 kind** 时不互相阻塞，且每条只被一个 owner 领走 |
| `state IN ('pending','retry_wait') AND next_attempt_at<=now()` | 只领"该发且到期"的 |
| `state='claimed' AND claim_until < now()` | **超期 claim 可被重新领取** —— 这正是 relay 进程崩溃后的接管机制（对应 `attempt` 再 +1） |
| `kind = $1` | 每个 relay 只处理自己负责的 kind（dispatch / reply / wakeup / audit…），互不抢占 |

**② 发布（publish）** —— 先把事件投到 Stream/队列，再回写：

```sql
UPDATE outbox SET state='published', published_at=now(),
       claim_owner=NULL, claim_until=NULL, version = version + 1
WHERE tenant_id=$1 AND outbox_id=$2 AND version=$3 AND state='claimed'
```

乐观锁（`version`）+ 状态条件双约束：若不是自己那条 claim 或已被他人改动 → 返回 `ErrVersionConflict`，**不覆盖**。

**③ 续租（renew claim）** —— 发布耗时长时延长占用：

```sql
UPDATE outbox SET claim_until=$5, version = version + 1
WHERE tenant_id=$1 AND outbox_id=$2 AND version=$3 AND state='claimed' AND claim_owner=$4
```

**④ 失败重试（retry）** —— 交还 claim 并安排下次时间：

```sql
UPDATE outbox SET state='retry_wait', next_attempt_at=$4,
       claim_owner=NULL, claim_until=NULL, version = version + 1
WHERE tenant_id=$1 AND outbox_id=$2 AND version=$3 AND state='claimed'
```

**状态机（本子系统实际可达路径）：**

```text
pending ──claim──▶ claimed ──MarkPublished──▶ published   （终态，不再重发）
                      │  ▲
                      │  └────────── claim（next_attempt_at 到期后重新领取）
                      ▼
                  retry_wait ──▶（到期）──▶ claimed ──▶ published
     claimed（claim_until 超期）──▶ 可被任一 relay 重新 claim（attempt 再 +1）
```

**关于 `dead_letter` 的诚实说明：** 该状态存在于 `outbox.state` 的 CHECK 约束中，也被指标与告警使用（如审计积压指标按 `state` 分组输出、审计保留策略会识别它），但**通用 relay 的发布循环只写 `published` 或 `retry_wait`，不会把 `dispatch`/`reply`/`wakeup` 事件写成 `dead_letter`**。因此：

- 运维**不应**在 `dispatch`/`reply` 上等待或依赖 `dead_letter` 出现；这些事件的"重试耗尽"表现为长期停留在 `retry_wait`（`attempt` 持续增长），应由积压告警捕获并人工处置。
- 若为 `outbox` 建立按 `state` 分组的积压面板，应把 `pending`/`claimed`/`retry_wait` 三类作为主信号，`dead_letter` 视作其他 kind（如审计）可能出现的状态。

**为什么整条链路只用条件更新、不用锁：** 因为 relay 与 worker、delivery 一样是**可替换执行者**。它崩溃后不需要任何"解锁"动作——`claim_until` 到期即可被别人领取。若改成"先加锁再发布"，就必须处理"锁持有者死亡后谁来释放"的问题，反而引入了新的故障模式。

> 相关实现（可选核对）：`trpcservice/storage/messaging/postgres/store.go`（claim / MarkPublished / RenewClaim / MarkRetryWait / 超期 claim 查询）、`trpcservice/relay/base.go`（relay 主循环调用顺序）、`trpcservice/storage/messaging/messaging.go`（`OutboxState` 常量与 `OutboxStore` 接口）。

---

## 九、Delivery：从 Result 到企业微信最终回复

```text
Reply Stream → Delivery.Deliver(event)
  ResolveReplyContent(tenant, request, content_ref)
     → result.ResultRef 必须 == event.ContentRef，否则 ErrVersionMismatch
  resolveAdapter: ResolveVersionedAdapter(tenant, binding, config_version)
     → adapter 非 nil 且 adapter.ID() == event.Target.Channel，否则 ErrTenantScope
  contentType 归一化；文本按 adapter 的 MaxTextBytes() 分片（不切断 UTF-8 rune）
  for each segment: deliverSegment(...)
```

`deliverSegment` 顺序：

1. `Ledger.ClaimDelivery(key, plan, Claim{Owner, TTL=30s})`。
2. 未领到：`sent`/`failed` → 返回 nil（终态，不再动作）；`ambiguous` → `reconcile`；`pending`/`sending`/`retry_wait` → `DeferredError{NotBefore}`。
3. Delivery 本身没有 P3 barrier；P3 位于 Reply Relay 的 `PublishReply` 前。
4. `deliverWithClaimRenewal`：后台按 `claimTTL/3` 续租 claim；续租失败 → 取消调用 context。
5. 错误分类：
   - `AmbiguousDeliveryError` → `state='ambiguous'`，`last_error_class='response_lost'`，`FinishDelivery`；
   - `PermanentDeliveryError` → `finishFailed(reconcile=false)`；
   - `RetryableDeliveryError` → `finishRetry(请求的 delay)`；其他 → `finishRetry(0)`；
   - `Delivered=false` → `finishRetry(ErrBackendUnavailable)`；
   - `ProviderMessageID == ""` → `state='ambiguous'`，`last_error_class='missing_provider_message_id'`。
6. P4 barrier 位于本函数 `ClaimDelivery` 成功后、调用 provider 前。
7. `state='sent'` + `provider_message_id`，`FinishDelivery`。
8. 仅在投递终态（`sent` 或 `failed`）持久化后，才 ACK account-queue entry。

`reconcile` 的分支：`ReconciliationDelivered` → `sent`；`ReconciliationNotDelivered` → `retry_wait` + `last_error_class='reconciled_not_delivered'`；`ReconciliationUnknown` → 继续 defer；`reconcile_attempt` 达上限 → `failed / reconcile_exhausted`。

**为什么 claim 必须在调用下游之前：** 若先调用再 claim，两个节点可能同时调用同一片段；claim 是"谁有资格发送"的租约。`client_request_id`（由 `(tenant_id, delivery_key, segment_no)` 稳定推导）是交给下游的去重键——这是 P4 能否收敛的唯一依据。

**durable truth：** `delivery_ledger` 行（`state`、`version`、`attempt`、`reconcile_attempt`、`claim_owner`、`claim_until`、`provider_message_id`、`client_request_id`）。
**此刻被杀：** `sending` 且 claim 未到期 → 等待 `claim_until` 过后被置为 `ambiguous/owner_lost` 并重新领取；已 `sent` → 只允许 ACK，不允许重发（不变量 I6）。

---

## 十、故障时的实际调用链

```text
N1 SIGKILL
  → N1 停止续租 / 停止消费 / 停止发送
  → Redis entry 超过 reclaim 条件，或 delivery_ledger claim_until 到期
  → N2 Reclaim entry / EnsureFenceAtLeast 校准 / Acquire 更高 fence / ClaimDelivery
  → PostgreSQL 拒绝 N1 旧 fence（stale fence, 40001），接受 N2 新 terminal commit
  → N2 发送或继续发送唯一 reply segment；sent 后只 ACK
```

---

## 十一、把一次请求串起来：每一步的 durable truth 与"此刻被杀会怎样"

| # | 步骤 | durable truth | 此刻被杀 |
|---|---|---|---|
| 1 | 入口按 `/readyz` 选择后端 | 无 | 入口换后端重试；不改变业务状态 |
| 2 | `ResolveCandidate` → `IssueCandidate` | `channel_ingress_candidate.state='issued'` | 候选过期被清理；平台重传产生新候选 |
| 3 | `AcquireVerifier` → `Verify` → `MarkCandidateVerified` | candidate `state='verified'` + receipt | 候选未能 promoted，重传重走；**不会**误推 tenant |
| 4 | `PromoteVerified` | candidate `state='promoted'` | 同上 |
| 5 | `claim_inbox` | `inbox` 行（四元组唯一键） | 未提交则重传；已提交则收敛 |
| 6 | `prepare_dispatch` | `session_head.last_allocated_input_seq` + `execution_record` + `inbox='dispatch_ready'` + dispatch outbox | 整体回滚重来；或由 dispatch-relay 继续 |
| 7 | dispatch-relay 发布 | `outbox` dispatch 行（`published`） | claim 超时后重发；已发布不重发 |
| 8 | Worker 取 lease（含 fence 校准） | Redis lease key + fence key | lease 到期，另一节点重新获取（更大 fence） |
| 9 | 读历史 → 执行模型 → 写 `result_payload` | `result_payload` 行（未终态） | entry 保持 pending；存活节点重跑（模型次数可 >1） |
| 10 | `commit_turn` | `session_event` + `session_head` + `session_commit` + reply outbox（同一事务） | 整体回滚或整体提交；无中间态 |
| 11 | Worker `Broker.Ack` | Redis entry 移除 | 未 ACK 则 pending 可 reclaim；已终态会被识别为 `already_terminal` |
| 12 | reply-relay → Reply Stream | `outbox` reply 行（`published`） | claim 超时后重发 |
| 13 | `ClaimDelivery` | `delivery_ledger` `state='sending'` + owner/until | claim 到期 → `ambiguous/owner_lost` → 重新领取 |
| 14 | 调用下游 → 得 receipt | 尚未持久化 | **P4 窗口**：进 `ambiguous`，靠 `client_request_id` 对账 |
| 15 | `FinishDelivery(sent)` | `delivery_ledger.state='sent'` + `provider_message_id` | 已 sent 只 ACK，不重发 |
| 16 | ACK reply event | Redis reply entry 移除 | 未 ACK 则重新投递，但 `sent` 使其无副作用 |

> 关键判据：**没有 durable truth 的步骤可安全重试；已有 durable truth 的步骤只能继续后半段，不能从头乱跑。**

---

## 十二、代码审阅重点

- 🔴 所有 repository 查询是否**同时**带 tenant 与 binding/session predicate（防串租户，I1/I2）。
- 🔴 是否存在 ACK 早于 durable terminal commit 或 ledger `sent` 的路径（I5）。
- 🔴 `commit_turn` 的 `p_fence < last_fence` 拒绝是否真正在 **DB 层**执行，而非仅应用层乐观判断。
- 🔴 `delivery_ledger` 的条件更新是否保留 `state='sending' AND version=? AND claim_owner=? AND client_request_id=?` 四重条件（削弱任一即可能双发，I6）。
- 🟠 所有 background goroutine 是否随 role context 退出；renewal 是否有明确 owner。
- 🟠 `process_start_id` 是否写入 lease、delivery claim、日志与 metrics label（可观测性；否则故障无法归因）。
- 🟠 是否存在绕过候选（candidate）一次性验签、直接用请求体 tenant 建上下文的旁路（I1）。
- 🟠 WeCom 响应不确定时是否进入显式 `ambiguous`，而不是直接标 `sent` 或盲目重发（I7）。
- 🟡 分片逻辑是否保证不切断 UTF-8 rune；非法 UTF-8 是否整体透传而非静默改写内容。
- 🟡 SQLSTATE 映射是否完整；把 `ErrInvariantViolation` 当竞争重试会导致无限重试掩盖数据不一致。

---

## 十三、相关文档

- 机制与因果链：`FULL-GUIDE.md`
- 架构 / 数据模型 / 状态机：`design.md`
- 关键代码摘录与逐段解释：`CODE-APPENDIX.md`
- 从零复现材料（DDL / Lua / 接口 / 算法 / 配置 / 启动顺序 / 验证清单）：`REFERENCE-IMPLEMENTATION.md`
- 测试与验收：`testing-and-acceptance.md`
- 发布与运维：`release-and-operations.md`
