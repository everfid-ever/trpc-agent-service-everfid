# 在途任务接管与幂等投递 — 全景导读

## 0. 一句话

当用户消息已被系统接收、但尚未产生最终回复时，执行或发送节点发生故障，存活节点应自动继续处理同一原始消息；用户不重发，系统不串租户、不丢会话、不重复产生最终回复或外部业务效果。

本文档给非实现者建立心智模型。完整独立说明见 [FULL-GUIDE.md](./FULL-GUIDE.md)，自足复现材料见 [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)。

---

## 0. 怎么读本文（非代码读者必看）

- 🟢 **§0–§2 与 §4 全文无代码**，可逐字读完，足以理解"要解决什么、五个阶段、四个故障窗口"。
- 🟡 **§3（P1–P4 业务故事）含少量内部函数名**（如 `Broker.Reclaim`、`EnsureFenceAtLeast`）。它们不是必须理解的代码——**只需读每段的第一句与"恢复："开头的那句**，其余可以跳过。
- 🔴 **可直接跳过**：本文中所有出现 `` `xxx_worker.yyy()` `` 形式的函数调用、`` `state='...'` `` 形式的状态串、以及任何 Go / SQL / Lua 代码块。

### 技术名 → 大白话对照表

> 读不懂名字时回到这张表。这些名字是**系统内部账本与动作的代号**，不是需要你掌握的代码。

| 文档里出现的名字 | 大白话 |
|---|---|
| `inbox` | **收件箱**：证明"这条消息系统确实收到了"的账本 |
| `preprocess_job` | **待办事项单**：消息在正式处理前要先做一步准备工作，这张单子记录它做到哪了 |
| `execution_record` | **任务执行记录**：这条消息正在被处理 / 处理到哪一步 |
| `session_commit` | **结案的记录**：这一轮已得出结论（终态），不再改变 |
| `session_head` | **会话进度卡**：记录这个会话已经处理到第几轮、最后一次有效接管权是谁的 |
| `outbox` | **待发清单**：已经产生、但还没送到下一环节的事情 |
| `delivery_ledger` | **投递台账**：这条回复发到哪一步了（未发 / 发送中 / 已发 / 不确定） |
| `claim_inbox` | 收件动作：把"收到"变成账本上的一条事实 |
| `prepare_dispatch` | 派工动作：为这条消息分配一个顺序号，并生成执行任务 |
| `commit_turn` | 结案动作：把结果、会话进度、待发回复**一次性**记入账本 |
| `lease` | 临时处理许可（带过期时间） |
| `fence` | 接管权的序号，越接管越大 |
| `reclaim` | 回收：把超时未确认的任务交给新执行者 |
| `Broker.Reclaim(...)` / `EnsureFenceAtLeast(...)` | 两个内部动作：分别是"回收超时任务"和"把接管序号校准到不小于账本值"。**不必记住** |
| `state='sending'` / `state='sent'` 等 | 台账上的状态标签："发送中" / "已确认发出" |

---

## 1. 五个持久化事实（术语表核心）

一条输入消息从接收到最终可见回复，依次产生五个**不同**的持久化事实。它们由不同状态承载，不能用一个「处理完成」布尔值合并——因为故障点不同，正确的恢复策略也不同。

| # | 持久化事实 | 权威表 / 状态 | 含义 | 故障后该做什么 |
|---|---|---|---|---|
| F1 | 原始输入已收件 | `inbox`：`state` ∈ {`preprocess_pending`,`dispatch_pending`} | 平台已收到且去重落库 | 继续预处理 / dispatch |
| F2 | 执行任务已分发 | `execution_record`：`outcome='queued'/'running'/'pending'`；`preprocess_job.state='ready'` | 输入已被转为可执行的执行任务 | 由存活 worker reclaim 或 Outbox 重放 |
| F3 | terminal result 已提交 | `session_commit`（终态）、`session_head.next_input_seq+1`、`execution_record.outcome` 终态 | 模型/工具结果已原子提交，reply outbox 已写入 | 只继续投递**已保存**的结果，绝不重跑模型 |
| F4 | 待发回复已领取 | `delivery_ledger`：`state='sending'/'pending'/'retry_wait'` | 回复片段已被某 owner claim 待发送 | 由存活 delivery 重新 claim 或续处理 |
| F5 | 下游接受已确认 | `delivery_ledger`：`state='sent'` + `provider_message_id` | 下游已接受且本地已持久化 | 只 ACK，不可重发 |

> **关键不变量（I2）：** 每个 provider 输入只有一个 durable 事实（`inbox` 唯一键 + `claim_inbox` + `prepare_dispatch` 收敛）。后续所有阶段都建立在 F1 这一条之上。

---

## 2. 四个必须分开验证的窗口（P1–P4）

| 窗口 | 已发生 | 尚未发生 | 风险 |
|---|---|---|---|
| **P1** | 输入已持久化 / 可靠入队（F1，且 `preprocess_job` 已 durable） | Executor 开始（F2 尚未成为 execution） | 消息被遗忘，或领取后不可回收，用户被迫重发 |
| **P2** | 模型或工具已开始（F2 `running`） | terminal result（F3） | lease 丢失、旧 owner 迟到提交、工具重复调用 |
| **P3** | Result / Outbox 已提交（F3） | Reply API 调用（F4 尚未 sending） | 系统重跑模型、回复丢失、重复提交 |
| **P4** | 下游已接受回复（拿到 provider message ID） | 本地 `sent` 确认（F5） | 用户可能收到重复，或系统误判丢失 |

为什么必须分开验证？因为四个窗口的**正确修复动作互不相同**：

- P1 修的是「入队可靠性 + dispatch 重放」；
- P2 修的是「lease/fence 让旧 owner 失效 + 可重跑模型但只一个 terminal commit」；
- P3 修的是「Outbox 重放 + 绝不重跑模型」；
- P4 修的是「Ledger claim 不双发 + 下游去重/对账 + 诚实 `ambiguous`」。

一个幂等键覆盖不了所有阶段：「消息已收到」「结果已生成」「回复已被下游接受」是三个不同事实。

---

## 3. P1–P4 业务故事

> 🟡 **非代码读者提示：** 本节含内部函数名。**每段读第一句 + "恢复："开头的一句即可**，其余细节可跳过；不影响理解。

### 3.1 P1：可靠接收后、执行前

Tenant A 的原消息进入 `inbox`（F1，`preprocess_pending` → `dispatch_pending`），`preprocess_job` 被标记为 `ready`。在 `preprocess_worker.dispatch()` 调用 `Dispatcher.Dispatch` **之前**，故障注入暂停点命中（P1 barrier），随后该节点被强杀。

恢复：存活节点的 `preprocess` worker 通过 `ClaimReadyForDispatch` 扫描到 `state='ready'` 且 `dispatched_at IS NULL` 的 job，继续 `Dispatch` → `MarkDispatched`。用户不重发；测试人员也**不得**手工重新入队。

### 3.2 P2：模型或工具执行中

Tenant A 原消息 durable 成功，Worker N1 领取并获取 session lease（含递增 fence）。在模型/工具调用完成、渲染出 outbound、**提交 terminal commit 之前**（P2 barrier 命中），N1 被强杀。租约停止续期，Redis stream entry 保持 pending。

恢复：N2 通过 `Broker.Reclaim` 拿到同一 entry，读取该会话持久化的 `last_fence`，`EnsureFenceAtLeast` 校准后 `Acquire` 更高 fence，重放相同 request；模型可再次调用（**模型调用次数 >1 是允许的**）。N2 以新 fence 成功提交唯一 terminal turn 与 Reply Outbox；N1 即使晚到，因 `commit_turn` 中 `p_fence < v_head.last_fence` 被拒（`stale fence` → `ErrVersionConflict`）。Tenant B 全程对照正常。

### 3.3 P3：结果已提交、回复未发送

terminal outcome、会话推进、reply outbox 已在 `commit_turn` 单事务中原子保存（F3），但 Delivery 还没调用下游（F4 未 sending）。在 `deliverSegment` 中 `ClaimDelivery` 成功、调用 adapter **之前**（P3 barrier 命中），节点被强杀。

恢复：存活 delivery 扫描未发布 reply outbox，重新发布并 `ClaimDelivery` 已保存结果。它**绝不重新执行模型**——`result_payload` 里的结果就是终态内容。P3 检验的是 Transactional Outbox 的价值：发送是外部副作用，不能放进提交事务；但外部发布失败也不能让已提交结果消失。

### 3.4 P4：下游已接受、本地未确认

发送节点已取得下游接受证据（provider message ID），但在把 `delivery_ledger` 写为 `sent`（F5）**之前**（P4 barrier 命中）死亡。新节点不能凭「客户端暂时没显示」或「上次 HTTP 超时」判断未发送。

Ledger 主路径 `pending → sending → sent`，并允许 `retry_wait` 与 `ambiguous`。如果下游支持 `client_request_id` 去重或查询，系统可 `reconcile` 后收敛；若不支持，必须保留 `ambiguous`、告警，或按明确「至少一次」策略处理。**不能把 P3 的成功替代 P4，也不能承诺协议无法提供的 exactly-once。**

---

## 4. 基本保证（量化）

- **至少一次处理：** 模型和无副作用工具可重试（模型调用次数允许 >1）。
- **一次 terminal commit：** 同一 session input sequence 只有一个有效 terminal result（由 `commit_turn` + `fence` + `session_commit_terminal_input_idx` 唯一索引保证）。
- **一次业务效果：** 外部写操作必须有独立业务幂等键和对账能力（如 `tenant_id + request_id + operation`）。
- **投递边界诚实：** P4 能否收敛到「一次客户端可见回复」，取决于 Provider 的去重/查询能力；不能去重时只能是「至少一次 + `ambiguous` 告警」。

---

## 5. 范围与不包含

**包含：**

- 消息已持久化未执行（P1）、模型/工具执行中（P2）、结果已提交未发送（P3）、下游已接受本地未确认（P4）四个关键故障窗口的接管与幂等投递。
- 任务租约（Redis lease）、执行权回收（fence）、Fencing Token、Inbox/Outbox 与 Delivery Ledger。
- 原始消息自动续处理或安全重试；区分模型调用次数、任务尝试次数与最终回复次数。
- 用户无需重发；每个输入只产生一次可见最终回复（在下游可去重的诚实边界内）。

**不包含 / 不承诺：**

- 跨主机、整机断电、Docker daemon 失效、共享磁盘、共享 PostgreSQL·Redis 故障下的可用性（同属单一故障域，见 README §2）。
- 模型调用次数恒为 1。
- 下游无法按 `client_request_id` 去重或查询时 P4 的绝对 exactly-once。
- 把 Docker 自动重启（`restart: "no"` 已禁用）当作存活节点接管。
- 健康恢复 / 连接恢复 / 队列恢复单独当作完整业务 RTO。

---

## 6. 下一步

- 设计评审 / 写实现前：读 [FULL-GUIDE.md](./FULL-GUIDE.md)。
- 理解结构与时序：读 [design.md](./design.md)。
- 写代码对照：读 [implementation-walkthrough.md](./implementation-walkthrough.md) 与 [CODE-APPENDIX.md](./CODE-APPENDIX.md)。
- 从零实现 / 复现验收：读 [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)。
- 做验收 / 写 CI：读 [testing-and-acceptance.md](./testing-and-acceptance.md)。
- 上线与值班：读 [release-and-operations.md](./release-and-operations.md)。

---

## 7. 最小复现命令速览

```bash
# P1：已持久化未执行
TRPC_INFLIGHT_TENANT_ID=<tenant-a-id> bash scripts/e2e/inflight-takeover.sh p1
# P2：模型/工具执行中
TRPC_INFLIGHT_TENANT_ID=<tenant-a-id> bash scripts/e2e/inflight-takeover.sh p2
# P3：结果已提交未发送
TRPC_INFLIGHT_TENANT_ID=<tenant-a-id> bash scripts/e2e/inflight-takeover.sh p3
# P4：下游已接受本地未确认
TRPC_INFLIGHT_TENANT_ID=<tenant-a-id> bash scripts/e2e/inflight-takeover.sh p4
```

脚本只负责：拉起隔离 compose 项目 → 等两节点 ready → 等你发唯一标记消息 → 命中 barrier → 强杀 victim → 释放存活节点 barrier。它**不伪造**用户输入，也不重投消息、不删租约。

## 8. 三个计数：必须量化区分（全包核心）

| 计数 | 允许 >1？ | 落库字段 | 含义 |
|---|---|---|---|
| **模型调用次数** | **允许**（P2 接管可重跑） | 不落库 | 一次 terminal commit 前模型可被多 owner 调用多次 |
| **任务尝试次数** | 受控 | `execution_record.park_attempt` / `delivery_ledger.attempt` | 输入序号 / 回复片段重试次数，有上限 |
| **最终回复次数** | **必须 = 1** | `delivery_ledger` 每 segment 一行，`state='sent'` 后不可重发 | 每个可见最终回复片段只一次 |

任何「重复 / 丢失」判断先落到这三个计数上：一次 terminal commit 可能来自多次模型调用；一次最终回复可能背后有多次 `attempt`；但最终回复一旦 `sent`，只许 ACK，不许重发。

## 9. 不变量速查（追踪矩阵）

| # | 不变量 |
|---|---|
| I1 | tenant 只能由验签后的 binding 推出 |
| I2 | 每个 provider 输入只有一个 durable 事实（inbox 唯一键） |
| I3 | 同一会话同一时刻只有一个有效提交者（Redis lease + fence + commit_turn 拒绝旧 fence） |
| I4 | terminal 结果与待发回复原子提交（commit_turn 单事务） |
| I5 | 未 ACK 前不得 ACK；ACK 必须在 terminal commit 之后 |
| I6 | 两个发送者不能同时发同一回复片段（delivery_ledger 条件更新） |
| I7 | 下游可去重时 P4 可收敛，否则诚实 ambiguous |
| I8 | 公开 callback 地址不随实例切换（wecom-ha-entry 探测 + 轮转转发） |
| I9 | 重新加入不复用旧 owner（ProcessStartID 每次启动新 8 字节 hex） |
| I10 | 进程活着不等于可服务（/readyz 覆盖 db + redis + malware） |
