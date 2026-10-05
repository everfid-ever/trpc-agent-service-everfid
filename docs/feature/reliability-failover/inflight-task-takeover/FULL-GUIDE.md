# 在途任务接管与幂等投递：完整实现说明

> 本文档自包含。读完后即使没有仓库，也能理解本能力的设计、不变量与验收边界。
> 等价实现的权威材料（DDL / Redis 契约 / Go 接口 / 四挂点算法 / Ledger CAS SQL / 配置全表）见 [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)。

---

## 1. 本包与「故障后新消息可用」的区别

新消息能被存活节点处理，只能说明故障后的新链路可用。用户真正关心的往往是：**在故障前已经发送、系统已经收到、但还没看到最终回复的那条原消息怎么办。** 本包验证的正是这条原消息能否由存活节点自动完成，用户不重发，租户和会话不改变。

两者的本质差异：

- 「新消息可用」只要求消费链路活着；
- 「在途任务接管」要求**已经落库的半成品状态**（inbox / execution / session_commit / delivery_ledger）能被另一个节点继续推进到终态。

---

## 2. 一条消息的五个持久化事实

```text
原始输入已收件(F1) → 执行任务已分发(F2) → terminal result 已提交(F3)
       → 待发回复已领取(F4) → 下游接受已确认(F5)
```

每个事实都由不同状态承载，不能用一个「处理完成」布尔值合并。故障点不同，正确恢复策略也不同：尚未执行的任务要继续分发；已提交结果只能继续投递；已被下游接受但本地未确认时必须处理不确定性。

| 事实 | 权威表 / 状态 | 故障后动作 |
|---|---|---|
| F1 原始输入已收件 | `inbox.state` ∈ {`preprocess_pending`,`dispatch_pending`} | 继续预处理 / dispatch 重放 |
| F2 执行任务已分发 | `execution_record.outcome` ∈ {`queued`,`running`,`pending`} + `preprocess_job.state='ready'` | 存活 worker reclaim 或 Outbox 重放 |
| F3 terminal result 已提交 | `session_commit`(终态) + `session_head.next_input_seq+1` + `execution_record.outcome` 终态 | 只继续投递已保存结果，绝不重跑模型 |
| F4 待发回复已领取 | `delivery_ledger.state` ∈ {`sending`,`pending`,`retry_wait`} | 存活 delivery 重新 claim 或续处理 |
| F5 下游接受已确认 | `delivery_ledger.state='sent'` + `provider_message_id` | 只 ACK，不可重发 |

### 2.1 可重试性判定：什么算"自动续处理"，什么算"安全重试"，什么**不可重试**

需求中的"自动续处理或安全重试"是两种不同动作，必须分开定义，否则实现者会错误地对已提交结果重跑、或对不确定投递盲目重发。

**判定原则（一句话）：** **看"上一次已经 durable 到哪里"。没有 durable 记录的步骤可以安全重试；已有 durable 记录的步骤只能从该事实之后续处理，绝不能从头重来；已被下游受理但本地未确认的，既不能重试也不能判失败，只能对账。**

| 故障发生在 | 上次 durable 到 | 恢复类别 | 允许的动作 | **禁止**的动作 | 为什么 |
|---|---|---|---|---|---|
| 收件事务提交前 | 无（F0） | **安全重试** | 平台重传 / 重新 `claim_inbox` | — | 没有 durable 记录，重放无副作用 |
| 收件后、dispatch 前 | F1（`inbox`） | **续处理** | 从 `preprocess_job.state='ready'` 重新 dispatch | 让用户重发；手工重新入队 | 输入已 durable，重放 dispatch 由 `prepare_dispatch` 幂等收敛 |
| dispatch 后、执行开始前 | F2（`execution_record`） | **续处理** | 存活 worker reclaim 同一 stream entry | 重新分配 `input_seq` | 序号已分配，重复分配会产生空洞或任务翻倍 |
| 模型/工具执行中 | F2（`running`）+ lease | **安全重试** | 存活节点取更高 fence 后**重跑模型/工具** | 旧 owner 继续提交 | `commit_turn` 尚未发生，无业务效果；重跑只影响"调用次数"这一过程指标 |
| └ 其中：**工具函数体执行中** | F2 + 会话转写中的 tool_call 消息 | **安全重试（工具状态不可恢复）** | 以同一输入重开一轮，由模型重新决策 | 声称"断点续跑"；假设工具只执行一次 | 工具无执行级记录，详见 §4.5 |
| 外部写工具已调用、结果未确认 | — | 🔴 **不可重试** | 按业务幂等键查询/对账后决定 | 盲目重试 | 副作用可能已生效；必须靠业务幂等键与资源 ID 对账 |
| 结果已提交、回复未发出 | F3（`session_commit` 终态） | **续处理** | 只发送 `result_payload` 中**已保存**的结果 | 重跑模型 | 结果已是权威事实，重跑会得到另一份答案并可能重复触发写工具 |
| 回复已领取、未调用下游 | F4（`delivery_ledger='sending'` 且 claim 未过期） | **续处理** | 等 claim 到期后由存活节点重新 claim | 抢先发第二条 | claim 是"谁有资格发送"的租约 |
| 回复调用了下游、响应丢失 | 未知（是否 F5 不明） | 🔴 **不可重试、不可判失败** | 置 `ambiguous` + 按 `client_request_id` 对账 | 盲目重发；直接把状态改成 `sent`/`failed` | 用户可能已收到；两种误判都会造成重复或丢失 |
| 下游已受理、本地未持久化 `sent` | 部分 F5（有 provider message id） | **续处理 + 对账** | 用已获得的 receipt 收敛为 `sent` | 重发；丢弃 receipt | receipt 存在即可避免重发 |

**与"三类计数"的关系（避免误判验收）：**

| 恢复类别 | 模型调用次数 | 任务尝试次数 | 最终回复次数 |
|---|---|---|---|
| 安全重试 | 可能 >1 | +1（受上限约束） | 不变（仍为 1） |
| 续处理 | **不变**（不重跑模型） | 可能 +1 | 不变 |
| 不可重试（对账） | 不变 | 对账可能增加 `reconcile_attempt` | 不变，或如实标为不确定 |

**实现上如何落实这条判定：** 恢复类别不是"策略配置"，而是由**已 durable 的事实**天然决定——因为 ACK 只发生在 terminal commit 之后（F3），未 ACK 的 entry 必然处于"无 durable 提交"状态，因而可安全重跑；而 F3 之后的路径不再经过执行器，天然无法重跑模型。换言之：**顺序即策略。**

---

## 3. P1：可靠接收后、执行前

### 3.1 前提
原始输入和预处理任务已经持久化（`inbox` F1、`preprocess_job` 已 durable），执行任务尚未发出（F2 尚未成为 `execution_record`）。

### 3.2 风险
- 接收节点强杀后，原消息若无 durable record，则永久丢失；
- 任务被领取但不可回收（没有 reclaim 机制），则永远 pending；
- 或运维被迫让用户重发——直接违背「用户无需重发」。

### 3.3 方案
在单事务内完成 `claim_inbox → 写 inbound_payload → 写 preprocess_job`，回调才返回成功。平台重传命中 `inbox` 唯一键时读回既有行（见 `claim_inbox`）。preprocess worker 周期性 `ClaimReadyForDispatch` 扫描 `state='ready' AND dispatched_at IS NULL` 的 job 继续 dispatch；`dispatch` 前由 P1 barrier 暂停。节点死亡后，另一节点的扫描自然接力，**不需要任何人工重入队**。

### 3.4 必须接受的事实
「接收成功」不是「客户端已发送」，也不是「HTTP handler 开始执行」，而是**存在可被另一节点发现的 durable record**。`preprocess_job` 的 `prepared_payload_ref` 与 `channel_binding_id` 必须随 job 持久化，否则转入异步链路后 tenant 归属与 payload 会丢失（`TrustedSource: "channel_binding:" + job.ChannelBindingID` 保留可信来源）。

---

## 4. P2：模型或工具执行中

### 4.1 前提
worker 已领取原任务、已发起模型/工具调用（F2 `running`），但 terminal result 尚未提交（F3 未发生）。

### 4.2 风险
- 故障后原队列消息若被 ACK，任务消失，无人续处理；
- lease 丢失后两个 owner 同时执行，产生双写；
- 旧 owner 迟到提交覆盖新 owner 的合法结果；
- 工具（有外部副作用）被重复调用产生重复业务效果。

### 4.3 方案
worker 消费或 reclaim 后按 session key 获取 lease。lease value 含 owner、`leaseID`、递增 `fence` 与 TTL。提交前 `commit_turn` 用 `p_fence < v_head.last_fence` 校验：旧 fence 被拒（`stale fence` → `ErrVersionConflict`），新 fence 才能推进 `session_head.last_fence`。原队列消息**不 ACK**，由存活节点 `Broker.Reclaim` 接管；reclaim 后读取持久化 `last_fence`、`EnsureFenceAtLeast` 校准，再 `Acquire` 更高 fence。

`beforeCommit` 闭包在每次提交前先检查 `leaseLost` 再 `Renew`；续租失败即 `markLeaseLost()` 取消执行 context。

### 4.4 必须接受的事实
**模型调用次数允许大于 1。** 模型内部生成状态通常不可跨节点恢复，因此新 owner 从头重跑模型是预期行为。正确性目标是：
1. 只有一个有效 terminal commit（由 fence + `session_commit` 终态唯一索引保证）；
2. 外部写操作按业务幂等键只有一个业务效果；
3. 客户端最终回复按协议能力去重，或明确「至少一次」。

**绝不能只做 X 的因果链：**
- 不能只 ACK：ACK 必须在 terminal commit 之后（不变量 I5），否则故障会丢失在途任务；
- 不能只靠进程内存锁：进程被 SIGKILL 后内存锁消失，必须靠 Redis lease TTL + 数据库 fence 双保险；
- 不能只靠数据库 fence：Redis 重启可能丢失 fence 计数，所以 `EnsureFenceAtLeast` 会用持久化 `last_fence` 校准，避免 fence 倒退。

### 4.5 子窗口：工具调用执行中崩溃（本窗口最容易被误解的一问）

P2 是一个区间，跨度可以很长。演练脚本能停在**"模型与工具都跑完、结果还没提交"**这个确定性点上；但真实故障最常见的形态是**停在工具函数体内部**——工具正在调用外部系统、或正在跑几分钟的长任务。这两个位置虽然同属 P2，durable 状态并不相同，必须分开讲。

问题形式是：**Bot 正执行一个工具调用，所在节点突然挂掉。这次工具执行的相关记录保住了吗？切到另一个节点后，Bot 会接着执行没跑完的工具调用吗？**

先给结论，再给依据：

> **结论：** 对绝大多数工具（`allow` 策略），durable 的只有**"模型请求调用某工具及其参数"这一事实**；工具**执行到哪一步、产生了什么中间结果、是否已生效，都没有任何记录**。接管后**不会从工具断点续跑**，而是以同一条输入**重开一轮**：模型重新推理，并在历史里看到上次那次调用"没有结果"的痕迹，由模型决定是否再次调用工具。因此工具可能被执行两次（at-least-once），平台只保证**一次有效提交 + 一次可见回复**。
> **唯一的例外**是需要人工确认的 `ask` 工具：它有工具级 durable 账本（`tool_attempt` + 加密结果 `tool_result_payload`），接管后**不重跑**，而是采用已存结果，或诚实地以"效果不确定"终结。

#### 4.5.1 崩溃瞬间"留下了什么"

以长程任务里最常见的 `allow` 工具为例（工具函数体正在执行）：

| 事实 | 存放位置 | 崩溃瞬间是否已 durable |
|---|---|---|
| 用户输入原文（含附件引用） | `inbound_payload`（AES-256-GCM，AAD 绑定 tenant/request/payloadRef/digest） | ✅ 在 |
| 该输入已被受理、已分配 `input_seq` | `inbox`（F1）+ `execution_record`（`outcome='running'`） | ✅ 在 |
| 会话归属（租户 + Bot Binding + 会话 ID） | `execution_record` / `session_head` 主键、SDK `app_name = tenantID + "/" + agentAppID` 编码 | ✅ 在 |
| **模型请求调用哪个工具、参数是什么** | 官方会话后端的 `session_events` 中那条 assistant 消息（含 `tool_calls`） | ✅ 在（在工具开始执行**之前**就已写入，是全窗口唯一的"工具相关"记录） |
| 该工具执行到哪一步、已产生什么中间结果 | —— | ❌ **没有任何表/字段记录** |
| 工具返回值 | `session_events` 中的 tool 消息 | ❌ 未写入（工具尚未返回） |
| 渲染后的最终回复、终态提交 | `result_payload` / `session_commit` / reply `outbox` | ❌ 未发生 |
| 本次尝试已调用模型几次、工具几次 | 进程内 `execution_budget` 计数器 | ❌ 随进程消失（这三类计数本就只在内存中作为单轮熔断用） |
| 成本预留 | `budget_reservation` | ⚠️ 部分（预留行在；实际用量按治理审核路径处理） |

一句话概括：**"模型想调用什么工具"是可恢复的，"工具执行到了哪里"不可恢复。** 平台从未声称做过工具级检查点，任何依赖该假定的设计都会在接管后失效。

#### 4.5.2 接管节点实际做了什么

1. 原 stream entry **未 ACK**（ACK 只在 terminal commit 之后，不变量 I5），Redis 租约随 TTL 到期；
2. 存活节点每 `ReclaimInterval`（装配值 5s）执行 `Broker.Reclaim`，领回该 entry；
3. `ReadLastFence` → `EnsureFenceAtLeast` 校准 → `Acquire` 取得**更高的 fence**；
4. `ExecuteWithLease` → `OpenForRun`：要求 `input_seq == next_input_seq` 且 `fence >= last_fence`，否则直接返回非终态错误（不重复分配序号）；
5. 从 `inbound_payload` 解码出**同一条**用户消息——输入恢复不依赖旧进程的任何内存；
6. 重新 `run.Run(...)`：**模型重新推理**（模型调用次数 +1，这是预期行为）；
7. 构造模型请求前，框架对历史做一次清洗：上一次尝试留下的"有 tool_call、无 tool_result"的 assistant 消息会被**降级**为一条 user 角色的文本（带 `[orphan_tool_call]` 标记，内含工具名 / 调用 ID / 参数原文），从而既不破坏上游 provider 的消息合法性要求，又让模型能"看到"上次那次调用没有拿到结果；
8. 模型据此二选一：直接作答，或**再次调用该工具**——若再次调用，工具是**从函数入口重新执行**，不是断点续传；
9. 结束时 `beforeCommit` 续租 → `commit_turn` 以本节点 fence 提交：`fence < last_fence` 一律 `stale fence` 拒绝，因此**旧节点若复活也无法覆盖新结果**。

#### 4.5.3 "继续执行"的准确语义（不要过度承诺）

| 常见说法 | 是否成立 | 准确表述 |
|---|---|---|
| "工具会从断点继续跑" | ❌ | 工具执行状态无记录，不存在断点 |
| "系统必然重跑该工具" | ❌ | 是**重开一轮 + 模型重新决策**；模型看到上次调用无结果后，可能直接作答而不重调 |
| "工具至少被执行一次" | ✅ | 若模型重调则外部副作用可能发生两次（at-least-once） |
| "用户最终只看到一条回复" | ✅ | 由 fenced `commit_turn` + 每 `input_seq` 唯一终态保证；管道末端仍受 P4 的下游去重边界约束 |
| "上下文不会丢" | ✅ | 会话历史来自官方会话后端的持久化转写，与进程内存无关 |

#### 4.5.4 例外：`ask` 工具是唯一有工具级账本的路径

需人工确认的危险工具走另一条路：授权被"消费"（`confirmation_grant`：`available` → `consumed`）、写入 `tool_attempt(state='effect_unknown')`、执行工具、把结果加密存入 `tool_result_payload`，再 `FinishToolAttempt(succeeded|failed)`。恢复语义因此完全不同：

| 崩溃位置 | durable 状态 | 接管后的动作 |
|---|---|---|
| 授权消费**之前** | `confirmation.state='approved'` | 可以安全执行（尚未产生任何外部效果） |
| 授权消费**之后**、结果落库前 | `confirmation.state='consumed'` + `tool_attempt.state='effect_unknown'` | **不执行**：读得到 `tool_result_payload` 就采用该结果（并把 `effect_unknown` 升级为 `succeeded`）；读不到则以"工具效果不确定"终结并告警 |

也就是说：**`ask` 工具宁可承认"不知道有没有生效"，也不盲目重跑**。这正是 `effect_unknown` 这个状态存在的意义，也是该账本只给危险工具用的原因（每个工具调用多两次以上数据库往返）。

#### 4.5.5 诚实边界（长程任务的真实代价）

- **恢复粒度是"一轮"，不是"一步"**：一轮里做了 20 次工具调用的长任务，接管后这 20 步都要重新推进，且**已经发生的外部副作用不会回滚**；
- 因此**有副作用的工具必须自带业务幂等键**（`allow` 路径不保证单次执行）——平台的承诺是"一次有效提交 + 一次可见回复"，不是"工具只执行一次"；
- 若工具既不可重入又不可查询，本窗口只能"重跑"，风险由业务承担（见 §18 P4 能力矩阵的同构判断）；
- 若业务确实需要工具级可恢复，只有两条路：① 把它升级为 `ask` 工具，获得 `tool_attempt` 账本与加密结果存储；② 用 Graph Agent 并开启 checkpoint（`CheckpointSaver` 落在 Redis、带 TTL、按 tenant/版本/命名空间隔离）。
- **但第 ② 条在当前实现里也救不了自动接管，原因有两层（都已核对）：**
  1. **平台只在人工确认续跑时传 resume 坐标**（`lineage_id` / `checkpoint_id` / `namespace` + `graph.ResumeCommand`），自动接管路径不传；
  2. **即使传了也无处可查**：Graph 执行器的 `lineage_id` 默认取 `invocation.InvocationID`，而 invocation 每次 `Run` 都是新生成的 UUID，因此重跑落在**全新 lineage** 上，检查点存储里查不到上一次的检查点（checkpoint 键按 lineage 摘要隔离）。
  结论：**"开了 checkpoint 就能自动续跑"在当前实现中不成立**。要获得跨接管续跑，必须由调用方提供稳定的 lineage 与 resume 坐标——这是明确的未接线项，不是隐藏能力。

---

## 5. P3：结果已提交、回复未发送

### 5.1 前提
terminal outcome、会话推进、`reply outbox` 已经原子保存（F3），Delivery 还没有调用下游（F4 未 sending）。

### 5.2 风险
- 系统误以为「任务没完成」而重跑模型，产生第二个 terminal commit（被唯一索引拒绝，但浪费算力且可能扰乱副作用）；
- reply 丢失，用户收不到最终回复；
- 或重复提交导致多份 outbox。

### 5.3 方案
`commit_turn` 将 terminal outcome、result ref、session sequence、fence、reply outbox **在一个事务**写入（不变量 I4）。Relay 至少一次发布 outbox；Delivery 通过 `ClaimDelivery` 领取后调用 adapter 发送**已保存**的 `result_payload`，绝不重新执行模型。

### 5.4 必须接受的事实
**为什么 P3 不能重跑模型？** 因为 F3 已经是一个合法终态；模型重跑会违反「一次 terminal commit」，且若模型带副作用（工具调用）则二次执行破坏业务幂等。P3 故障只恢复 relay/delivery，不触发 `Execute`。这正是 Transactional Outbox 的价值：发送属于外部副作用，不能放进结果提交事务；但外部发布失败也不能让已提交结果消失——outbox 重放负责补发。

---

## 6. P4：下游已接受、本地未确认

### 6.1 前提
发送节点已经取得下游接受证据（如 `provider_message_id`），但在将 ledger 写为 `sent`（F5）前死亡。

### 6.2 风险
- 新节点凭「客户端暂时没显示」或「上次 HTTP 超时」误判未发送，导致重复发送；
- 或误判已发送，导致用户永久收不到回复；
- 强行承诺 exactly-once 而下游根本不支持去重。

### 6.3 方案
Ledger 主路径 `pending → sending → sent`，并允许 `retry_wait` 与 `ambiguous`。

- 新 owner 在 `claim_until<=now()` 且 `state='sending'` 时，先把旧 claim 置 `ambiguous`/`owner_lost`，再重新 claim（见 `ClaimDelivery` 的 owner-lost 清理）；
- 若下游支持 `client_request_id` 去重或查询，系统可 `reconcile` 后收敛；
- 若不支持，必须保留 `ambiguous`、告警，或按明确「至少一次」策略处理。

`client_request_id` 由 `StableDeliveryRequestID(key)` 从 `(tenant_id, delivery_key, segment_no)` 稳定推导，是交给下游的去重键；owner/version/state 三重条件保证不会双发。

### 6.4 必须接受的事实（诚实边界）
**只有下游能按 `client_request_id` 去重或查询时，P4 才可收敛到一次可见回复；否则只能「至少一次 + `ambiguous` 告警」。** 不能把 P3 的成功替代 P4，也不能承诺协议无法提供的 exactly-once。这是设计上的诚实，不是缺陷。

---

## 7. 测试暂停点如何保证命中准确

测试 barrier 默认关闭，仅在显式配置 P1–P4、目标实例和可选 tenant 时启用。它只在已经达到对应 durable boundary 时阻塞该任务，并通过 `/statusz` 的 `inflight_test_barrier` 报告已命中。测试人员随后强杀**真正命中的 owner**；受害进程消失后，存活节点才被释放继续正常接管。

barrier 不是重试器：它**不会**创建任务、删除租约、变更 Ledger 或补发回复。若测试需要手工改状态才成功，结论应是「未验证自动接管」。

四个挂点的精确位置（见 [implementation-walkthrough.md](./implementation-walkthrough.md)）：

| Point | 文件 | 位置 |
|---|---|---|
| P1 | `preprocess/worker.go` `dispatch()` | `Dispatcher.Dispatch` 之前（durable job 已存在、尚未成为 execution） |
| P2 | `worker/runner.go` | 模型/工具与 `renderOutbound` 之后、`encodeResultRef`/`PutResult`/`beforeCommit`/`CommitTurn` 之前 |
| P3 | `channels/delivery/service.go` `deliverSegment()` | `ClaimDelivery` 成功后、`deliverWithClaimRenewal` 之前 |
| P4 | 同上 | adapter 返回且 `ProviderMessageID != ""` 后、置 `sent` 与 `FinishDelivery` 之前 |

---

## 8. 幂等不是一个键，而是一组边界

| 层级 | 稳定识别 | 防止的重复 |
|---|---|---|
| Inbox | tenant + binding + provider 消息身份 + payload digest | 平台 callback 重传 |
| Session commit | request + input sequence + version + fence | 双节点或旧节点重复提交 |
| Reply event | tenant + stable delivery key + segment | relay 至少一次发布 |
| Delivery Ledger | tenant + delivery key + segment + claim version | 两个发送者同时发同一回复 |
| 外部写工具 | tenant + 原请求 + 业务操作 | 模型重试导致重复创建资源 |

**为什么一个幂等键覆盖不了所有阶段？** 因为「消息已收到」「结果已生成」「回复已被下游接受」是三个不同事实，各自落在不同存储、不同生命周期、不同故障窗口。Inbox 去重解决平台重传；fence 解决并发提交；Ledger claim 解决双发；业务幂等键解决工具副作用。缺任一层都会在对应窗口失守。

---

## 9. 标准演练顺序

1. 建立双节点健康基线，并为两个租户保留不同原会话代号（Tenant A 为靶机，Tenant B 为并发对照）。
2. 选择一个 P 点，只让 Tenant A 原消息停在该点；Tenant B 全程作为对照。
3. 记录原消息、任务、owner、tenant/session、暂停命中和持久化证据（inbox / preprocess_job / execution_record / session_commit / delivery_ledger 行）。
4. 强杀实际责任节点，记录 `t0`，确认它持续停止（`State.Running == false`）。
5. 仅释放存活节点的测试暂停（POST `/test/failover/barrier/release`）；**不改队列、不删租约、不手改任务或发送记录。**
6. 在预设 `T_TASK/T_DELIVER` 内检查同一原消息的完成和最终可见回复。
7. 观察 `W_DUPLICATE`，记录重试次数、迟到回复、重复 terminal、永久 pending 和副作用幂等结果。
8. 重建基线，再对 Tenant B 和其它 P 点独立重复。

> 三个计数必须分别核对（模型调用次数 / 任务尝试次数 `execution_record.park_attempt`·`delivery_ledger.attempt` / 最终回复次数）。详见 [testing-and-acceptance.md](./testing-and-acceptance.md)。

---

## 10. 验收与边界

### 10.1 通过要求
原消息在故障前有可靠接收证据（F1），受害节点停止期间由系统完成（F3→F5），**不需要用户重发**，不串 tenant/session，不出现重复最终回复。工具写操作还要按业务幂等键核对只有一次预期效果。

### 10.2 不通过（反例）
- 超时未处理；
- 原消息丢失；
- 串 tenant / session；
- 旧节点迟到提交成功（stale fence 未拦截）；
- 重复最终回复（同一 segment 出现两次 `sent`）；
- 必须手工重新入队后才完成；
- 把 Provider 无法去重的 P4 写成恰好一次。

### 10.3 边界重申
- 不承诺跨主机 / 整机断电 / 共享 PG·Redis 故障下的可用性（单一故障域）。
- 不承诺模型调用次数恒为 1；P2 接管可重跑模型。
- 不承诺下游无法去重/查询时 P4 的绝对 exactly-once。
- 不把 Docker 自动重启当作存活节点接管。
- 不把健康/连接/队列恢复单独当作完整业务 RTO。

关键代码见 [CODE-APPENDIX.md](./CODE-APPENDIX.md)，精确步骤与证据字段见 [testing-and-acceptance.md](./testing-and-acceptance.md)。

---

## 11. 消息状态转换总表

| 阶段 | 表 / 字段 | 进入状态 | 退出触发 | 故障后由谁恢复 |
|---|---|---|---|---|
| 收件 | `inbox.state` | `preprocess_pending` → `dispatch_pending` | preprocess 校验 | preprocess worker |
| dispatch | `inbox.state` / `execution_record` | `dispatch_ready` / `queued` | `prepare_dispatch` 单事务 | worker Reclaim |
| 执行 | `execution_record.outcome` | `running` → `pending`(park) | 模型/工具；或 `park_execution` | 新 fence owner |
| 提交 | `session_commit.outcome` / `session_head.next_input_seq` | 终态 | `commit_turn` 单事务 | 仅重投（不重跑模型） |
| 投递 claim | `delivery_ledger.state` | `sending` | `ClaimDelivery` | 存活 delivery |
| 投递终态 | `delivery_ledger.state` | `sent` / `ambiguous` / `failed` | `FinishDelivery`/`ReconcileDelivery` | 对账 / 告警 |

---

## 12. 不变量追踪矩阵（验收逐条对标）

| # | 不变量 | 实现边界 | 证据 |
|---|---|---|---|
| I1 | tenant 只能由验签后的 binding 推出 | `channel_public_route`→`channel_binding_locator`→verify→Tenant Context | `/statusz` owner、ingress 记录、delivery adapter 解析 |
| I2 | 每个 provider 输入只有一个 durable 事实 | `inbox` 唯一键 + `claim_inbox` + `prepare_dispatch` | inbox state/input_seq、唯一键冲突记录 |
| I3 | 同一会话同一时刻只有一个有效提交者 | Redis lease + 递增 fence + `commit_turn` 的 `p_fence < last_fence` 拒绝 | lease key、`session_head.last_fence`、stale fence 拒绝日志 |
| I4 | terminal 结果与待发回复原子提交 | `commit_turn` 单事务写 session_commit + session_head + outbox（`p_events` 生产为 null） | session_commit 行、outbox 行、reply_cursor |
| I5 | 未 ACK 前不得 ACK；ACK 必须在 terminal commit 之后 | `consumer.handle` 只在 `executeErr==nil` 时 `Broker.Ack` | Redis pending entries、reclaim 记录 |
| I6 | 两个发送者不能同时发同一回复片段 | `delivery_ledger` 条件更新（state/version/owner/client_request_id） | ledger 行状态轨迹、attempt 计数 |
| I7 | 下游可去重时 P4 可收敛，否则诚实 ambiguous | `client_request_id`（`StableDeliveryRequestID`）+ `reconcile` | ledger `ambiguous`/`reconciled_not_delivered` |
| I8 | 公开 callback 地址不随实例切换 | `wecom-ha-entry` readiness 探测 + 轮转转发 | 入口 `/statusz` backends healthy 集合 |
| I9 | 重新加入不复用旧 owner | `ProcessStartID`（每次启动新 8 字节 hex） | `/statusz.process_start_id` 前后对比 |
| I10 | 进程活着不等于可服务 | `/readyz` 覆盖 db + redis + malware 依赖 | 入口摘除记录、503 计数 |

---

## 13. 故障窗口数据边界对照（已落库 / 未落库）

| 窗口 | 已落库（存活节点可见） | 未落库（随进程死亡丢失） |
|---|---|---|
| P1 | `inbox`(F1) + `preprocess_job`(ready) | 尚未创建 `execution_record` |
| P2 | `execution_record`(running) + Redis lease + stream pending | 模型中间态、render 结果、`result_payload` |
| P2-a（模型已请求工具、工具执行中） | 上列全部 + 官方会话后端 `session_events` 中那条含 `tool_calls` 的 assistant 消息（**仅此一条工具相关记录**） | 工具执行进度、工具中间结果、工具返回值、本轮的模型/工具调用计数 |
| P2-b（`ask` 工具消费授权后、结果落库前） | 上列全部 + `confirmation_grant`(consumed) + `tool_attempt`(effect_unknown) | 工具结果密文（`tool_result_payload` 未写入时） |
| P3 | `session_commit`(终态) + `session_head` 推进 + reply outbox(pending) + `result_payload` | adapter 尚未调用 |
| P4 | `delivery_ledger`(sending 或 sent/ambiguous) + provider_message_id(部分) | 本地 `sent` 持久化 |

所有窗口都不依赖进程内存状态——这是本设计能接管的前提：只要「半成品」落在 PostgreSQL 或 Redis（带 TTL 自动释放），存活节点就能从最后一个 durable 状态接力。

---

## 14. 常见误区（为什么不能只做 X — 扩展）

- **只做接收去重（Inbox）→ 不够**：P2/P3/P4 仍需 lease/fence/ledger 三层保证；Inbox 去重只解决平台重传。
- **只做 lease 不加 fence 校验 → 不够**：Redis 重启或时钟问题会让旧 owner 翻案；`commit_turn` 的 `p_fence < last_fence` 是最终防线。
- **只做数据库 fence 不校准 Redis → 不够**：Redis 数据丢失后 fence 可能倒退，所以 `EnsureFenceAtLeast(key, persistedFence)` 用持久化 `last_fence` 校准。
- **P3 重跑模型 → 错误**：F3 已是终态，重跑违反「一次 terminal commit」，且工具副作用重复。
- **ACK 早于 terminal commit → 错误**：丢失在途任务（I5）；故障后无人续处理。
- **一个幂等键覆盖全阶段 → 错误**：「收到」「生成」「接受」是三个事实，落在不同存储/生命周期。
- **P4 承诺 exactly-once → 错误**：仅当下游支持 `client_request_id` 去重/查询才收敛，否则诚实 `ambiguous`。
- **把健康/连接/队列恢复单独当 RTO → 错误**：RTO 必须是「收到→最终可见」的真实端到端时间。

---

## 15. 演练证据采集清单（每次 P 点一套）

1. `run_id`、compose 项目名、节点端口。
2. tenant / channel_binding / session 标识。
3. `request_id`、`external_message_id`、`inbox.state`、`input_seq`。
4. `execution_record.outcome`、`park_attempt`、Redis lease 存在性。
5. `delivery_ledger.state`、`segment_no`、`attempt`、`provider_message_id`、`client_request_id`、`last_error_class`、`reconcile_attempt`。
6. barrier `point`/`hit`/`released`、`/statusz` JSON 快照。
7. victim 容器 `State.Running`、`t0`（SIGKILL 时刻）、survivor `/readyz`。
8. 三计数：模型调用次数（日志）、`park_attempt`、`delivery_ledger.attempt`、每 segment `sent` 行数。
9. 最终可见回复内容 + 去重/对账证据（P4）。

凭据与真实用户标识须脱敏。

> 本节是**汇总视角**；可执行的采集命令、按窗口的最小证据 SQL、判定表与结论模板见 [testing-and-acceptance.md](./testing-and-acceptance.md) §五–§十一。

---

## 16. 消息去重与碰撞处理（Provider 层边界）

平台 callback 重传是常态。`claim_inbox` 的语义是：先 `INSERT ... ON CONFLICT (tenant_id, channel, external_account_id, external_message_id) DO NOTHING`，再 `SELECT ... FOR UPDATE`，最后比较 `payload_digest` / `payload_ref` / `agent_app_id` / `session_id`：

- 属主与摘要一致 → 认作同一输入，返回既有行（I2）；
- 不一致 → `RAISE 'inbox idempotency collision'`（23505），拒绝。

这就是 Provider 层去重边界：同一物理消息不会变成两条 `execution_record`；不同消息若撞唯一键但内容不同，则被碰撞保护拦截，不会静默覆盖。

---

## 17. fence 校准细节（为什么 Redis 重启不会让旧 owner 翻案）

`EnsureFenceAtLeast(key, persistedFence)` 在 `acquire` 之前调用：

```go
persistedFence, _ := w.Sessions.ReadLastFence(ctx, sessionstore.SessionKey(key))
w.Leases.EnsureFenceAtLeast(ctx, key, persistedFence)
lease, _ := w.acquire(ctx, key)
```

逻辑链：
1. 从 PostgreSQL `session_head.last_fence` 读出持久化最大值；
2. `ensureFence` Lua 仅在 Redis 当前 fence < 持久化值时 `SET` 到持久化值；
3. `acquire` 的 `INCR` 在此基础上再 +1，保证新 owner 拿到的 fence 一定 ≥ 持久化值；
4. 即使 Redis 重启清零，新 owner 也会先被校准到 `last_fence` 再递增，旧 owner（若拿的是更早的小 fence）在 `commit_turn` 被 `p_fence < last_fence` 拒绝。

所以「Redis fence + PostgreSQL last_fence」是双保险，缺一不可。

---

## 18. P4 下游能力矩阵（诚实边界的判定表）

| 下游能力 | P4 可收敛到 | 系统动作 |
|---|---|---|
| 支持 `client_request_id` 去重（幂等接收） | 一次可见回复 | 直接去重，或 `ReconciliationDelivered` → `sent` |
| 支持按 `client_request_id` 查询发送状态 | 一次可见回复 | `ReconcileDelivery` 查到已接受 → `sent`；未接受 → `retry_wait` |
| 仅「发送即接受」，无查询/去重 | 至少一次 | 保留 `ambiguous` + `last_error_class='response_lost'` + 告警 |
| 返回但 `ProviderMessageID==""` | 至少一次 | 一律 `ambiguous`/`missing_provider_message_id`，绝不伪造 receipt |

**结论：** P4 能否 exactly-once 完全取决于下游能力，系统不臆造。文档与接口都只承诺「至少一次 + 诚实 ambiguous」，这是设计诚实，不是缺陷。

---

## 19. 验收通过 / 不通过的硬标准（一句话版）

- 通过 = 故障前有可靠接收证据 + 受害节点停止期间系统自动完成 + 用户不重发 + 不串 tenant/session + 最终回复只一次 + 工具副作用按业务幂等键只一次。
- 不通过 = 超时 / 丢失 / 串租户 / 旧 fence 迟到提交成功 / 重复最终回复 / 手工重入队后才完成 / 把不可去重的 P4 写成恰好一次。

以上任一不成立，即判定本能力未验证。

> 展开版判定表、最小证据闭环与反例回归构造见 [testing-and-acceptance.md](./testing-and-acceptance.md) §七–§八。

---

## 20. 术语对照表（避免歧义）

| 术语 | 本文含义 | 易混淆点 |
|---|---|---|
| durable fact（持久化事实） | 已落 PostgreSQL/Redis 且存活节点可见的状态 | ≠ 进程内存状态 |
| terminal commit | `session_commit` 出现终态行 + `session_head.next_input_seq+1` | ≠ 「HTTP 返回成功」 |
| fence | 单调递增执行权令牌，存 Redis + `session_head.last_fence` | ≠ 进程内锁 |
| lease | Redis 带 TTL 的执行权占用 | 过期自动释放，不依赖进程退出 |
| claim（Ledger） | `delivery_ledger` 的发送权占用，带 `claim_owner`/`claim_until` | 过期自动回收为 ambiguous |
| 幂等键 | 业务去重标识（outbox / client_request_id / delivery key+segment） | 一个键覆盖不了全阶段 |
| 至少一次 | 允许重复尝试，但可通过对账收敛 | ≠ exactly-once |
| ambiguous | 下游接受状态未知，需对账或告警 | 诚实边界，不是 bug |

## 21. 与「故障后新消息可用」的验收分界

| 维度 | 新消息可用 | 在途任务接管（本包） |
|---|---|---|
| 验证对象 | 故障后新链路能消费 | 故障前已收未完成的原消息能被自动续处理 |
| 关键证据 | 新消息得到回复 | 同 `request_id` 在受害节点停止期间完成 + 用户不重发 |
| 失败表现 | 链路整体不可用 | 原消息丢失 / 重复 / 串租户 / 旧 owner 翻案 |

本包不替代「新消息可用」的验收，二者独立但可组合演练。

---

## 22. 一句话收尾

在途任务接管的本质是：**把一条消息的五个持久化事实（F1–F5）分段落库，使任一节点在任意事实之间死亡时，存活节点都能从「最后一个已落库事实」接力，且靠 Inbox 唯一键、fence、Delivery Ledger 条件更新、业务幂等键四层边界保证不丢、不重、不串、不翻案。** 模型调用可重跑（≥1），任务尝试受控，最终回复必须恰好一次（在下游可去重的诚实边界内）。这就是本包全部设计的出发点与验收终点。
