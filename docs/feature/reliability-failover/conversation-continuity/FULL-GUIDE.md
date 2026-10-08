# 故障恢复后继续对话：完整实现说明

> 本文是**自包含**的能力说明，可直接照其实现等价系统。凡涉及表名、字段、函数、环境变量、Lua、Go 标识符，均与参考实现逐字一致；如需与原实现交叉核对，可在括号内标注的位置查看对应源文件。

---

## 1. 要解决的实际问题

平台同时服务两个企业微信 Bot。Bot A 绑定 Tenant A，Bot B 绑定 Tenant B。用户曾分别在两个原会话中输入不同代号；任一应用节点故障后，用户继续在同一聊天窗口询问代号，系统必须：

1. **身份不能串**：A 的回复只能含 Tenant A 的代号，B 只能含 Tenant B 的；服务端 `request` / `session` / `terminal result` / `delivery record` 必须指向同一 tenant。
2. **历史不能丢**：进程内存全失后仍能从持久化 `session` 回忆各自上下文。
3. **旧执行者不能覆盖新执行者**：隔离后的旧 N1 即便恢复网络，也不能用迟到写入覆盖 N2 已提交的结果。

仅靠模型 Prompt、进程内缓存、Docker 重启或“再发一条消息成功”都不能证明上述三点成立——它们都绕过了“服务端持久化链路”这一真正证据面。

---

## 2. 参与者与权威状态

| 参与者 | 职责 | 绝不能作为唯一事实的内容 |
|---|---|---|
| 企业微信 | 原始消息、签名、最终显示回复 | 服务端是否已持久接收、是否已提交业务结果 |
| 稳定入口 | 将 callback 交给健康节点 | tenant 路由、消息去重、会话状态 |
| N1 / N2 | 验签、执行、转发、投递 | 会话历史、未完成任务、发送完成状态 |
| PostgreSQL | binding、Inbox、session、result、outbox、ledger | 高吞吐消费调度 |
| Redis | 任务传输、pending reclaim、lease/fence | tenant 或 terminal result 的最终裁决 |

**分层原则：** 数据库保存业务事实；队列保存可重放工作；应用节点保存暂态计算。节点死亡后仍继续，正是因为权威事实在库里，不在进程里。

---

## 3. 从 Bot 到 tenant 的可信路径（为什么身份不串）

外部请求**不能**自行指定 tenant。系统先通过 callback 的 route key 找到候选 Bot binding，再在该 binding 的凭据范围内验签。验签成功后，binding 才决定 tenant、Agent、发送凭据、配置版本和能力集合：

```text
callback → channel_public_route(route_key_digest) → channel_binding_locator(opaque_binding_id)
        → channel_ingress_candidate 验签 → binding → Tenant Context(tenant_id, agent_app_id, config_version, secrets)
```

相关表（真实字段见 `REFERENCE-IMPLEMENTATION.md` §2）：

- `channel_public_route(channel, route_key_digest, opaque_binding_id, binding_version, enabled, ...)`：公开路由表，约束 `opaque_binding_id` 长度 16..256、`route_key_digest` 长度 16..256。
- `channel_binding_locator(opaque_binding_id, tenant_id, config_version, binding_id, identity_secret_ref/version, session_secret_ref/version)`。
- `channel_ingress_candidate(candidate_token_digest, opaque_binding_id, channel, route_key_digest, purpose='channel_verify', binding_version, state, ...)`，`state IN ('issued','verifier_acquired','verified','promoted','burned')`。
- `channel_binding(tenant_id, config_version, binding_id, channel, external_account_id, agent_app_id, secret_ref, secret_version, send_secret_ref, send_secret_version)`。

因此两个 Bot 即使使用同一模型上游，也不共享会话、工具、知识或发送身份。测试中模型“自称属于某租户”**没有证据价值**；只有 binding、tenant、session 与投递记录的一致关联才是证据（不变量 I1）。

---

## 4. 如何保存原会话（四元组持久化链路）

会话链路由四个稳定身份串起：

```text
tenant_id（验签后推出）
  + agent_app_id（Bot Binding 推出）
  + session_id（外部 chat 稳定标识）
  + external_message_id（单条入站消息）
```

- `inbox` 唯一键：`(tenant_id, channel, external_account_id, external_message_id)`（PK，行 5105）。每次入站消息先 `claim_inbox` 去重并持久化原始输入。
- 入站经 `prepare_dispatch` 分配 `input_seq`，写入 `session_head(tenant_id, agent_app_id, session_id)`（PK 三元组）与 `execution_record`。
- 会话只保存**已提交** turn；模型生成到一半、工具失败或取消的内容不进入下一轮上下文。

完成一轮时，系统在同一持久化提交（`commit_turn`）中完成（不变量 I4）：

1. 写 terminal result（`result_payload` + `session_commit.outcome`）；
2. 推进 `session_head` 的已提交历史和 `next_input_seq`；
3. 写 reply `outbox`（kind='reply'，idempotency_key = `reply:<request_id>`）；
4. 更新本会话最后有效 `last_fence`。

这样 N2 接管后从数据库读取的永远是稳定历史。若节点死在提交前，该轮没有进入历史；若死在提交后发送前，结果和待发回复仍存在，不需要重跑模型。

---

## 5. 为什么需要 lease 和 fence 两种机制（因果链）

**只做 Redis 锁行不行？不行。** Redis 锁（lease）解决“谁当前可执行”，但解决不了**网络隔离后的迟到写**：

```text
N1 fence=20，随后失联（未死，只是连不上协调服务）
N2 fence=21，提交成功，session_head.last_fence=21
N1 恢复网络，带 fence=20 提交
数据库拒绝 N1（p_fence=20 < last_fence=21）；N2 的结果保持不变
```

Lease 是带 TTL 的临时执行许可，防止 N1/N2 在同一时刻正常并发执行同一 session。故障节点停止续租后 lease 到期，N2 可领取新的许可。但 lease 不能解决“旧执行者迟到写”，于是每次许可都会得到**递增 fence**，数据库在 `commit_turn` 中以 `IF p_fence < v_head.last_fence THEN RAISE 'stale fence' USING ERRCODE='40001'` 拒绝更小 fence 的写入。

**为什么不能 ACK 后再写库？** 如果先 `Broker.Ack`、再写 `session_commit`，那么节点在 ACK 之后、写库之前崩溃，这条消息既不会重投（已 ACK）也不会有结果（未写库），永久丢失。正确顺序是：**terminal commit 成功 → 才 `Broker.Ack`**；`executeErr != nil` 时直接返回（不 ACK），保留 pending 供存活节点 reclaim（不变量 I5）。

**为什么容器重启不算接管？** 本包 `restart: "no"`，强杀后受害节点必须保持停止。若依赖 Docker 重启，验证的是“重启恢复”，且旧 ProcessStartID 可能复用，无法证明“另一节点接管”。每次启动新生成的 `ProcessStartID`（8 字节 hex）使重新加入不复用旧 owner（不变量 I9）。

---

## 6. 正常消息的完整时序

```text
用户 → Bot A → 稳定入口 → 健康节点
  → 可信 Ingress：route key → binding → 验签 → Tenant A Context
  → claim_inbox 去重并持久化原始输入（state=preprocess_pending/dispatch_pending）
  → prepare_dispatch 分配 input_seq，写 session_head + execution_record，inbox→dispatch_ready
  → Dispatch Outbox 产生执行任务
  → Worker 获取该会话 lease/fence（ReadLastFence → EnsureFenceAtLeast → Acquire）
  → 读取 Tenant A 的已提交历史（session_head + session_event）并执行模型
  → 原子提交 commit_turn：result + session turn + reply outbox（单事务）
  → Relay 发布 reply 事件
  → Delivery 领取台账 ClaimDelivery、调用企业微信、FinishDelivery 置 sent
```

任何一步失败都不能跳过前面的持久化边界。尤其 ACK 只能在 terminal commit 之后。

---

## 7. N1 强杀后会发生什么（接管时序）

```text
t0    N1 被 SIGKILL：停止续租、停止消费、停止发送；受害节点保持停止
t1    N2 reclaim goroutine（每秒 Broker.Reclaim）发现 pending entry / 入口摘除 N1
t2    入口 readiness 探测把 N1 摘除，新 callback 转给 N2（/statusz healthy 轮转）
t3    N2 读取相同 binding 与共享数据库，仍可验证 Bot A/B 并找到各自原会话
t4    若有未确认工作，N2 在 reclaim 周期后领取；若需处理同一 session，则取得更大 fence
t5    N2 按持久化历史继续下一轮；旧 N1 即使后续恢复也因 stale fence 失去提交权
t6    N2 发送最终回复并把 delivery_ledger 推进为 sent
```

> `t_connect`（连接/入口恢复）、`t_worker`（执行者可处理/回收）、`t_ready`（业务链路整体 ready）各自代表不同事实，不能把任一当作精确端到端 RTO（见 §10）。

这个过程**不允许**手工删除租约、手工重绑 Bot、手工重新入队或先重启受害节点，否则只能证明人工恢复，不是自动接管。

### 7.1 术语澄清：需求里的"连接接管"在本架构中具体指什么

需求表述为"通过**连接**与 Worker 自动接管"。这一条需要澄清，否则容易去找并不存在的机制：

- **本系统是 HTTP 回调（webhook）架构，不存在需要转移所有权的长连接。** WeCom / 飞书 / WebUI / 通用 httpcallback 四个渠道全部是"平台主动向登记 URL 发 HTTP 请求"（`trpcservice/channels/` 下无 WebSocket 或长连接消费者）。
- 因此"连接接管"在这套实现里等价于三件可核查的事：
  1. **公开回调地址不变** —— 企业微信登记的 URL 始终指向稳定入口，实例生死不改变它；
  2. **新回调能到达存活节点** —— 入口按 `/readyz` 摘除受害节点并转发给健康节点（对应上表 `t_connect`）；
  3. **Bot 身份本身是共享配置** —— 两个节点持有相同的版本化 binding，所以存活节点本来就能代表同一个 Bot A/B 完成验签与回复；不需要"把 Bot 连接交过去"。
- **推论：** 验收时不要寻找"连接所有权转移"的日志或状态；正确的证据是 ① 入口健康集合只剩存活节点；② 该节点仍在同一租户会话上取得更高 fence 并提交成功；③ 客户端在故障后仍能收到回复。三者合计即"连接接管成立"。

> 与之相对，**Worker 接管**是真实存在的状态转移：Redis pending entry 被 reclaim、lease 被重新取得、fence 递增（对应 `t_worker`）。

---

## 8. 什么叫“最终回复正确”（计数区分，务必写清）

必须区分三类计数，混为一谈会误判：

| 计数 | 含义 | 本子系统约束 |
|---|---|---|
| **模型调用次数** | P2 执行中调用 LLM 的次数 | 允许 >1（接管可重跑模型），不承诺恒为 1 |
| **任务尝试次数** | `execution_record.park_attempt` / `delivery_ledger.attempt` | 受 `park_execution` 的 max_attempts(≤64) 与 delivery MaxAttempts(8) 约束 |
| **最终回复次数** | `delivery_ledger` 每 segment 一行，state='sent' 后不可重发 | 每输入一份内容只对应一条客户端可见 final |

“最终回复正确”需同时满足：客户端只看到一条最终回复；回复内容只含所属 tenant 的代号；服务端 `request` / `session` / `terminal result` / `delivery record` 指向同一 tenant；没有另一租户的历史、工具结果或凭据痕迹。流式占位、进度消息与最终回复分别计数，不能用“正在处理”代替最终交付。

---

## 9. 投递为何必须 P3/P4 两段对账

`delivery_ledger` 状态机：`pending → sending → sent`，并含 `retry_wait` / `ambiguous` / `failed`。

- **P3 barrier**（`ReplyRelay` 已构造 reply event、`PublishReply` 前）：暂停点验证已提交结果的 relay publish 可由存活节点重放，不重跑模型。
- **P4 barrier**（`ClaimDelivery` 成功、调用 provider 前）：暂停点验证 delivery claim 接管后可安全调用 provider。provider 已接受但本地未知的独立窗口由 `client_request_id` 去重/对账收敛。

错误分类（见 `delivery/service.go`）：
- `AmbiguousDeliveryError` → state=`ambiguous`，`last_error_class='response_lost'`，进入 `reconcile`。
- `PermanentDeliveryError` → `finishFailed(reconcile=false)`。
- `RetryableDeliveryError` → `finishRetry(请求的 delay)`；其他 → `finishRetry(0)`。
- `Delivered=false` → `finishRetry(ErrBackendUnavailable)`。
- `ProviderMessageID==""` → `ambiguous`，`last_error_class='missing_provider_message_id'`。

发送 owner 崩溃时，Claim TTL（默认 30s）到期后由存活节点接管；`sent` 记录只能 ACK，不能重发（不变量 I6）。下游可去重时 P4 收敛，否则诚实 `ambiguous`（不变量 I7）。

---

## 10. 观测与时间口径（不能夸大精度）

| 时刻 | 代表的事实 | 不代表什么 |
|---|---|---|
| `t0` | 外部确认 N1 被终止 | 业务已恢复 |
| `t_connect` | 入口/连接所有者已恢复 | Worker 一定能提交 |
| `t_worker` | 执行者可处理/回收任务 | 用户已经看到回复 |
| `t_ready` | 所需业务链路均已 ready | 精确端到端 RTO |
| 收到→最终可见 | 一条请求的真实响应时间 | 故障期间未接收消息的恢复时间 |

需要在独立证据目录保存：节点状态、`owner`、`lease/fence`、关联 ID（`tenant_id, binding_id, session_id, request_id, stream_entry_id, fence, delivery_key`）和客户端结果。采样间隔 `DELTA_POLL` 必须随结论记录，**不能宣称比观测频率更精确的结论**。

---

## 11. 设计边界与明确不承诺

| 边界 | 说明 |
|---|---|
| 单节点故障 | 本包验证单应用节点 SIGKILL 后两 Bot 继续对话 |
| 不替代 P1–P4 | 在途任务接管见相邻 `inflight-task-takeover` |
| 不替代重加入 | 同主机多容器见相邻 `single-host-multicontainer` |
| 跨主机/整机/共享依赖 | 不承诺；PostgreSQL·Redis·Qdrant·ClamAV 为共享故障域 |
| 模型次数 | 不承诺恒为 1，P2 可重跑 |
| 下游不可去重/不可查询 | P4 不承诺绝对 exactly-once，诚实 `ambiguous` |
| 自动重启 | 不把 Docker 重启（`restart:"no"`）当作存活节点接管 |
| RTO | 不把健康/连接/队列恢复单独当作完整业务 RTO |

---

## 12. 网络隔离（脑裂）场景的专门分析

单节点故障有两类，恢复策略完全不同，**必须分开验收**：

| 故障类型 | 受害节点实际状态 | 谁能提交 | 必须靠什么机制 |
|---|---|---|---|
| **进程死亡** | 进程不存在，不再持有任何资源 | N2 | lease TTL 到期 + reclaim |
| **网络隔离** | 进程**仍活着**，只是连不上 Redis/DB | 只有 N2 能成功；N1 的提交必须被拒 | **fence**（lease 本身不足以解决） |

网络隔离是这套设计里最容易被低估的场景。假设只做 lease：

```text
N1 与 Redis 之间网络中断，但 N1 进程仍在跑、模型已算完
  → N1 无法续租，lease 在 TTL 后过期
  → N2 取得新 lease 并提交
  → 网络恢复，N1 尝试提交它"算好的"结果
  → 若没有 fence：N1 的迟到写会覆盖 N2 的结果（lost update）
```

加入 fence 后的正确行为：

```text
N1 acquire 时拿到 fence=20
  → 网络中断，N1 无法续租，但进程仍持有 fence=20 的执行上下文
N2 acquire 时拿到 fence=21，提交成功 → session_head.last_fence=21
  → N1 网络恢复，携带 fence=20 调 commit_turn
  → commit_turn: IF p_fence(20) < v_head.last_fence(21) THEN RAISE 'stale fence' (40001)
  → N1 提交被拒，N2 结果不变
```

**关键推论：**

1. `EnsureFenceAtLeast` 不是可选优化。没有它，Redis fence 计数在重启/主从切换后回退，新 lease 可能拿到小于数据库 `last_fence` 的值，合法接管者反被拒绝。
2. fence 的权威在**数据库**，不在 Redis。Redis 只负责"发放严格递增的值"，数据库负责"拒绝任何小于 `last_fence` 的提交"。
3. `CancelWithLease` 也必须走 `beforeCommit` 续租检查——否则"取消"这一写操作本身可能由已失权的节点完成。
4. 网络隔离下 N1 可能已经把模型调用完成、甚至已经调用过外部写工具。因此对**外部副作用**必须有业务幂等键，不能靠"提交被拒"来避免重复副作用。

**验收方法：** 不要只测 SIGKILL。额外做一次"网络隔离"演练（例如临时阻断 N1 到 Redis 的连接而不杀进程，或在测试环境用 iptables/网络策略模拟），观察 `commit_turn` 是否报出 `stale fence`（SQLSTATE 40001）并且 `session_head.last_fence` 未被回退。这是唯一能证明 fence 真正生效的方法。

---

## 13. 为什么"Docker 把 N1 拉起来了"不能证明接管

四类恢复机制必须拆开验收，混在一起会得出错误归因：

| 子场景 | 受害节点状态 | 能得出的结论 | 不能得出的结论 |
|---|---|---|---|
| **存活实例接管** | 受害实例**持续停止** | N2 是否能自行承接 N1 职责 | — |
| 自动重启 / 平台重建 | 允许重建受害实例 | 容器重建时间与恢复行为 | 不能证明 N2 曾接管 |
| 计划内优雅停机 | 先摘除、排空、退出 | 维护停机是否安全 drain | 不能替代强杀验收 |
| 共享依赖故障 | 应用实例可仍在，依赖停止 | 影响范围、降级一致性 | 不应预设持续服务 |

因此本包编排显式设置 `restart: "no"`：

- 若允许自动重启，`docker kill` 之后 Docker 会很快把容器拉起来，入口的健康集合可能从未真正变成"只剩 N2"，而 `process_start_id` 也会变成新值——此时"用户还能对话"既可能来自 N2 接管，也可能来自 N1 自愈，**结论无法归因**。
- 另一个隐患：自动重启会掩盖真正的启动期问题。若新进程因配置错误起不来，自动重启会进入 crash loop，把"接管成功"伪装成"间歇可用"。

**验收纪律：** 强杀后必须用两次 `docker inspect` 断言 `State.Running == false`，且**在接管验收完成前不启动受害节点**。只有在接管验收完成后，才允许显式 `compose start` 让 N1 重新加入（重新加入属相邻 `single-host-multicontainer` 包的验收范围）。

---

## 14. 配置与部署最小清单

| 配置 | 要求 | 违反后果 |
|---|---|---|
| `TRPC_WEBUI_LOCAL_INSTANCE_ID` | N1/N2 唯一且稳定 | owner 名称重合 → 接管不可归因 |
| 进程启动身份 | 每次启动随机 8 字节 hex，不可配置 | 重新加入被误认为旧 owner 复活（I9） |
| `TRPC_POSTGRES_DSN` | 两节点同一库 | N2 看不到 N1 已接收的消息 |
| `TRPC_REDIS_ADDRESS` | 两节点同一实例 | 队列/租约分裂 |
| `TRPC_LISTEN_ADDRESS` | 默认 `:8080` | 启动拒绝 |
| `TRPC_WECOM_LOCAL_ENABLED` | `true` | 主 Bot 不可用 |
| `TRPC_WECOM_SECONDARY_LOCAL_ENABLED` | `true`，且 `(Corp ID, Agent ID)` 与主 Bot 不同 | 配置解析拒绝：`incomplete` / `incompatible` |
| 主 Bot：`WECOM_CORP_ID` / `WECOM_AGENT_ID` / `WECOM_APP_SECRET` / `WECOM_CALLBACK_TOKEN` / `WECOM_ENCODING_AES_KEY` | 齐全；`WECOM_AGENT_ID` 为正整数 | 缺失即启动失败 |
| 次 Bot：`WECOM_SECONDARY_CORP_ID` / `WECOM_SECONDARY_AGENT_ID` / `WECOM_SECONDARY_APP_SECRET` / `WECOM_SECONDARY_CALLBACK_TOKEN` / `WECOM_SECONDARY_ENCODING_AES_KEY` | 齐全，且 `(Corp ID, Agent ID)` 与主 Bot 不同 | `incomplete` / `incompatible` |
| 恶意内容探针地址 | 可用（参与 `/readyz`） | `/readyz` 恒 503 → 节点被摘除 |
| `TRPC_WECOM_HA_ENTRY_BACKENDS` | ≥2、`http`、去重 | 入口拒绝启动 |
| `TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL` | 默认 1s，范围 `[100ms, 1m]` | 入口拒绝启动 |
| 回调地址 | 两个 Bot 同一 origin + 不同 `route_key` | 登记节点观测端口会使强杀后 callback 打到死节点 |

**装配值（部署生效，告警与时限结论必须引用）：** worker `LeaseTTL=30s` / `RenewInterval=10s` / `RetryWait=250ms` / `ReclaimInterval=5s` / `ReclaimLimit=100` / `DrainTimeout=30s`；delivery `ClaimTTL=30s` / `ClaimRenewInterval=10s` / `MaxAttempts=8` / `MaxReconcileAttempts=8`；relay `PollInterval=100ms`。

> ⚠️ 组件内部另有代码级兜底默认（如 `worker.Consumer` 在 `LeaseTTL<=0` 时退化为 5s）。文档、告警阈值、时限结论**一律引用装配值**，否则会得出错误量级的故障检测时限（差 6 倍）。

---

## 15. "上下文回忆正确"的判据（不是模型说了什么）

验收时容易犯的错误是：让模型自报租户，然后以此作为证据。**模型输出不构成路由证据。** 正确判据是四层一致：

```text
① 路由一致： callback 的 route_key → channel_public_route → channel_binding_locator
             → 验签 → binding，binding 的 tenant_id 与期望租户相同
② 入站一致： inbox 行 (tenant_id, channel, external_account_id, external_message_id) 命中期望租户
③ 会话一致： session_head / session_commit 的 (tenant_id, agent_app_id, session_id)
             三元组与期望租户一致；input_seq 连续推进；
             对话历史（模型上下文）取自官方会话后端的 session_events，
             其 app_name 前缀必须是 tenantID + "/" + agentAppID
④ 投递一致： delivery_ledger 的 tenant_id 与期望租户一致；每 segment 恰有一条终态
```

> **注意表名：** 平台表 `session_event`（单数）在生产 durable turn 中不产生新行（`p_events` 为 null），**不是**对话历史的来源；历史在官方会话后端的 `session_events`（复数，键为 `app_name`/`user_id`/`session_id`）。详见本包 [REFERENCE-IMPLEMENTATION.md](REFERENCE-IMPLEMENTATION.md) 的数据模型章节。

四层全部一致，且客户端可见回复内容只含本租户代号 → 才算"回忆正确且不串扰"。

**接管后转写（对话历史）会变成什么样（诚实说明）：** 由于一次接管 = 以同一输入重开一轮，且框架每次运行都会把输入追加为一条新事件，接管后的历史里会出现：① 上一次尝试留下的痕迹（若当时已请求工具调用，则该 tool_call 消息仍在；它没有配对结果，构造模型请求时会被降级为带 `[orphan_tool_call]` 文本标记的 user 消息）；② 重开的这一轮追加的**第二条同内容用户消息**。这不影响正确性（回复仍只提交一次、租户边界不变），但会让历史变长；排查时不要把它误判为"重复投递"或"串扰"。机制细节见 [../inflight-task-takeover/REFERENCE-IMPLEMENTATION.md](../inflight-task-takeover/REFERENCE-IMPLEMENTATION.md) §10。

**自动化的快速核对（SQL）：**

```sql
-- ② 入站归属
SELECT tenant_id, channel, external_account_id, external_message_id, request_id, agent_app_id, session_id, input_seq, state
FROM inbox WHERE request_id = '<REQUEST_ID>';

-- ③ 会话归属与推进
SELECT tenant_id, agent_app_id, session_id, input_seq, commit_id, outcome, fence, session_version, reply_cursor, result_ref
FROM session_commit WHERE request_id = '<REQUEST_ID>';

-- ④ 投递归属与唯一性
SELECT tenant_id, delivery_key, segment_no, segment_count, state, attempt, client_request_id, provider_message_id
FROM delivery_ledger WHERE tenant_id = '<TENANT_ID>' AND delivery_key = '<DELIVERY_KEY>';
```

**跨租户污染检查（期望 0 行）：**

```sql
SELECT s.tenant_id, s.session_id, c.tenant_id AS commit_tenant
FROM session_head s JOIN session_commit c
  ON c.agent_app_id = s.agent_app_id AND c.session_id = s.session_id
WHERE c.tenant_id <> s.tenant_id;
```

---

## 16. 与相邻文档包如何组合

| 能力 | 回答的问题 | 不能替代什么 |
|---|---|---|
| **本包**（故障后继续对话） | 新消息能否在原 tenant / 原会话正确处理 | 不能证明旧的在途消息已被接管 |
| `inflight-task-takeover` | 已接收的原消息在 P1–P4 各窗口是否自动完成 | 不能证明公开入口在节点故障时仍能接收新 callback |
| `single-host-multicontainer` | 两实例生命周期、稳定入口与反向故障是否稳定 | 不能证明整机故障或跨故障域可用性 |

三者组合后的完整故事才是：**入口仍能收到新消息 → 正确路由到租户和会话 → 已接收旧任务可回收 → 新节点有唯一提交权 → 最终回复可恢复且可追踪。**

---

## 17. 如何阅读其余文档

- `design.md`：架构图、四元组持久化链路、数据模型、会话状态机、正常/故障时序图、模块划分、错误码与拒绝语义。
- `implementation-walkthrough.md`：启动装配 → 可信入口候选生命周期 → inbox → dispatch → worker 接管 → 会话存储 → 原子提交 → relay → delivery，逐步给出 durable truth 与"此刻被杀会怎样"。
- `CODE-APPENDIX.md`：关键实现代码摘录与状态变化解释。
- `REFERENCE-IMPLEMENTATION.md`：**从零复现**所需的完整 DDL、Redis Lua 契约、Go 接口、算法、配置全表、启动顺序、验证清单。
- `testing-and-acceptance.md`：测试方案、脚本逐条解释、稳定性/负载检查、通过判定。
- `release-and-operations.md`：配置门禁、部署、发布顺序、观测、告警、回滚约束、分场景故障处置手册。
