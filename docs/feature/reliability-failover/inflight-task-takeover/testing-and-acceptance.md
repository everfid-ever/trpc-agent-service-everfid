# 在途任务接管与幂等投递 — 测试与验收

> 本文给出完整测试方案：参数、P1–P4 逐步命令与预期、脚本行为逐条解释、「三种计数」的核对方法、证据字段与可直接执行的证据 SQL、通过/不通过判定表与最小证据闭环、反例清单与回归断言、时间口径、结论模板与 CI 集成建议。复现材料见 [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)。

---

## 一、统一流程与参数

脚本使用 `webui-multinode` profile 启动隔离 Compose 项目。两个节点均显式启用 `TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED=true`；每轮只对 **node-a** 的一个 point 调用受保护的 `arm` endpoint。脚本确认该节点命中后直接 SIGKILL 它，node-b 从 durable state 接管。

> 🔴 `release` endpoint 只用于 endpoint 单测或不杀进程的局部调试；标准 E2E **不调用 release**。若某轮需要手工改状态、重投消息或释放存活节点的 barrier 才"通过"，该轮结论应为**未验证自动接管**。

```bash
# 用法
$0 <p1|p2|p3|p4>          # 或 set TRPC_INFLIGHT_POINT
TRPC_INFLIGHT_TENANT_ID=<tenant-a-id> \
  bash scripts/e2e/inflight-takeover.sh p2
```

| 变量 | 含义 | 默认 |
|---|---|---|
| `TRPC_INFLIGHT_POINT` / `$1` | 选 P1–P4 窗口 | （必填） |
| `$1` | 选 P1–P4 窗口 | `p2` |
| `TRPC_WEBUI_LOCAL_TOKEN` | 控制面与 WebUI 签名 token | `local-webui-token-change-me` |
| `TRPC_WEBUI_LOCAL_ROUTE_KEY` | WebUI 路由 key | `local-webui` |
| `TRPC_LOCAL_MULTINODE_NODE_A_PORT` / `..._NODE_B_PORT` | 临时节点观测端口 | 脚本随机分配 |

**前置校验（脚本自动执行，失败即退出码 2）：**

- `docker`、`docker compose version`（v2）、`curl` 必须存在；
- `deploy/compose/secrets/deepseek-api-key` 必须存在且**非空**；
- 两个节点的 `/readyz` 必须在 `TRPC_INFLIGHT_TIMEOUT_SECONDS` 内返回 200。

---

## 二、脚本行为逐条解释

`scripts/e2e/inflight-takeover.sh` 主线：

1. 以随机项目名和随机 host ports 执行 `docker compose --profile webui-multinode up --detach --build`。
2. 等待 node-a、node-b 的 `/readyz`。
3. 对 node-a `POST /test/failover/<point>/arm`（`X-TRPC-Local-Token` 鉴权），并经签名 WebUI callback 写入一条带唯一 `external_message_id` 的输入。
4. 轮询 node-a `/test/failover/<point>/status` 至 `{"hit":true}`；这证明已到达该 durable boundary。
5. `docker compose kill -s KILL webui-node-a`；等待 node-b ready，并轮询 node-b 的同一会话 reply。
6. 成功时打印原 message id；失败时保留 Compose 日志到临时目录，随后销毁仅本轮创建的项目和 volumes。

| 参数 | 作用 | 影响 |
|---|---|---|
| `$1` | 选窗口 | 决定 node-a 要 arm 的 P1–P4 barrier |
| `TRPC_WEBUI_LOCAL_TOKEN` | 请求鉴权 | 保护 arm/status/release 控制面与 callback 签名 |
| `TRPC_LOCAL_MULTINODE_NODE_A_PORT` / `...B...` | 端口覆盖 | 方便固定端口的本地调试；脚本默认随机 |

> **脚本负责制造一条经签名的 WebUI 输入。** 真实 WeCom 的人工会话连续性由 `single-host-multicontainer.sh` 和外部渠道演练验证；P1–P4 脚本只证明共享 durable state 上的接管。

---

## 三、P1–P4 逐步命令与预期

> 通用：把命令中的 `p2` 换成 `p1`/`p3`/`p4` 即对应窗口。以下以 **Tenant A 为靶机、Tenant B 为并发对照**。

### 3.1 P1（已持久化未执行）

```bash
TRPC_INFLIGHT_TENANT_ID=<tenant-a-id> bash scripts/e2e/inflight-takeover.sh p1
```

- **命中前**：`inbox` 出现 `preprocess_pending`/`dispatch_pending` 行；`preprocess_job` 出现 `ready` 行；barrier 命中（P1 挂在 `preprocess.Worker.dispatch` 调用 `Dispatcher.Dispatch` **之前**）。
- **强杀后**：`preprocess_job` 保持 `ready` / `dispatched_at IS NULL`。
- **存活节点动作**：`RunOnce → ClaimReadyForDispatch` 自动接力 `dispatch`，创建 `execution_record`（`input_seq` 与 `prepare_dispatch` 收敛到同一值）。
- ✅ **通过**：原消息最终产生 execution 并走到 terminal；用户未重发。
- ❌ **不能作为通过**：人工重新入队；或 victim 恢复后由 Docker 重启继续（编排已 `restart:"no"`）。

### 3.2 P2（模型/工具执行中）

```bash
TRPC_INFLIGHT_TENANT_ID=<tenant-a-id> bash scripts/e2e/inflight-takeover.sh p2
```

- **命中前**：`execution_record.outcome='running'`，已持有 lease（Redis `trpc:{env}:{tag}:lease`）；barrier 命中（P2 挂在模型/工具与 `renderOutbound` 之后、`CommitTurn` 之前）。
- **强杀后**：lease 停止续期 → TTL 过期；Redis stream entry 保持 pending（未 ACK）。
- **存活节点动作**：`Reclaim` 同一 entry → `ReadLastFence` → `EnsureFenceAtLeast` 校准 → `Acquire` 更高 fence → **重跑模型**（模型调用次数允许 >1）→ `commit_turn(fence=新)` 成功，`session_head.last_fence` 推进。
- **旧 owner 迟到**：`commit_turn(fence=旧)` → `stale fence`（SQLSTATE `40001`）→ `ErrStaleFence`，无状态变化。
- ✅ **通过**：同一 `request_id` 完成唯一 terminal commit；旧 fence 被拒；最终回复只一次。
- ❌ **不能作为通过**：只证明"新请求成功"（必须同一 `request_id`）。

### 3.2.1 P2 的两个子窗口：模型已请求工具、工具执行中 / `ask` 工具已消费授权

`p2` barrier 停在"模型与工具都跑完、结果尚未提交"，**停不到工具函数体内部**。要在本地复现子窗口，需让被测工具自身阻塞（长 sleep 或等待外部信号），再强杀该节点。判定与证据如下（机制见 [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md) §10、[FULL-GUIDE.md](./FULL-GUIDE.md) §4.5）：

```bash
# 准备：用一个会阻塞的工具或把工具实现改为 sleep 600；启动两节点后
# 1) 发一条带唯一标记的消息，等日志确认工具已进入执行（尚未返回）
# 2) 强杀持有该任务的节点
docker kill --signal=KILL <victim_container>
```

| 检查项 | 子窗口 P2-a（`allow` 工具执行中） | 子窗口 P2-b（`ask` 工具已消费授权） |
|---|---|---|
| 期望 durable 记录 | 转写中存在含 `tool_calls` 的 assistant 消息；**无** `tool_attempt` 行 | 同左，**且有** `tool_attempt.state='effect_unknown'` |
| 期望 durable 缺失 | 工具进度、工具结果、`result_payload` | `tool_result_payload`（若工具尚未返回） |
| 接管后动作 | 取更高 fence → **以同一输入重开一轮**，模型重新决策；工具可能被再次调用 | **不重跑工具**：读到 `tool_result_payload` 则采用该结果；读不到则以 `ReasonToolEffectUnknown` 终结并告警 |
| 通过判据 | 同一 `request_id` 唯一 terminal commit；每 segment ≤1 行 `sent`；用户无需重发 | 同上，且**工具未被第二次调用**（外部系统按业务幂等键只有一个业务效果） |
| 不得作为通过 | "工具只执行了一次"（`allow` 路径不承诺）；转写中出现第二条同内容用户消息（重开一轮的正常现象） | 把 `effect_unknown` 直接判为成功或直接重跑 |

```sql
-- 证据 ①：转写里是否留下本轮工具调用（SDK 表，复数）
SELECT id, created_at, event->'choices'->0->'message'->'tool_calls' AS tool_calls
FROM session_events WHERE app_name = $1 AND user_id = $2 AND session_id = $3
  AND event->'choices'->0->'message'->'tool_calls' IS NOT NULL ORDER BY id;

-- 证据 ②：ask 工具的效果判定状态（allow 工具此处应为空集）
SELECT grant_id, tool_call_id, state, result_ref, updated_at
FROM tool_attempt WHERE tenant_id = $1 AND request_id = $2;
```

### 3.3 P3（结果已提交未发送）

```bash
bash scripts/e2e/inflight-takeover.sh p3
```

- **命中前**：`session_commit` 出现终态行、`session_head.next_input_seq` 已 +1、`reply` outbox 已被 relay claim、`result_payload` 已写；barrier 命中在构造 `ReplyEvent` 后、`PublishReply` 前。
- **强杀后**：reply outbox claim 到期，`result_payload` 不变；Delivery Ledger 尚未被该 barrier 触及。
- **存活节点动作**：relay 重领 reply outbox → 发布同一 `ReplyEvent` → `Deliver` → `ClaimDelivery` → 发送**已保存**的 `result_payload`；**绝不重跑模型**。
- ✅ **通过**：已提交结果被发送；`session_commit` 终态唯一（未被二次提交）；恢复阶段的模型调用次数 = 0。
- ❌ **不能作为通过**：只看到 outbox 记录；或重新触发了模型执行。

### 3.4 P4（下游已接受本地未确认）

```bash
bash scripts/e2e/inflight-takeover.sh p4
```

- **命中前**：`delivery_ledger.state='sending'` 且 `claim_until` 有效（已 `ClaimDelivery`）；barrier 命中在 provider 调用之前。
- **强杀后**：`state` 停在 `sending`，claim 到期，**没有** provider receipt；这是可安全重试的窗口。
- **存活节点动作**：重新 claim 后以同一 `client_request_id` 调 provider 并完成 `sent`；若在真实 provider 调用后的网络中断形成 `ambiguous`，再按下游去重/查询对账；下游不支持时保留 `ambiguous` 并告警。
- ✅ **通过**：同一原输入只产生一次 visible reply，ledger 终态可解释。
- ❌ **不能作为通过**：把 P4 测试 barrier 当作“provider 已接受未落库”的模拟；该未知结果窗口靠 adapter reconcile 的单独反例测试覆盖。

### 3.5 速查表（命中前 / 强杀后 / 存活动作）

| 命令 | 命中前状态 | 强杀后状态 | 存活节点动作 | 期望结果 |
|---|---|---|---|---|
| `... p1` | `inbox` 有行 + `preprocess_job.state='ready'` | job 保持 `ready` / `dispatched_at` NULL | `ClaimReadyForDispatch` 接力 dispatch | execution 被创建并走到 terminal |
| `... p2` | `execution_record.outcome='running'` + lease | lease TTL 过期 + stream pending | `Reclaim` + 新 fence 重跑模型 | 唯一 terminal commit，旧 fence 被拒 |
| `... p3` | `session_commit` 终态 + reply outbox claimed | reply event 未发布 | relay 重领并发布 + 发送 `result_payload` | 已提交结果被发送，模型不重跑 |
| `... p4` | `delivery_ledger.state='sending'`、provider 尚未调用 | claim 到期 | 回收超时 claim + 安全发送 | 一次 visible reply |

---

## 四、「三种计数」的核对方法

| 计数 | 允许 >1？ | 落库字段 | 核对方法 |
|---|---|---|---|
| **模型调用次数** | **允许** | 不落库 | 看执行/重试日志；P2 接管必然 ≥2 次；**不据此判定失败** |
| **任务尝试次数** | 受控 | `execution_record.park_attempt`、`delivery_ledger.attempt`、`delivery_ledger.reconcile_attempt` | 查表；`park_attempt` ≤ `park_execution` 的 `p_max_attempts`（≤64）；`attempt` ≤ `MaxAttempts`（装配值 8）；`reconcile_attempt` ≤ `MaxReconcileAttempts`（装配值 8） |
| **最终回复次数** | **必须 = 1（每 segment）** | `delivery_ledger` 每 segment 一行，`state='sent'` | `count(*) FILTER (WHERE state='sent')` 每 segment 必须 = 1 |

**核对 SQL：**

```sql
-- 任务尝试次数
SELECT request_id, park_attempt, outcome FROM execution_record
WHERE tenant_id=$1 AND request_id=$2;
SELECT delivery_key, segment_no, attempt, reconcile_attempt, state, last_error_class
FROM delivery_ledger WHERE tenant_id=$1 AND delivery_key=$2 ORDER BY segment_no;

-- 最终回复次数（每 segment 必须恰好 1 行 sent）
SELECT delivery_key, segment_no,
       count(*) FILTER (WHERE state='sent') AS sent_rows,
       count(*) AS rows_total
FROM delivery_ledger WHERE tenant_id=$1 AND delivery_key=$2 GROUP BY 1,2;
```

> 🔴 **铁律：** 三者互不等价。一次 terminal commit 可能来自多次模型调用；一次最终回复背后可能有多次 `attempt`；但最终回复一旦 `sent`，只许 ACK，不许重发。

---

## 五、证据字段清单

每条用例必须可关联以下字段（凭据与真实用户标识脱敏）：

| 类别 | 字段 |
|---|---|
| 运行 | `run_id`、Compose 项目名、节点端口、镜像 digest / commit SHA |
| 租户 / 会话 | `tenant_id`、`channel_binding_id`、`session_id` |
| 消息 | `request_id`、`external_message_id`、`inbox.state`、`input_seq` |
| 任务 | `execution_record.outcome`、`park_attempt`、lease key、fence |
| 交付 | `delivery_ledger.state`、`segment_no`、`attempt`、`reconcile_attempt`、`provider_message_id`、`client_request_id`、`last_error_class` |
| barrier | `point`、`hit`、`released`、`/statusz` JSON 快照 |
| 故障 | victim 容器 `State.Running`、`t0`（SIGKILL 时刻）、survivor `/readyz` |
| 计数 | 模型调用次数（日志）、`park_attempt`、`delivery_ledger.attempt`、`sent` 行数 |
| 回复 | 最终可见回复内容 + 去重/对账证据（P4 必需） |

---

## 六、按窗口的最小证据 SQL（可直接执行）

```sql
-- P1：接收证据 + 存活节点自动 dispatch
SELECT j.state, j.dispatched_at, j.attempt, e.request_id, e.input_seq
FROM preprocess_job j
LEFT JOIN execution_record e ON e.tenant_id = j.tenant_id AND e.request_id = j.request_id
WHERE j.tenant_id = $1 AND j.request_id = $2;

-- P2：唯一 terminal commit + fence 轨迹
SELECT commit_id, outcome, fence, session_version, result_ref
FROM session_commit WHERE tenant_id = $1 AND request_id = $2;
SELECT last_fence, next_input_seq, version FROM session_head
WHERE tenant_id = $1 AND agent_app_id = $2 AND session_id = $3;

-- P3：已提交结果被发送（result_payload 存在），且无新的 execution 产生
SELECT e.outcome, p.content_digest, p.created_at
FROM execution_record e
JOIN result_payload p ON p.tenant_id = e.tenant_id AND p.request_id = e.request_id
WHERE e.tenant_id = $1 AND e.request_id = $2;

-- P4：最终回复只一次；若下游不可去重则应诚实 ambiguous
SELECT segment_no, state, attempt, provider_message_id, client_request_id
FROM delivery_ledger WHERE tenant_id = $1 AND delivery_key = $2 ORDER BY segment_no;
SELECT segment_no, state, last_error_class, reconcile_attempt
FROM delivery_ledger
WHERE tenant_id = $1 AND delivery_key = $2 AND state = 'ambiguous';

-- 通用：入口 inbox 归属（防串租户）
SELECT tenant_id, channel, external_account_id, external_message_id, request_id, state, input_seq
FROM inbox WHERE request_id = $2;
```

---

## 七、通过 / 不通过判定表与最小证据闭环

### 7.1 判定表

| 子用例 | 必须通过 | 不能作为通过依据 |
|---|---|---|
| **P1** | 已持久化原消息在故障后**自动**执行 | 用户重新发一条相同文本 |
| **P2** | 新 fence owner 完成**原 `request_id`**，旧 fence 被拒 | 只证明新请求成功 |
| **P3** | 已提交 result 被发送，模型不重跑 | 只看到 outbox 记录 |
| **P4** | 用 provider 接受/对账证据解释去重，或诚实 `ambiguous` 已告警 | 「客户端暂未显示」 |
| **通用** | 同一 `request_id` 完成、最终回复一次、不串 tenant/session | 手工重新入队后才完成 |

**全局不通过（任一即失败）：**

- 超时未处理（超出 `T_TASK` / `T_DELIVER`）；
- 原消息丢失（无 `inbox` / `execution_record` 证据）；
- 串 tenant / session；
- 旧节点迟到提交成功（`stale fence` 未拦截，即 fence 机制失效）；
- 重复最终回复（同一 segment `sent` 行 > 1）；
- 必须手工重新入队 / 补发后才完成；
- 把 Provider 无法去重的 P4 写成恰好一次。

### 7.2 最小证据闭环（缺一不可）

一个 P 点用例判定"通过"至少需要以下六项证据同时成立：

1. **接收证据**：`inbox` 在 `t0` 之前已有行（该原消息确实已被可靠接收）；
2. **未人工干预**：全程无「手工重新入队 / 手改状态 / 删 lease / 补发回复」；
3. **自动完成**：同一 `request_id` 在受害节点**停止期间**出现终态 `session_commit`；
4. **单回复**：`delivery_ledger` 每 segment 恰好一行 `sent`；
5. **对照正常**：作为对照的租户（建议 Tenant B）全程不串 tenant / session；
6. **计数合规**：模型调用次数 ≥1（P2 可 >1）、任务尝试次数在阈值内、最终回复 =1。

---

## 八、反例清单

### 8.1 设计上必须避免（反模式）

| 反模式 | 后果 |
|---|---|
| 只做接收去重 | P2/P3/P4 全部失守（需要 lease / fence / ledger 三层） |
| 只靠 lease 不加 fence 校验 | Redis 重启或网络恢复后旧 owner 翻案；`commit_turn` 的 `p_fence < last_fence` 才是最终防线 |
| P3 重跑模型 | 违反"一次 terminal commit"，且工具副作用重复 |
| ACK 早于 terminal commit | 在途任务永久丢失（不变量 I5） |
| 用一个幂等键覆盖全阶段 | "收到""生成""已接受"是三个不同事实，必须分层 |
| P4 承诺 exactly-once | 仅当下游支持 `client_request_id` 去重/查询时才收敛，否则只能诚实 `ambiguous` |
| 把 Docker 自动重启当接管 | 编排已 `restart:"no"`；重启不算存活节点接管 |
| 把健康/连接/队列恢复单独当 RTO | 不承诺单点恢复即完整业务 RTO |

### 8.2 必须能复现「不通过」的回归构造

这些用例用于验证**验收本身可信**：本能力必须在这些构造下稳定判定失败。

| 反例构造 | 预期判定 |
|---|---|
| 把 `T_TASK` / `T_DELIVER` 设成远小于实际恢复时间 | 不通过（超阈值） |
| 让 Tenant B 的消息被 Tenant A 的 owner 处理 | 不通过（I1 失守，串 tenant） |
| 人为让旧 owner 在 N2 提交后提交 | 不通过（应被 `stale fence` 拒绝） |
| 禁用 ledger claim 的条件更新（去掉 version/owner/client_request_id） | 不通过（同一 segment 两行 `sent`，I6 失守） |
| 杀前手动 re-dispatch | 不通过（非自动接管） |
| 下游不支持去重却标注 P4 成功 | 不通过（应 `ambiguous` + 告警，I7 失守） |

---

## 九、外部工具（副作用）验证

模型调用次数可以大于一，但**外部写入必须只产生一次预期效果**。使用独立测试资源，故障后按业务幂等键查询实际记录数量。

- [ ] 工具调用**前**持久化 idempotency key（如 `tenant + 原请求 + 业务操作`）；
- [ ] 调用**后**持久化外部 resource ID，供后续对账；
- [ ] 超时或未知响应进入显式不确定状态（不自动盲重试）；
- [ ] 必须人工或查询外部系统对账后，才决定继续或放弃。

> 无法对账的外部工具，不得被标记为"自动重试安全"。

---

## 十、验收剧本与时间口径

| 步骤 | 动作 | 记录字段 | 期望 |
|---|---|---|---|
| 0 | 基线两节点 `/readyz` | 端口、HTTP 200 | 双双健康 |
| 1 | 发唯一标记消息 | `request_id`、`external_message_id` | `inbox` 入库 |
| 2 | 等 barrier hit | `/statusz` JSON | `enabled=true` / `hit=true` / `point=p*_` |
| 3 | `t0` = SIGKILL victim | victim 容器 id、时刻 | `State.Running=false` |
| 4 | 等 survivor `/readyz` | `t_connect` | 200 |
| 5 | release 存活节点 barrier | HTTP 204 | 恢复测试暂停 |
| 6 | 等完成 | `t_ready` | 同 `request_id` 出现终态 + 最终回复可见 |
| 7 | 核对三计数 | 日志 + §四/§六 SQL | 模型 ≥1 / 任务受控 / 最终 =1 |
| 8 | 对照租户核对 | Tenant B 的 session / ledger | 不受影响、无串扰 |
| 9 | 收尾 | 诊断目录、`DELTA_POLL` | 证据可复核 |

采样间隔 `DELTA_POLL` 必须随结论记录，**不得宣称比采样更精确的 RTO**。

---

## 十一、验收结论模板（每个 P 点一份）

```text
用例: <p1|p2|p3|p4> / tenant=<id> / run_id=<id>
镜像: <digest> / commit=<sha> / config_version=<n>
t0(强杀): <timestamp>  victim=<container>  victim_running=false
barrier: point=p*_  hit=true  released=<timestamp>  (survivor=<node>)
接收证据(F1): inbox.request_id=<id> state=<state> input_seq=<n> @ <t0 之前>
自动完成: session_commit.outcome=<...> fence=<n> @ t_ready=<timestamp>
最终回复: delivery_ledger sent 行数=<n> / segment_count=<n>
三计数: 模型调用=<n>(日志) 任务尝试(park_attempt/attempt/reconcile)=<n>,<n>,<n> 最终回复=<n>
对照租户(Tenant B): 正常 / 异常(<原因>)
人工干预: 无 / 有(<具体动作>)   ← 有则判定不通过
判定: 通过 / 不通过（原因: ...）
DELTA_POLL: <n>s   证据存档: <diagnostics 目录>
真实 WeCom 验收: assumed / recorded
```

---

## 十二、CI 集成建议

- 每个 P 点作为**独立 CI job**，使用隔离 Compose 项目 + `TRPC_INFLIGHT_KEEP_ENVIRONMENT=false`。
- 断言退出码：`0`=通过，非 `0`=不通过；**禁止用"脚本跑完"代替"断言通过"**。
- 显式记录 `TRPC_WECOM_REAL_ACCEPTANCE`（默认 `assumed`），真实对话证据另存独立运行目录。
- §8.2 的反例用例作为**"必须失败"的契约测试**一并运行；若某个反例没能被判失败，说明验收判据本身失效，应优先修复判据而不是放过。
- 每个 job 产出 §十一 的结论模板文本作为 artifact，便于横向比较不同 P 点的恢复行为。

## 当前实现增量验收：工具执行中强杀

除 P1–P4 barrier 外，新增以下判定：可查询工具在接管后只能查询并复用已完成结果；幂等工具的多次调用必须使用同一 idempotency key；`manual` 工具不得再次调用外部实现，终态原因应为 `tool_effect_unknown`。证据至少包含 `tool_execution.attempt/fence/lease_owner/state`、`tool_attempt.state`、`tool_result_payload.result_ref` 和最终 `session_commit`。
