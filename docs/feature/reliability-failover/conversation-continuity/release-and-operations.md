# 故障恢复后继续对话 — 发布与运维

> 本文覆盖：配置门禁、部署拓扑、发布顺序、观测指标、告警阈值、回滚约束、日常巡检、容量测算与**分场景故障处置手册**。
> 所有环境变量名、默认值、owner 命名、状态机取值均与参考实现逐字一致；如与原实现交叉核对，可见各节标注的源文件位置。

---

## 一、配置门禁（上线前必须逐项确认）

### 1.1 必填配置

| 配置 | 要求 | 违反后果 |
|---|---|---|
| `TRPC_WEBUI_LOCAL_INSTANCE_ID` | N1/N2 唯一且稳定；同一槽位重新加入也不得复用别人的值 | owner/consumer 名称冲突 → 租约与 claim 误判 |
| 进程启动身份（`ProcessStartID`） | 由进程启动时随机生成 8 字节 hex（16 字符），**不可配置、不可复用** | 重新加入被误认为旧 owner 复活（不变量 I9 失效） |
| `TRPC_POSTGRES_DSN` | 两个应用节点必须指向**同一**业务库 | N2 看不到 N1 已接收的消息 → 无法接管 |
| `TRPC_REDIS_ADDRESS` | 两个应用节点必须指向**同一** redis | 队列与租约分裂 → 双写或任务永久 pending |
| `TRPC_LISTEN_ADDRESS` | 默认 `:8080`；不得有前导/尾随空白 | 启动即拒绝 |
| `TRPC_WECOM_LOCAL_ENABLED` | `true`（主 Bot） | 入口注册失败 |
| `TRPC_WECOM_SECONDARY_LOCAL_ENABLED` | `true`（第二 Bot） | 只有一个租户 → 无法验证不串扰 |
| 主 Bot 凭据：`WECOM_CORP_ID`、`WECOM_AGENT_ID`、`WECOM_APP_SECRET`、`WECOM_CALLBACK_TOKEN`、`WECOM_ENCODING_AES_KEY` | 齐全；`WECOM_AGENT_ID` 必须可解析为正整数 | 缺失即启动失败 |
| 次 Bot 凭据：`WECOM_SECONDARY_CORP_ID`、`WECOM_SECONDARY_AGENT_ID`、`WECOM_SECONDARY_APP_SECRET`、`WECOM_SECONDARY_CALLBACK_TOKEN`、`WECOM_SECONDARY_ENCODING_AES_KEY` | 齐全；`(Corp ID, Agent ID)` 元组必须与主 Bot **不同** | 任一缺失 → `secondary WeCom local configuration is incomplete`；与主 Bot 相同 → `... is incompatible` |
| 恶意内容探针地址（malware） | 可用；参与 `/readyz` | `/readyz` 恒 503 → 节点被入口摘除 |
| 稳定入口后端列表 | ≥2 个 `http` 地址、去重、无 user/query/fragment | 入口配置拒绝：`entry requires at least two backends` |

**门禁命令（在改动任何配置后执行）：**

```bash
# 1. 静态校验 Compose 渲染结果（不启动容器）
docker compose -f deploy/compose/docker-compose.local.yml \
  --profile wecom-ha-local config -q

# 2. 校验两个节点的身份不同
docker compose -f deploy/compose/docker-compose.local.yml --profile wecom-ha-local config \
  | grep -E "TRPC_WEBUI_LOCAL_INSTANCE_ID|TRPC_WECOM_HA_ENTRY_BACKENDS"

# 3. 校验两组 Bot 凭据元组不同（人工确认 WECOM_CORP_ID/AGENT_ID 与 SECONDARY 版不同）
```

> 🔴 **红线：** 两个应用节点一旦出现相同 `TRPC_WEBUI_LOCAL_INSTANCE_ID`，owner 名称将完全重合。此时租约、consumer group、delivery claim 都无法区分执行者，"谁接管了谁"不可证，演练结论无效。

### 1.2 参数取值纪律

| 参数 | 装配值（部署生效） | 取值纪律 |
|---|---|---|
| `LeaseTTL`（会话执行许可） | 30s | **不得写死在业务逻辑中**；它是故障检测时限的一部分，必须可记录、可在演练前固定 |
| `RenewInterval` | 10s | 必须显著小于 TTL（一般 ≤ TTL/3）；大于 TTL 会导致自认为仍持有租约但实际上已过期 |
| `RetryWait`（竞争重试间隔） | 250ms | 过小会放大 DB 压力；过大延长竞争收敛时间 |
| `ReclaimInterval`（worker 回收扫描） | 5s | 决定接管发现延迟；过大延长 RTO |
| `ClaimTTL`（投递台账领取） | 30s | 决定发送者崩溃后多久可被接管 |
| `ClaimRenewInterval` | 10s | 必须小于 `ClaimTTL` |
| delivery `MaxAttempts` / `MaxReconcileAttempts` | 8 / 8 | 与告警阈值联动，不能只调大不调告警 |
| delivery `DefaultRetryDelay` / `MaxRetryDelay` | 1s / 1min | 退避上限过大将拉长 P4 收敛时间 |

> ⚠️ 上表是 `webui-local` 角色**显式传入的装配值**，即部署实际生效的值。组件内部另有代码级兜底默认（如 `worker.Consumer` 在 `LeaseTTL<=0` 时退化为 5s、`ReclaimInterval<=0` 时退化为 1s），仅在调用方未设置时生效。**告警阈值与时限结论一律引用装配值**，混用会得出差一个数量级的错误 RTO。

---

## 二、部署拓扑

```text
                      外部 HTTPS tunnel
                             │
              ┌──────────────▼───────────────┐
              │  wecom-ha-entry   :58087      │  ← 唯一允许暴露给企业微信控制台的端口
              │  每秒探测 /readyz，轮转转发    │
              └───────┬──────────────┬───────┘
                      │              │
        ┌─────────────▼───┐   ┌──────▼──────────┐
        │ wecom-ha-node-a │   │ wecom-ha-node-b │   restart: "no"
        │  :58088（观测）  │   │  :58089（观测）  │
        └────────┬────────┘   └────────┬────────┘
                 └─────────┬───────────┘
                           ▼
        postgres（权威）  redis（队列/租约）  qdrant  clamav  otel
        └── 同属单一故障域：不承诺其故障下的可用性 ──┘
```

编排启动顺序（`depends_on` 已声明）：

```text
postgres/redis (service_healthy) ─┐
qdrant (service_healthy) ─────────┼─▶ wecom-ha-bootstrap (webui-local-bootstrap, 一次性)
clamav (service_healthy) ─────────┘            │ service_completed_successfully
                                               ▼
                            wecom-ha-node-a / wecom-ha-node-b (wecom-local)
                                               │ service_started
                                               ▼
                                       wecom-ha-entry (wecom-ha-entry)
```

**企业微信回调地址配置（关键）：** 两个 Bot 使用**同一个公开 origin**，仅靠 durable route key 区分：

```text
Bot A（主）  : https://<tunnel-host>/callbacks/wecom?route_key=local-wecom
Bot B（次）  : https://<tunnel-host>/callbacks/wecom?route_key=local-wecom-secondary
```

🔴 **不得**把 `58088` / `58089`（节点观测端口）登记为企业微信回调地址。它们绕过入口的 readiness 摘除逻辑，会让强杀后的 callback 打到死节点上，演练结论失真。

🟠 入口不是业务网关的替代品：它不决定 tenant、不保存会话、不做业务幂等。它只做"选择有限且可观测的健康后端"。callback 在**尚未获得业务响应**时遇到连接错误、502、503、504 才会换后端；一旦后端返回业务响应，入口不会自行发起第二次业务调用。Provider 重传与入口切换造成的重复，最终仍由后端 `inbox` 的 provider message identity 归并。

---

## 三、发布顺序

1. **先发布向后兼容的 schema**，再发布使用新字段的二进制。新增列/表必须默认值可空或可推导；删除、重命名、收紧 `CHECK` 一律走"先兼容、后清理"两阶段。
2. **先发布兼容新旧 schema 的 consumer**，再执行扩展迁移。理由：旧 consumer 遇到未知 envelope 字段必须能忽略，遇到未知 delivery 状态必须能安全遍历，而不是 panic 或误判为 failed。
3. **canary 单节点验证**以下五类信号后，才允许第二节点加入：
   - tenant route 正确（两个 Bot 各自命中自己的 tenant）；
   - lease 获得/续约/释放正常，无 `lease_lost` 激增；
   - delivery retry 路径可达（可注入一次可重试错误观察 `retry_wait → pending`）；
   - stale fence 拒绝确实发生（可由旧 fence 模拟请求验证被 DB 拒绝）；
   - backlog 不增长（pending / oldest pending 稳定）。
4. **两节点并行运行并通过基线稳定窗口**后，才允许扩大流量或开启第二个 Bot。
5. **红线：** 不允许在 worker 尚未升级时启用新 envelope 字段或新 delivery 状态；否则老节点无法解释新状态，可能把 `ambiguous` 误当 `failed` 而放弃可收敛的投递。

---

## 四、观测指标

### 4.1 必须可观测的对象

| 类别 | 指标 / 观测点 | 说明 |
|---|---|---|
| 实例身份 | `/statusz.instance_id`、`/statusz.process_start_id`、`/statusz.owners.*` | 把 lease、consumer、claim、日志映射到**真实进程**；不得靠容器名猜 owner |
| 健康 | `/livez`、`/readyz`、入口 `/readyz`、入口 `/statusz.backends[].healthy` | live 只说进程没退出；ready 才算能服务业务 |
| 会话执行 | lease 获得次数、续约失败次数、`ErrLeaseLost` 计数 | 续约失败是"即将失去执行权"的先兆 |
| 提交 | `commit_turn` 成功数、`ErrStaleFence` 拒绝数、`ErrVersionConflict` 数 | stale fence 拒绝是 fence 机制**正在工作**的证据 |
| 队列 | stream pending 总数、oldest pending 年龄、reclaim 次数 | 承接能力是否跟得上 |
| 投递 | `delivery_ledger` 各状态计数（按 state 分组）、`attempt` 分布、`reconcile_attempt` 分布 | `ambiguous` / `failed` 是必须告警的状态 |
| 隔离 | 跨 tenant predicate 拒绝数（如 `ErrTenantScope`） | 任何非零都需立刻查因 |

**SQL：当前投递台账状态分布（可直接巡检）**

```sql
SELECT tenant_id, state, count(*) AS segments, max(attempt) AS max_attempt, max(reconcile_attempt) AS max_reconcile
FROM delivery_ledger
GROUP BY tenant_id, state
ORDER BY tenant_id, state;
```

**SQL：当前是否有超期未确认的发送（疑似发送者崩溃）**

```sql
SELECT tenant_id, delivery_key, segment_no, claim_owner, claim_until, attempt, version, updated_at
FROM delivery_ledger
WHERE state = 'sending' AND claim_until <= now()
ORDER BY claim_until;
```

（`claim_until` 已过期仍处于 `sending`，说明原 owner 没能在 TTL 内完成；下一次 `ClaimDelivery` 会先把它改写为 `ambiguous / last_error_class='owner_lost'`。）

**SQL：会话是否发生过旧 owner 迟到写被拒（需要 DB 日志或审计侧配合）**

```sql
-- 会话最后有效 fence 与最后提交时间，用于确认接管后 fence 单调递增
SELECT tenant_id, agent_app_id, session_id, last_fence, version, next_input_seq, updated_at
FROM session_head
ORDER BY updated_at DESC
LIMIT 50;
```

**SQL：每输入是否只产生一条终态提交（"每个输入只产生一次可见最终回复"的服务端证据）**

```sql
SELECT tenant_id, agent_app_id, session_id, input_seq, count(*) AS terminal_commits
FROM session_commit
WHERE outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout')
GROUP BY tenant_id, agent_app_id, session_id, input_seq
HAVING count(*) > 1;
-- 期望：永远返回 0 行（终态唯一索引保证）
```

### 4.2 时间口径（不得夸大精度）

| 时刻 | 代表的事实 | **不代表**什么 |
|---|---|---|
| `t0` | 外部确认受害节点被终止 | 业务已恢复 |
| `t_connect` | 入口/连接所有者已恢复 | Worker 一定能提交 |
| `t_worker` | 执行者可处理或回收任务 | 用户已经看到回复 |
| `t_ready` | 所需业务链路均已 ready | 精确端到端 RTO |
| 收到 → 最终可见 | 一条请求的真实响应时间 | 故障期间未接收消息的恢复时间 |

采样间隔 `DELTA_POLL` 必须与结论一起记录。**不得宣称比观测频率更精确的 RTO。**

---

## 五、告警规则

### 5.1 分级

| 优先级 | 条件 | 含义与首查动作 |
|---|---|---|
| 🔴 P0 | 入口 `/readyz` 连续失败（无任何健康后端） | 全部节点 unready：新 callback 无法进入。先查 db/redis/malware 探针，再查节点 `/readyz` 与 `/statusz` |
| 🔴 P0 | 生产实例出现非计划重启（重启计数增加） | 违反"受害节点必须持续停止"的前提；立即确认是否为平台自动重建掩盖了接管 |
| 🔴 P0 | `delivery_ledger` 中 `ambiguous` 长时间不清零 | 用户可能已收到或未收到，**不可**盲目重发。进入 §7.4 对账流程 |
| 🟠 P1 | `failed` 状态新增 | 该回复片段已永久失败，用户看不到结果。需人工补发或按业务策略处理，并保留证据 |
| 🟠 P1 | `lease_lost` / `ErrLeaseLost` 激增 | 大量会话被判定失去执行权：检查 Redis 抖动、续租线程是否被 GC/阻塞、`LeaseTTL` 是否过小 |
| 🟠 P1 | stream oldest pending 年龄超阈值 | 承接能力不足或 worker 不健康。检查 reclaim 是否在跑、consumer 是否退出 |
| 🟠 P1 | `attempt` / `reconcile_attempt` 接近上限 | 即将进入 `failed`。优先查下游（企业微信）错误分类是否被正确识别 |
| 🟡 P2 | stale fence 拒绝异常升高 | 可能是正常的接管痕迹；异常升高则说明多节点持续争抢同一会话，检查是否需要会话级排队 |
| 🟡 P2 | 跨 tenant predicate 拒绝（`ErrTenantScope`）非零 | 任何非零都要查因：可能是配置错误或潜在越权尝试 |

### 5.2 不建议的做法

- ❌ 把 `/livez` 作为业务可用性 SLI：它恒 200，节点读不到数据库时依旧通过。
- ❌ 只监控容器 `Running` 状态：进程活着但 `db.Ping` / `redis.Ping` / malware 探针失败时，它应该被摘除。
- ❌ 用"最近有没有回复成功"作为唯一告警：故障期间本就没有新输入，这不是健康信号。

---

## 六、回滚约束

二进制回滚**只在 schema 保持向后兼容时**可行。以下操作一律禁止，它们会破坏可恢复性：

| 禁止操作 | 为什么 |
|---|---|
| 删除 `inbox` / `outbox` 行 | 破坏 Provider 去重边界与待发事实，重传将变成新输入或被永久丢弃 |
| 手工修改 `session_head.last_fence` | 使 fence 单调性失效，旧 owner 迟到写可能被接受 |
| 手工回退 `session_head.next_input_seq` | 会与 `session_commit` 终态唯一索引冲突，或让已终态输入被再次执行 |
| 清空 Redis pending stream | 未 ACK 的工作永久丢失（本应由 reclaim 接管） |
| 把 `delivery_ledger` 的 `sent` 改回 `pending` | 直接造成重复发送，破坏"每个输入只产生一次可见最终回复" |
| 把 `ambiguous` 直接改成 `sent` | 谎报投递成功，掩盖可能的重复或丢失 |
| 删除 `channel_ingress_candidate` 行 | 验签候选生命周期被打断，可能让进行中的 callback 无法完成可信提升 |

回滚后**必须保留**：`RUN_ID`、镜像 digest、`config_version`、owner 变化记录（`/statusz.process_start_id` 前后）、`delivery_ledger` 状态轨迹，供复盘与审计。

---

## 七、故障处置手册（分场景）

> 所有处置的前提：**不删除租约、不手工重新入队、不手工重绑 Bot、不先重启受害节点**。任何"手工修好"都不是自动接管。

### 7.1 场景 A：单个应用节点被非优雅终止（含计划内强杀演练）

**症状：** 某节点 `/readyz` 不再响应；入口 `/statusz` 中该后端 `healthy:false`；存活节点仍在服务。

**处置：**

1. 记录 `t0`（外部确认终止的时刻）与受害节点的 `process_start_id`（若此前有快照）。
2. 确认受害节点**持续停止**：`docker inspect --format '{{.State.Running}}' <container>` 应为 `false`，且间隔数秒复查仍为 `false`。
3. 确认入口仍 ready 且只剩存活后端：`curl -s http://127.0.0.1:58087/readyz`、`curl -s http://127.0.0.1:58087/statusz`。
4. 确认存活节点仍在 ready：`curl -s http://127.0.0.1:<node-port>/readyz`。
5. 记录 `t_connect` / `t_worker` / `t_ready`：分别对应入口就绪、存活节点 reclaim 到工作、业务链路整体就绪。
6. 观察接管证据：
   - Redis 中该会话的 lease key 值发生变化（owner 变为存活节点的 `webui-local-worker-<instance>-<process_start_id>`）；
   - `session_head.last_fence` 单调不减；
   - 若旧节点后续恢复，其提交会被拒（`stale fence` / SQLSTATE `40001`）。
7. 用真实 Bot 在原会话发一条不含代号的新消息，核对只回复本租户代号、只有一条最终回复。
8. **在接管验收完成前不得启动受害节点**。完成后再显式启动，并按 §7.3 判断重新加入是否正常。

**期望时限：** 由 `ReclaimInterval`（装配值 5s）+ `LeaseTTL`（装配值 30s）+ `ClaimTTL`（30s）共同决定。任意一项被调整，时限随之变化，结论必须重测。

> ⚠️ 必须引用**装配值**而非代码级兜底默认值：`worker.Consumer` 内部在 `LeaseTTL<=0` 时会退化为 5s、`ReclaimInterval<=0` 时退化为 1s，但 `webui-local` 角色显式传入 30s / 5s。混用两者会得出错误量级的故障检测时限。

### 7.2 场景 B：入口无任何健康后端

**症状：** 入口 `/readyz` 返回 503 `no ready callback backend`；`/statusz` 中所有 backend `healthy:false`；入口 `/livez` 仍 200。

**处置：**

1. 区分"入口进程故障"与"后端业务不可用"：`/livez` 200 + `/readyz` 503 ⇒ 入口进程正常，是后端问题。
2. 逐节点查 `/readyz`：503 时查其根因。`/readyz` 覆盖三项：`db.PingContext`、`redis.Ping`、malware 探针。三者任失败即 503。
3. 若两个节点都因同一共享依赖（如 PostgreSQL）失败，这**不在本包承诺范围**：本包只承诺单应用节点故障，共享依赖属同一故障域。此时按数据库/Redis 自身的恢复流程处置，并如实记录"这是依赖故障，不是应用实例接管"。
4. 若仅一个节点失败而另一个正常，但入口仍判两者都不可用，检查：
   - 入口探测周期是否被人为调大（`TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL`，允许范围 `[100ms, 1m]`）；
   - 后端地址是否正确（`TRPC_WECOM_HA_ENTRY_BACKENDS`）；
   - 网络/端口是否可达（入口探测的是 `<backend>/readyz`）。
5. 🔴 **不要**为了"让入口恢复"而直接重启受害节点——那会把"存活节点接管"与"重启自愈"两个结论混在一起。

### 7.3 场景 C：实例重新加入异常

**症状：** 显式启动受害节点后，出现旧回复被重发、连接振荡、backlog 增长、或 ownership 高频变更。

**处置：**

1. **首先验证是否换了进程身份**：对比重新加入前后的 `/statusz.process_start_id`。若**相同**，说明进程身份没有更新——这是 🔴 严重问题，因为旧 lease / consumer / delivery claim 可能被误认为仍属于同一执行者。
2. 若身份已更新，检查旧 owner 是否被正确废弃：
   - Redis 中旧 lease key 是否已过期或释放（不应长期残留且被旧进程续约）；
   - Redis consumer group 中旧 consumer 的 pending 是否可被新 consumer reclaim；
   - `delivery_ledger` 中旧 `claim_owner` 是否已随 `claim_until` 到期被置为 `ambiguous/owner_lost`。
3. 检查是否出现"旧 reply 被重发"：`delivery_ledger` 中同一 `(tenant_id, delivery_key, segment_no)` 不应出现多于一条最终 `sent`；`sent` 后只允许 ACK，不允许重发。
4. 检查 backlog 是否收敛：观察 stream pending 数量与 oldest pending 年龄是否回落。
5. 若振荡持续，先把它当作 🔴 停止发布，回退到单节点基线，再逐项排查身份/租约/claim 三处。

### 7.4 场景 D：投递处于 `ambiguous`（P4 窗口）

**症状：** `delivery_ledger.state='ambiguous'`，`last_error_class` 为 `response_lost`、`missing_provider_message_id` 或 `owner_lost`。含义是"上一次发送调用无法判断下游是否已受理"。

**处置（务必克制）：**

1. **禁止**直接重发。用户可能已经看到该条消息，重发会造成重复可见回复。
2. 判断下游能力：
   - 若企业微信侧支持按稳定客户端请求 ID（`delivery_ledger.client_request_id`，由 `(tenant_id, delivery_key, segment_no)` 稳定推导）查询或去重，则走对账：`ReconcileDelivery` 会把 `ReconciliationDelivered` 收敛为 `sent`，把 `ReconciliationNotDelivered` 收敛为 `retry_wait` 并打 `last_error_class='reconciled_not_delivered'`，未知则继续 defer。
   - 若下游**不支持**去重或查询，则系统只能承诺**至少一次投递尝试**，不能承诺用户绝对只看到一条。此时按明确的人工策略处理（查证 + 记录 + 必要时人工告知），并在对外表述中如实说明，**不得**改写成"恰好一次"。
3. 观察 `reconcile_attempt`：达到上限（默认 8）会进入 `failed / reconcile_exhausted` 并告警 🟠；进入该状态说明系统已诚实放弃自动收敛，需要人工介入。
4. 全过程保留证据：`delivery_key`、`segment_no`、`client_request_id`、`attempt`、`reconcile_attempt`、`last_error_class` 时间序列。

### 7.5 场景 E：会话上下文回忆错误（串租户或丢历史）

**症状：** Bot A 回复了 Tenant B 的代号，或答不出此前写入的代号。

**处置（按证据链倒查，不要先猜模型）：**

1. **确认是不是路由问题**：查该消息的 `inbox` 行 `(tenant_id, channel, external_account_id, external_message_id)` 与 `channel_binding_id` 归属。若 tenant 本身就错了 → 是 binding/route 配置或验签问题（不变量 I1 失效），与模型无关。
2. **确认是不是会话键问题**：查 `session_head` / `session_commit` 的 `(tenant_id, agent_app_id, session_id)` 三元组。若 session_id 变了 → 是上游 chat 标识不稳定（例如客户端换了 chat），不是服务端恢复问题。
3. **确认历史是否真的提交过**：查 `session_commit` 是否有对应 `input_seq` 的终态提交。没有终态提交 = 当时那轮根本没落库（可能死在 commit 前），属预期行为，不是"丢历史"。
4. **确认是否读了别租户的历史**：任何 repository 查询都必须同时带 tenant 与 session/binding predicate。若发现缺 tenant 条件的查询 → 🔴 立即修，并复查历史上是否发生过跨租户读取。
5. **最后才是模型层**：只有在 1–4 全部排除后，才检查 prompt 组装与模型上下文构造。

🔴 注意：**模型"自称属于某个 tenant"不具有证据价值**。判据只能是 binding / tenant / session / delivery 的一致关联。

### 7.6 场景 F：重复的最终回复

**症状：** 用户在同一个聊天里看到两条相同语义的最终回复。

**处置：**

1. 查 `delivery_ledger`：同一 `(tenant_id, delivery_key, segment_no)` 是否存在多条终态记录。设计上主键唯一，不应出现；若出现，说明绕过 Ledger 或主键被破坏 → 🔴 严重。
2. 查是否属于**分片**：若原始回复超过适配器单条上限，会被切成多个 segment（`segment_count > 1`），每个 segment 一行——这不是重复，是正常分片。核对 `segment_count` 与客户端看到的条数。
3. 查 `session_commit`：同一 `input_seq` 是否存在多条终态提交。终态唯一索引应保证不会；若出现，说明索引被删或有旁路写入。
4. 查是否**两个 owner 同时发送**：Ledger 的条件更新（`state='sending' AND version=... AND claim_owner=... AND client_request_id=...`）应保证只有一个成功。若两方都成功，检查 SQL 是否被改写削弱了条件。
5. 区分"最终回复"与"流式/进度消息"：进度提示、占位消息不计入最终回复计数，但应单独观测其是否被误当最终回复重复发送。

---

## 八、日常巡检清单

| 频率 | 检查项 | 通过判据 |
|---|---|---|
| 每次发布前 | §1.1 配置门禁三项命令 | 全部通过；两组 Bot 元组不同 |
| 每日 | `delivery_ledger` 状态分布（§4.1 SQL） | 无长期 `sending`、无增长中的 `ambiguous`、无新增 `failed` |
| 每日 | 入口 `/statusz` | 至少一个 backend `healthy:true` |
| 每日 | 各节点 `/statusz.owners` | owner 名含各自 `process_start_id`；无重复 |
| 每周 | stream pending 与 oldest pending 趋势 | 稳定，无单调增长 |
| 每周 | `session_commit` 终态唯一性检查（§4.1 SQL） | 返回 0 行 |
| 每次演练后 | `session_head.last_fence` 单调性 | 接管后只增不减 |
| 每月 | 单节点接管演练（`scripts/e2e/wecom-ha-failover.sh`） | 见 `testing-and-acceptance.md` 判定表 |
| 每季度 | 双 Bot 真实原会话验收（外部） | `TRPC_WECOM_REAL_ACCEPTANCE=recorded` |

---

## 九、容量与故障冗余测算

| 维度 | 要求 | 说明 |
|---|---|---|
| 单实例容量 | 应不低于**峰值全量**（而非峰值 / 2） | 任一实例宕机后，存活实例需独自承接两个 Bot 全部负载 |
| 模型并发 | 按峰值并发 + 接管期间的重复执行预留 | P2 接管会**重跑模型**，接管窗口内模型调用量会高于稳态 |
| PostgreSQL 连接池 | 按"两节点 + 接管期额外续租/提交"估算 | 续租是高频小事务；池耗尽会表现为 `ErrLeaseLost` 激增 |
| Redis 连接与内存 | 覆盖 stream pending 峰值 + 每会话一个 lease key + 一个 fence key | fence key 不设 TTL，随会话数增长但单值极小 |
| 文件描述符 | 按 stream 消费 + 投递并发 + 入口转发连接估算 | fd 耗尽会先表现为消费停滞、backlog 增长 |

> ⚠️ 低流量演练只能证明**可用性**，不能外推为**容量承诺**。报告容量结论必须基于容量测算与压测，而非一次强杀演练的通过。

---

## 十、变更影响矩阵（改一处之前先看这里）

| 变更 | 直接影响 | 必须同步 |
|---|---|---|
| 调小 `LeaseTTL` | 故障检测更快，但续租/GC 抖动更易触发误失权 | 同步 `RenewInterval` 与告警阈值；重测接管时限 |
| 调大 `ReclaimInterval` | 接管更慢 | 重测 RTO；更新验收时限 |
| 调 `ClaimTTL` | 发送者崩溃后接管更快/更慢 | 同步 `ClaimRenewInterval`；确保 `< ClaimTTL` |
| 新增 `channel` / 第二 Bot 之外的 Bot | 路由与凭据面扩大 | 同步 binding/route 初始化、`(Corp,Agent)` 唯一性校验、回调登记 |
| 改 `delivery` 状态机取值 | 影响所有节点对新状态的解释 | 必须两阶段发布：先发布"能安全遍历未知状态"的 consumer，再启用新状态 |
| 改 `commit_turn` 的校验条件 | 直接关系"一次有效提交" | 🔴 任何弱化 `p_fence < last_fence` 或 expected_version 的改动都必须重做全量接管验收 |
| 改 owner 命名公式 | 影响 lease/claim/日志的可归因性 | 必须保留 `InstanceID` + `ProcessStartID` 两个部分，缺一即不可追责 |

---

## 十一、相关文档

- 机制与因果链：`FULL-GUIDE.md`
- 架构 / 数据模型 / 状态机：`design.md`
- 逐模块实现走读：`implementation-walkthrough.md`
- 关键代码摘录：`CODE-APPENDIX.md`
- 从零复现材料（DDL / Lua / 接口 / 算法 / 配置 / 启动顺序 / 验证清单）：`REFERENCE-IMPLEMENTATION.md`
- 测试方案与通过判定：`testing-and-acceptance.md`
