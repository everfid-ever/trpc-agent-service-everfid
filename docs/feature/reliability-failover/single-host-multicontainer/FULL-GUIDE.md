# 单主机多容器实例可用性：完整实现说明

## 1. 本包验证什么

本包验证同一主机上的两个独立应用实例能共同服务两个租户；其中一个实例被非优雅终止后，另一个实例仍能通过**原企业微信 callback 地址**继续服务；故障实例以**新进程身份**重新加入后不会带回过期所有权或旧回复；随后反向故障也成立。

它验证的是**应用进程/容器故障**，不证明宿主机、电源、Docker daemon、共享磁盘、数据库或整机网络故障。所有这些组件仍在同一故障域，跨主机高可用需要另行设计和验证。

## 2. 什么才是真正的双实例

| 必须不同 | 可以共享 | 原因 |
|---|---|---|
| 部署槽位 `TRPC_WEBUI_LOCAL_INSTANCE_ID`、进程启动 `process_start_id`、owner/consumer 名称、宿主机观测端口、容器级本地暂态 | 镜像 digest、配置版本、`TRPC_POSTGRES_DSN`、`TRPC_REDIS_ADDRESS`、受支持的只读资源 | 身份与本地暂态必须隔离；业务事实必须共享 |

两套完全独立的数据库和队列**不叫接管**，因为 N2 根本看不到 N1 已接收的会话和任务；共享一个不支持并发写的本地文件也**不叫共享状态**，因为它可能损坏或竞争写。正确共享的是支持事务/协调语义的后端（PostgreSQL 权威态 + Redis 队列与协调）。两个实例通过 `TRPC_WEBUI_LOCAL_INSTANCE_ID`（`wecom-ha-node-a` / `wecom-ha-node-b`）区分槽位，通过 `process_start_id`（每次启动新 16 字符 hex）区分进程 incarnation。

## 3. 为什么 callback 入口必须独立

如果企业微信公开 URL 直接指向某个应用节点（例如 `wecom-ha-node-a:8080`），那么该节点死亡时，即便 N2 可以回收任务，新 callback 仍无法到达服务——企业微信只会把消息发到登记的固定地址。于是系统设置一个**稳定入口** `wecom-ha-entry`：

- 企业微信只认识入口地址（默认宿主机 `58087`）。
- 入口周期性（默认 `1s`）检查 N1/N2 的 `/readyz`。
- 只将 callback 转发给 ready 后端；后端不健康则摘除。
- 公开 origin 在故障前后保持一致，两个 Bot 共用同一 origin，只用 durable route key（`local-wecom` / `local-wecom-secondary`）区分。

```text
企业微信固定 URL (origin)
   → wecom-ha-entry (稳定入口, 唯一对外端口)
      → wecom-ha-node-a (ready 时可选)
      → wecom-ha-node-b (ready 时可选)
```

入口**不是业务网关的替代品**。它不决定 tenant、不保存会话、不做业务幂等，只做有限且可观测的健康后端选择。callback 在尚未获得业务响应时遇到连接错误、502、503 或 504，可尝试另一个健康后端；之后的去重仍依赖后端 durable Inbox（`claim_inbox` / `prepare_dispatch` 唯一键收敛）。详见 `CODE-APPENDIX.md` 第 1–3 节。

### 3.1 术语澄清："接管 Bot 连接"在回调式架构里指什么

需求表述里出现"接管 **Bot 连接**"。这一条**必须先澄清，否则会被误解**，因为不同 IM 平台的接入方式不同：

| 平台接入方式 | 是否存在需要"接管"的长连接 | "连接接管"的含义 |
|---|---|---|
| 长连接 / WebSocket 模式（如部分平台的长连接机器人） | 有：每条连接绑定一个进程，进程死亡会导致连接断开、消息丢失 | 需要由存活进程**重新建立连接**并接管会话 |
| **本系统采用的方式：HTTP 回调（webhook）** | **没有**：企业微信在需要时主动向登记 URL 发起 HTTP 请求，服务端不持有长连接 | "连接接管"= **公开回调地址不变 + 新回调被转给健康实例** |

**本系统全渠道均为回调式**（WeCom / 飞书 / WebUI / 通用 httpcallback 都走 HTTP 回调，代码中不存在 WebSocket 或长连接消费者）。因此本包所说的"接管 Bot 连接"，**不是**"接管一条 TCP/WebSocket 长连接"，而是三件事的组合：

1. **公开地址不变**：两个 Bot 在企业微信控制台登记的 URL 始终指向 `wecom-ha-entry`，实例生死不影响这个地址；
2. **新回调仍能到达服务**：入口按 `/readyz` 摘除受害实例，把新 callback 转给存活实例——这就是"连接（入口）接管"的全部内容；
3. **在途工作仍能被接手**：已进入 Redis 的未确认任务，由存活实例的消费者组 reclaim 接管（这才是"Worker 接管"，见 §7）。

**为什么不需要"重建连接"这一步：** 因为服务端从未持有连接。企业微信每次投递都是一次独立的 HTTP 请求，谁处理都行；处理权由 **lease + fence** 决定，而不是由"谁握着连接"决定。

**推论（对验收很重要）：** 不要去找"连接所有权转移"的代码或证据——它不存在。可核查的证据是：① 入口 `/statusz` 的健康集合变化；② 受害实例被摘除后新 callback 仍返回业务响应；③ 存活实例取得了更高的 fence 并完成提交。这三条共同等价于"连接接管成立"。

> 相关实现（可选核对）：`cmd/trpc-service/wecom_ha_entry_role.go`（入口探测与转发）、`trpcservice/channels/contract/adapter.go`（渠道适配契约，回调式）、`trpcservice/channels/` 目录下全部渠道实现（`httpcallback` / `wecom` / `feishu` / `webui`，均为回调式，无 WebSocket 或长连接消费者）。

## 4. live 与 ready 的差别

进程活着**不表示**可以处理业务。

- `/livez`：恒返回 200，仅表明进程未退出。
- `/readyz`：要求 `db.PingContext`、`redis.Ping` 与 `malware.Probe` 任一失败即 503。它代表"业务依赖齐备、可以服务"。

N1 可能仍在运行但已断开数据库，此时入口必须把它摘除而不是继续转发用户消息。入口自身也区分这两种状态：入口进程正常但没有任何 ready 后端时，它仍 `/livez` 200 但 `/readyz` 503。这种区别使监控和排障能知道问题在入口本身还是后端业务能力。状态接口契约见 `REFERENCE-IMPLEMENTATION.md` 第 6 节。

## 5. 实例身份与重新加入

实例由"部署槽位 + 本次进程启动身份"共同表示：

- 部署槽位：固定字符串 `wecom-ha-node-a` / `wecom-ha-node-b`，由 `TRPC_WEBUI_LOCAL_INSTANCE_ID` 注入。
- 进程启动身份：`process_start_id`，每次进程真正启动时由 `newWebUILocalProcessStartID()` 生成（8 字节 `crypto/rand` → 16 字符 hex）。**不可配置，自动生成**。

owner 与 consumer 名称都携带这两部分，命名规则：

```text
webui-local-<component>-<instance-id>-<process-start-id>
例：webui-local-worker-wecom-ha-node-a-9f3c1a2b4d5e6f07
```

组件 `component` ∈ {`worker`, `preprocess`, `dispatch-relay`, `reply-relay`, `wakeup-relay`, `wakeup`, `delivery`}。

```text
N1 首次启动：owner = webui-local-worker-wecom-ha-node-a-9f3c...
N1 被杀后重新加入：owner = webui-local-worker-wecom-ha-node-a-1b2c...   (process_start_id 不同)
```

这样租约、Delivery claim、日志和消费者 pending 可以区分旧进程与新进程。**重新加入不等于"旧 owner 复活"**：旧进程持有的 Redis lease 会随 TTL 过期或被 N2 reclaim；新进程用新的 `process_start_id` 从共享状态中观察，只回收已经过期或可重试的工作，绝不会把旧 owner 的未提交结果当作自己的。判定方法见第 9 节 `process_start_id` 前后对比。

## 6. 正常双实例如何协作

N1/N2 都有处理两个 tenant 的配置和凭据（主/次 Bot 均启用）。它们可以并发处理不同会话；同一会话由 Redis lease 串行，迟到写入由 PostgreSQL fence 拒绝（`commit_turn` 的 `p_fence < last_fence` 报错 `stale fence`）。两者都可扫描 Outbox、消费 Work/Reply Stream、尝试 Delivery claim；但数据库唯一约束与 ledger 条件更新保证同一事实只有一个有效提交或投递 owner。

启动后必须先观察稳定窗口：没有持续连接互踢、owner 高频变更、队列积压或重启。只有健康基线稳定，强杀后的变化才可归因于故障接管，而不是部署本身不稳定。

## 7. N1 强杀后的时序

```text
t0      docker kill --signal=KILL wecom-ha-node-a
        ├─ N1 进程立即消失，停止续租 / 消费 / ACK / finish delivery
        └─ 记录 t0（外部确认受害节点被终止的时刻，不代表业务已恢复）
t_probe 入口下一轮 probe 命中 N1 /readyz 失败 → 从 healthyBackends() 摘除 N1
        （public origin 不变，仍只转发 N2）
t_connect 入口/连接所有者恢复：N2 成为唯一健康后端，callback 可达
t_worker N2 在 LeaseTTL(装配值 30s)/ReclaimInterval(装配值 5s) 条件满足后领取 N1 遗留 work/reply
t_ready  业务链路整体 ready（N2 可处理 + 用户可见回复）
```

1. 关闭 N1 自动重启和自动替换（`restart: "no"`）；确认 N2 与共享依赖正常。
2. 记录 `t0`，对 N1 执行非优雅终止；确认 N1 持续 stopped（`State.Running=false`，且 sleep 后仍 false）。
3. 入口探测失败后将 N1 从健康后端集合移除；入口仍有 N2，因此公开 URL 不变。
4. N1 停止续租、消费和 ACK；N2 可在 TTL/reclaim 条件满足后领取未完成工作。
5. N2 获取新的 session fence，提交时拒绝 N1 旧结果（`stale fence`）；N2 继续发送或恢复发送。
6. 两个 Bot 在原会话发新消息，核对 tenant、历史代号、唯一最终回复和投递记录。

> 故障期间若需证明**原在途消息**完成（而非只验证新消息成功），必须再按在途任务接管包命中 P1–P4 暂停点；仅验证新消息成功不等于原任务完成。本包聚焦"实例级接管"，在途语义见 `../inflight-task-takeover`。

## 8. 重新加入与反向故障

接管业务验收完成后才允许显式启动 N1：

```bash
docker compose --profile wecom-ha-local start wecom-ha-node-a
```

N1 生成新的进程身份、通过 readiness 后重新成为候选实例。随后观察完整稳定窗口：不应发送旧 reply、不应把 stale fence 写回、不应造成连接振荡或 backlog 增长。判定新身份的方法是比对 `process_start_id` 与基线快照不同。

再重建双实例基线，强杀 N2。此轮必须独立记录实际连接/worker/delivery owner，因为两个实例在上一轮中承担的职责可能不同。对称故障才能说明系统不是只对一个固定主节点有效。

## 9. 演练证据和恢复时间

| 证据 | 来源 | 证明的内容 |
|---|---|---|
| 容器状态与重启计数 | `docker inspect --format '{{.State.Running}}'` | 受害实例是否持续停止，是否被 Docker 自动重建 |
| 入口健康集合 | `GET /statusz` 的 `backends[].healthy` | callback 是否仍有可用后端 |
| `process_start_id` | `GET /statusz` 的 `process_start_id` | 重新加入是否产生新 owner |
| owner 映射 | `GET /statusz` 的 `owners` | worker/relay/delivery 实际由谁承担 |
| consumer/lease/fence/claim | Redis / PostgreSQL 记录 | 谁接管了任务或投递职责 |
| tenant/session/request/delivery 关联 | 共享库查询 | 两个 Bot 是否仍在正确原会话 |
| 客户端最终回复 | 真实 Bot 会话或 delivery ledger `sent` 行 | 用户是否在预设时间内看到唯一结果 |

时间口径（采样间隔 `DELTA_POLL` 必须随结论记录，不得宣称比采样更精确的 RTO）：

| 时刻 | 含义 | 不代表 |
|---|---|---|
| `t0` | 外部确认受害节点被终止 | 业务已恢复 |
| `t_connect` | 入口/连接所有者恢复 | worker 一定能提交 |
| `t_worker` | 执行者可处理或回收任务 | 用户已看到回复 |
| `t_ready` | 业务链路整体 ready | 精确端到端 RTO |

## 10. 不能混合的四类恢复机制

| 子用例 | 受害节点状态 | 可以得出的结论 |
|---|---|---|
| 存活实例接管 | 受害实例**持续停止** | N2 是否能自行承接 N1 职责（本包主验证项） |
| 自动重启/替换 | 平台允许重建受害实例 | 容器重建时间与恢复行为（独立机制，不混入本包） |
| 计划内优雅停止 | 实例先摘除、排空、退出 | 维护停机时是否安全 drain |
| 共享依赖故障 | 应用实例可仍在，关键依赖停止 | 影响范围、降级和恢复一致性 |

把它们混在一个结果里会导致错误归因：例如"Docker 把 N1 拉起来了"不能证明 N2 曾经接管。因此 `restart: "no"` 是验收关键——强杀后运行时不得暗中重建 N1 来掩盖 N2 是否真的接管。自动重启是另一个独立场景，应在单独的子用例中验证。

## 11. 通过标准与边界

### 通过标准

- 双实例可稳定共存；共享状态按 tenant 隔离。
- 单实例持续停止时，另一实例无需人工修复即可服务两个 Bot（公开 callback 经原入口进入健康实例，用户无需改 URL）。
- 重新加入与反向故障不产生旧 reply、重复 final、连接抖动或不可恢复 backlog。
- `process_start_id` 在重新加入前后不同（新 owner）。
- 入口 `/statusz` 在每次故障后只保留存活健康后端。

### 边界（不承诺）

- 不承诺跨主机 / 整机断电 / Docker daemon / 共享磁盘 / 共享 PostgreSQL·Redis 故障下的可用性（同为单一故障域）。
- 不承诺模型调用次数恒为 1；P2 接管可重跑模型。
- 不承诺下游无法按 `client_request_id` 去重或查询时 P4 的绝对 exactly-once。
- 不把 Docker 自动重启当作存活节点接管。
- 不把健康恢复 / 连接恢复 / 队列恢复单独当作完整业务 RTO。

## 12. 两个计数必须写清（避免误读）

| 计数 | 含义 | 可被接管改变吗 |
|---|---|---|
| 模型调用次数 | P2 执行时模型推理次数，允许 >1 | 是，接管可重跑模型 |
| 任务尝试次数 | `execution_record.park_attempt` / `delivery_ledger.attempt` | 是，reclaim/retry 会增长 |
| 最终回复次数 | `delivery_ledger` 每 segment 一行，`state='sent'` 后不可重发 | 否，sent 后稳定，靠幂等约束不重复 |

## 13. 为什么必须共享权威态而非本地文件

"两个实例"的含义常被误读为"起两个进程"。真正的多实例接管要求：

- **业务事实共享**：已接收的会话、任务、待发回复必须落在 PostgreSQL（权威态）与 Redis（队列/协调），这样 N2 才能看到 N1 已接收的工作。
- **本地暂态隔离**：每个容器的临时目录、进程内存、consumer 状态相互独立；这些不能充当"共享状态"，因为并发写会损坏或竞争。
- **协调语义**：Redis lease + 递增 fence 保证同一会话同一时刻只有一个有效提交者；PostgreSQL 唯一约束 + ledger 条件更新保证同一事实只有一个有效 owner。

如果两个实例各自连一套独立 PostgreSQL，那叫"两个独立系统"，N2 根本看不到 N1 的工作，谈不上接管。因此本包的核心不是"多启动一个容器"，而是"共享权威态 + 独立身份 + 健康感知入口"。

## 14. lease 与 fence 如何阻止旧 owner 提交

同一会话的提交受两层保护（详见 `REFERENCE-IMPLEMENTATION.md` 的会话与租约章节）：

1. **Redis lease**：`acquire` 成功写入 `workerID|leaseID|fence`；续租失败触发 `markLeaseLost()`，执行 context 取消。N1 被 SIGKILL 后无法续租，lease 在 `LeaseTTL(30s)` 后过期。
2. **PostgreSQL fence**：`commit_turn` 要求 `p_fence >= head.last_fence`，否则报 `stale fence`（`40001`）。N2 在 reclaim 时 `EnsureFenceAtLeast` 校准并取到更高 fence；N1 即使"僵尸"进程试图提交旧结果，也会被 fence 拒绝。

```text
N1 lease fence=5  → SIGKILL → lease 过期
N2 reclaim → EnsureFenceAtLeast(>=5) → fence=6
N1 旧进程（若残留）commit fence=5 → commit_turn 拒绝: stale fence
```

这正是"重新加入 ≠ 旧 owner 复活"的存储层保证，与 `process_start_id` 的应用层身份互为印证。

## 15. 故障恢复时间预算（基于默认参数）

| 阶段 | 默认耗时上界 | 依据 |
|---|---|---|
| 入口探测到 N1 `/readyz` 失败 | ≤ `ProbeInterval`(1s) + probe 超时(3s) | `probe()` 周期 + 3s context |
| N1 lease 过期可被 reclaim | ≤ `LeaseTTL`(30s) | worker `LeaseTTL` |
| N2 reclaim 扫描间隔 | ≤ `ReclaimInterval`(5s) | worker/relay `ReclaimInterval` |
| N2 完成 drain（若主动） | ≤ `DrainTimeout`(30s) | worker `DrainTimeout` |
| delivery claim 过期 | ≤ `ClaimTTL`(30s) | delivery `ClaimTTL` |

> 这些是**上界**，实际通常更短。但结论中必须记录采样间隔 `DELTA_POLL`，不得宣称比采样更精确的 RTO（见本包 `release-and-operations.md` 的观测口径）。

## 16. 与在途任务接管（P1–P4）的边界

本包只验证"实例级接管"——即 N2 能继续服务、承接 pending。它**不自动证明**故障瞬间正在执行的**那条具体任务**的语义完整性。若需要，配合 `inflight-task-takeover` 包：

- P1：durable job 已存在、尚未成为 execution（dispatch 之前）。
- P2：模型/工具已跑完、尚未 commit。
- P3：已 claim delivery、尚未真正发送。
- P4：adapter 已返回 provider receipt、尚未置 `sent`。

演练时在 Compose 中透传 `TRPC_INFLIGHT_TEST_BARRIER=p2` 等，`node-a`/`node-b` 会在对应点阻塞；命中后 SIGKILL victim，释放 barrier，观察 survivor 是否收敛。本包主脚本不默认开启 barrier（生产安全默认空），但它使用的同一套 owner/lease/ledger 机制正是 P1–P4 的底座。

## 17. 常见误解

| 误解 | 正解 |
|---|---|
| "再起一个容器就叫高可用" | 必须共享权威态、独立身份、健康感知入口 |
| "入口重启一下就行" | 入口独立于应用节点，强杀应用节点不改公开地址 |
| "Docker 自动重启 N1 也算接管" | 自动重启是另一机制，`restart:"no"` 禁用，否则结论无效 |
| "模型只跑一次" | P2 接管可重跑模型；最终回复靠 ledger 幂等不重复 |
| "ready 就是 live" | live 仅进程活；依赖断了 live 仍 200 但 ready 503 |
| "重新加入是旧进程继续" | 新 `process_start_id`，旧 lease 过期，旧结果被 fence 拒绝 |

## 18. 端到端心智模型

```text
[企业微信] ──callback──► [entry :58087] ──healthy 轮转──► [node-a :8080] ──┐
                                                              [node-b :8080] ─┤
                                                                              ▼
                                                    PostgreSQL(权威态) + Redis(队列/lease/fence)
                                                                              │
                              强杀 node-a ⇒ entry 摘除 a ⇒ node-b reclaim ⇒ 用户无感
                              重启 node-a ⇒ 新 process_start_id ⇒ 稳定窗口 ⇒ 反向故障验证
```

> 完整实现代码见 `CODE-APPENDIX.md`；完整可照抄的 Compose 与入口实现见 `REFERENCE-IMPLEMENTATION.md`；逐条验收判定见 `testing-and-acceptance.md`。

## 19. 持续负载下的三个退化模式（机制解释）

本包不仅要求"强杀后能接管"，还要求"持续负载下不退化为假可用"。三种退化模式各自的机制根因如下：

### 19.1 连接振荡（flapping）

**表现：** 入口 `/statusz` 中某 backend 的 `healthy` 在两值之间反复翻转；consumer 归属频繁变化。

**机制根因：**

| 根因 | 机制 | 判据 |
|---|---|---|
| 节点反复重启 | `process_start_id` 短时多次变化 | `/statusz.process_start_id` 前后对比 |
| 节点 ready 判定抖动 | `/readyz` 依赖（db/redis/malware 探针）间歇失败 | 直接 `curl /readyz` 观察 503 与 200 交替 |
| 探测周期过短 | `TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL` 被调小，探针自身压垮后端 | 调回 `1s` 后是否消失 |
| 端口/地址配置错误 | 探测目标不可达，恒判不健康 | 手工 `curl <backend>/readyz` 验证 |

**为什么必须单独观测：** 振荡不会让服务立刻不可用，但会让"接管是否发生"无法归因——`healthy` 本来就在不停变，强杀造成的那一次变化淹没在噪声里。

### 19.2 任务积压（backlog）

**表现：** Redis stream pending 数或 oldest pending 年龄单调上升；`inbox` 非终态行数持续增长。

**机制根因：**

| 根因 | 机制 | 判据 |
|---|---|---|
| 单实例容量不足 | 接管后需独自承接两 Bot 全量 | 两实例同时运行时积压不涨，单实例时涨 → 容量问题 |
| 续租失败导致重复执行 | `lease_lost` 使同一会话被反复重跑，挤占吞吐 | `ErrLeaseLost` 计数与积压同涨 |
| 投递重试风暴 | `delivery_ledger` 大量 `retry_wait`，下游持续拒绝 | `attempt` 分布右移、`last_error_class='retryable'` 占比高 |
| reclaim 未运行 | 消费循环退出或异常 | `/statusz.owners` 有 owner，但 pending 不下降 |
| 连接池/fd 耗尽 | 续租是高频小事务；fd 耗尽早表现为消费停滞 | DB 连接数、进程 fd 数 |

**判据纪律：** 积压增长时**先区分**是容量问题还是故障接管失败。若接管方一直 ready 且积压能回落，是容量问题；若 pending 永久不下降，才是接管链路问题。

### 19.3 重复回复（duplicate reply）

**表现：** 用户在同一聊天看到两条同语义最终回复。

**机制根因与判据：**

| 场景 | 是否真重复 | 判据 |
|---|---|---|
| 回复过长被分片 | ❌ 正常 | `delivery_ledger.segment_count > 1`，客户端条数与 `segment_count` 一致 |
| 两个 owner 同时发送 | ✅ 真重复 | 同一 `(tenant_id, delivery_key, segment_no)` 出现多次 `sending` 且都成功 |
| claim 条件被削弱 | ✅ 真重复 | SQL 中 `state='sending' AND version=? AND claim_owner=? AND client_request_id=?` 缺项 |
| 重加入后旧 reply 被重发 | ✅ 真重复 | 旧 `claim_owner`（含旧 `process_start_id`）在重加入后仍推进 `attempt` |
| 客户端本地重复渲染 | ❌ 非服务端问题 | 服务端 ledger 只有一条 `sent` |

**核心保护：** `delivery_ledger` 的 claim 是四重条件更新（state + version + claim_owner + client_request_id），`sent` 之后只允许 ACK，不允许重发。

---

## 20. 部署最小清单与"重新加入"的正确操作序列

### 20.1 部署最小清单

| 项 | 要求 | 检查方式 |
|---|---|---|
| 共享 PostgreSQL | 两节点同一 `TRPC_POSTGRES_DSN` | 手工连库确认 binding/session 可见 |
| 共享 Redis | 两节点同一 `TRPC_REDIS_ADDRESS` | 确认 stream group 与 lease key 唯一 |
| 一次性 bootstrap | `wecom-ha-bootstrap` 成功退出（fixture 初始化） | `compose ps` 状态为 Exited(0) |
| 节点身份 | `TRPC_WEBUI_LOCAL_INSTANCE_ID` 两节点不同 | `compose config` 渲染后比对 |
| 自动重启 | 两节点与入口均 `restart: "no"` | `compose config` 渲染后确认 |
| 入口后端 | ≥2 个合规 `http` 地址 | `/statusz.backends` 长度为 2 |
| 回调登记 | 只在入口 public origin 登记 | `/callbacks/wecom` 可达；节点端口未登记 |
| 两组 Bot 凭据 | 5 个主 Bot + 5 个次 Bot 变量齐全且元组不同 | 启动日志无 `incomplete` / `incompatible` |

### 20.2 重新加入的正确操作序列

重新加入是**受控操作**，不是"让容器自己起来"：

```text
① 确认本轮接管验收已完成（存活实例已承接职责，且证据已采集）
② 确认受害容器当前 State.Running=false，且未被平台重建
③ 显式启动：docker compose ... start wecom-ha-node-a
④ 等该节点 /readyz 返回 200
⑤ 从 /statusz 读取新的 process_start_id，断言与上一轮不同   ← 关键
⑥ 等入口 /statusz 中该 backend healthy=true
⑦ 观察完整稳定窗口（本包脚本默认 TRPC_SINGLE_HOST_STABILITY_SECONDS=5，建议生产观察更长）
⑧ 在窗口内核对：无旧 reply 重发、无 ownership 高频变更、pending 不增长
⑨ 重建双实例基线后，再做反向强杀
```

**为什么第 ⑤ 步是关键：** 若 `process_start_id` 未变，说明该进程身份被复用——旧 lease、旧 consumer pending、旧 delivery claim 都可能被误认为仍属于同一执行者，重加入的语义就不再是"新执行者从共享状态观察"，而是"旧 owner 复活"，这正是本包要排除的情况。

**反向强杀的注意点：** 两个实例在上一轮中承担的职责可能不同（谁持有 lease、谁在发送都可能变）。因此反向故障**必须独立记录**实际连接/worker/delivery owner，不能沿用上一轮的假设。

---

## 21. 结论表述模板（用于对外汇报）

> 在单主机部署两个独立应用实例（`wecom-ha-node-a` / `wecom-ha-node-b`），通过独立健康探测入口 `wecom-ha-entry` 维持单一公开 callback 地址；两实例共享 PostgreSQL/Redis/Qdrant/ClamAV，但拥有独立的部署槽位身份与每次启动随机生成的进程身份。对其中一个实例执行非优雅终止（SIGKILL）后，入口在探测周期内将其摘除，存活实例在租约 TTL 与 reclaim 周期内接管 Bot 连接、任务执行与回复投递职责；受害实例保持停止，未由容器运行时自动重建。显式重新加入后进程身份更新，稳定窗口内未出现旧回复重发、所有权抖动或积压增长；随后对另一实例实施反向强杀，同样成立。
>
> **明确边界：** 本结论仅覆盖**单主机应用实例故障**。宿主机、Docker daemon、共享磁盘与共享 PostgreSQL/Redis 仍处于同一故障域，跨主机高可用需另行设计与验证。真实企业微信原会话与最终可见回复由外部账户采集（`TRPC_WECOM_REAL_ACCEPTANCE`），本包自动化部分只验证代码侧生命周期与入口接管事实。
