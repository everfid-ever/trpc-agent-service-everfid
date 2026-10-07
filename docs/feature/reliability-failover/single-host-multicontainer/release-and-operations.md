# 单主机多容器实例可用性 — 发布与运维

> 本文覆盖配置、部署、发布顺序、观测指标、回滚约束与故障处置手册。所有服务名、端口和环境变量均与实现代码一致（可选核对：`deploy/compose/docker-compose.local.yml`、`cmd/trpc-service/wecom_ha_entry_role.go`、`cmd/trpc-service/webui_local_role.go`）。

## 一、资源与配置门禁

单实例必须有处理两个 Bot 计划负载的 CPU/内存、模型并发、PostgreSQL pool、Redis 连接和文件描述符预算。低流量演练只能证明**可用性**，不可扩展为**容量承诺**。上线前按预估峰值（两 Bot 合并 QPS、平均任务时长、模型并发）做容量测算：

- 任一实例宕机时，存活实例需独自承接两 Bot 全部负载；因此单实例容量应不低于峰值 / 1（而非峰值 / 2）。
- `worker` 默认 `Shards [0,1,2,3]`、`ReclaimInterval 5s`、`ReclaimLimit 100`、`LeaseTTL 30s`；`delivery` 默认 `ClaimTTL 30s`、`MaxAttempts 8`、`MaxReconcileAttempts 8`。容量不足会表现为 reclaim 跟不上、积压增长（见 `testing-and-acceptance.md` 第 7 节）。
- 入口 `RequestLimit=2MiB`、探测周期默认 `1s`；不要把 `TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL` 调得过小（拖垮后端）或过大（延长故障窗口）。

## 二、部署与发布顺序

1. **共享依赖先行**：postgres / redis / qdrant / clamav / otel-collector 必须 healthy 后再启动应用（`depends_on` 已声明：`postgres`/`redis` `service_healthy`、`qdrant` `service_started`、`clamav` `service_healthy`）。
2. **一次性 bootstrap**：`wecom-ha-bootstrap`（`webui-local-bootstrap`）必须 `service_completed_successfully` 后，节点才可启动。它初始化共享 tenant/config fixture；重复运行是幂等的（迁移 `Up` 幂等）。
3. **节点并行启动**：`wecom-ha-node-a` / `wecom-ha-node-b` 可同时启动（身份不同，无排他锁）。两者 `restart: "no"`。
4. **最后启动入口**：`wecom-ha-entry` `depends_on node-a/node-b service_started`；启动后开始探测。
5. **登记回调**：将企业微信两个 Bot 回调 URL 设为入口 public origin（`58087`）的 `/callbacks/wecom`，靠 route key 区分，不要分别指向节点端口。

```bash
docker compose --profile wecom-ha-local up --detach --build
# 等 entry /readyz 200 后，再在企业微信控制台登记回调
```

## 三、观测指标与告警

| 维度 | 重点指标 | 来源/接口 |
|---|---|---|
| 容器 | ready/live、restart count、`State.Running`、CPU/内存/FD、进程启动时间（`process_start_id` 变化即重启） | `docker inspect`、节点 `/statusz` |
| 入口 | healthy backend count、`callback` 成功率、`/readyz` 可用性、入口到后端延迟 | 入口 `/statusz`：`backends[].healthy` |
| 调度 | active owner、lease lost 数、`stale fence` 拒绝数、pending oldest age、reclaim rate | Redis lease / PostgreSQL `session_head.last_fence`、worker 日志 |
| 业务 | per-tenant final success、duplicate final、reply retry/ambiguous、queue lag | `delivery_ledger.state`、`outbox.state`、`inbox.state` |
| 依赖 | PostgreSQL 连接池、Redis 命中/延迟、Qdrant/ClamAV 健康 | `/readyz` 依赖探测与容器健康状态 |

推荐以入口 `/statusz`、节点 `/readyz`、容器健康状态和持久化台账定期检查：

- **入口无健康后端**：`count(healthy==true)==0` 持续 > 探测周期 → 严重（公开回调将 503）。
- **单实例丢失**：某 `instance_id` 的 `/readyz` 持续失败且 `State.Running==true` → 进程假死，需人工介入。
- **积压增长**：`inbox_nonterminal` / `outbox_active` / `delivery_active` 单调上升超过阈值且未回落 → reclaim 失效。
- **重复回复**：`delivery_ledger` 同 `(delivery_key, segment_no)` `sent` 行 `>1` → 幂等失效（应按 `testing-and-acceptance.md` 7.3 复核）。
- **频繁重加入**：`process_start_id` 在短时间内多次变化 → 实例不稳定，先排查再继续验收。

入口 `/statusz` 输出每个后端 URL 与 readiness；节点 `/statusz` 输出 instance ID、process start ID 和实际 worker/relay/delivery owner。**入口只接受 `/callbacks/wecom` 与自身 health/status 路由；不得把节点观测端口公开为微信回调。**

## 四、运维边界（禁止操作）

- 不得用 `docker compose down` 注入单实例故障：这会连带关闭 postgres/redis 等共享依赖，超出本能力范围（属于"共享依赖故障"子用例）。
- 不可通过删除 Redis lease、重启存活实例、重绑 Bot 或手工发回复来"修复"本轮验收；应结束判定并另行排障。
- 不得把节点宿主端口 `58088`/`58089` 暴露为企业微信可达地址，只能用入口 `58087`。
- `restart: "no"` 是验收前提；生产若启用 `restart: unless-stopped` 之类策略，则"强杀后容器持续停止"的断言不再成立，需改用"自动重启替换"子用例单独验证。

## 五、回滚约束

- 停用新实例前先 drain、摘除 readiness，再等待有界 shutdown（`DrainTimeout 30s`）；强杀仅用于故障演练，不用于常规回滚。
- schema 回滚需保持 `inbox`/`outbox`/`delivery_ledger` 等表的读写兼容，**不得**清理未完成工作来获得"干净"状态——这会丢失在途任务与去重事实。
- 配置回滚（如 `TRPC_WECOM_HA_ENTRY_BACKENDS` 改动）属无状态变更，可在入口重启后生效；但节点身份变更（`TRPC_WEBUI_LOCAL_INSTANCE_ID`）会改变 owner 命名，需按"重新加入"语义处理（新 `process_start_id`），不应视为旧 owner 延续。

## 六、故障处置手册

### 6.1 单实例挂（受害实例持续停止）

- 现象：`entry/statusz` 中该节点 `healthy:false`，`docker inspect` `State.Running==false`。
- 处理：确认存活实例 `/readyz` 正常、入口 `/readyz` 仍 200；业务应已由存活实例接管。若需恢复，显式 `compose start <victim>`（新 `process_start_id` 重新加入），观察稳定窗口后再恢复负载。
- 反模式：依赖 `restart: "no"` 下 Docker 自动拉起——本拓扑默认不会，若观察到自动拉起说明策略被改，需先修正。

### 6.2 入口无健康后端

- 现象：`entry/readyz` 503，`backends` 全部 `healthy:false`，但两节点容器可能仍在运行。
- 处理：分别查 `node-a:58088/readyz`、`node-b:58089/readyz`；若节点 `/readyz` 失败，按"依赖故障"排查（db/redis/clamav）。若节点正常但入口仍摘除，检查入口到节点的网络与 probe 超时。
- 关键：入口 `/livez` 仍 200 说明入口进程没死，问题在后端或网络，不是入口崩溃。

### 6.3 重加入异常

- 现象：`compose start` 后 `process_start_id` 与基线相同，或重加入后旧 reply 重发、stale fence 写回、连接振荡、backlog 增长。
- 处理：
  - `process_start_id` 相同 → 排查是否误复用容器/卷或复制了旧进程状态（身份应由 `newWebUILocalProcessStartID()` 每次重新生成）。
  - 旧 reply 重发 / stale fence → 说明旧 owner 的未完成工作被错误重放，复核 reclaim 与 fence 校准逻辑（Redis lease + `EnsureFenceAtLeast`）。
  - 连接振荡 / backlog 增长 → 容量或 reclaim 失效，见第三节告警与 `testing-and-acceptance.md` 7.2。

### 6.4 连接振荡（flapping）

- 现象：入口健康集合在 node-a/node-b 间频繁切换；或节点 `/readyz` 抖动。
- 处理：检查共享依赖（postgres/redis/clamav）是否不稳定；检查 `ProbeInterval` 是否过小导致对瞬时抖动过度敏感；确认没有把节点端口误作对外地址导致外部流量直接打挂节点。
- 判定：稳定窗口内 `healthy` 集合应稳定在两后端 true（或单实例故障期稳定在一 true），不应高频翻转。

### 6.5 积压增长（backlog）

- 现象：`inbox_nonterminal` / `outbox_active` / `delivery_active` 单调上升，reclaim 不下降。
- 处理：
  - 确认存活实例 worker/delivery 是否在运行（`/statusz` 的 `owners` 是否变化、进程是否真在消费）。
  - 检查 Redis consumer group pending 是否被某"僵尸 consumer"（旧 `process_start_id`）占据——旧进程已死但 group 未 reclaim，需等待 `ReclaimIdle`/`ReclaimInterval` 回收，或排查 reclaim 循环是否异常退出。
  - 容量不足时临时扩容单实例资源或缩短 `ReclaimInterval`（需评估对 Redis 压力）。
  - 属于"存活实例接管"但接不住负载时，应将其视为容量问题，而非本能力缺陷。

### 6.6 共享依赖故障（超出本包范围）

- 现象：postgres/redis 宕机，两实例 `/readyz` 同时 503，入口随之无健康后端。
- 处理：这属于单一故障域内的依赖故障，本能力不保证其可用性；按依赖自身的高可用（外部 PostgreSQL/Redis 集群、跨主机部署）解决。本包只验证"应用实例级"故障接管，不替代依赖级 HA。

## 七、告警规则示例（语义级）

以下为可映射到现有观测系统的语义级规则：

```yaml
# 入口无健康后端（严重）：所有 backend healthy=false 持续 > 探测周期
- alert: WeComHAEntryNoHealthyBackend
  expr: sum(entry_backend_healthy) == 0
  for: 10s
  severity: critical

# 单实例丢失但容器仍运行（进程假死）
- alert: WeComHANodeNotReadyRunning
  expr: node_ready_success == 0 and node_container_running == 1
  for: 30s
  severity: warning

# 积压增长：inbox 非终态行数单调上升
- alert: WeComHABacklogGrowing
  expr: deriv(inbox_nonterminal[5m]) > 0 and inbox_nonterminal > 50
  for: 5m
  severity: warning

# 重复回复：同 (delivery_key, segment_no) sent>1
- alert: WeComHADuplicateDelivery
  expr: delivery_segment_duplicate > 0
  for: 0m
  severity: critical

# 频繁重加入：process_start_id 短时多次变化
- alert: WeComHAFrequentRejoin
  expr: changes(node_process_start_id[10m]) > 3
  for: 0m
  severity: warning
```

## 八、各故障模式处置命令速查

| 现象 | 第一步 | 第二步 | 升级 |
|---|---|---|---|
| 单实例挂（`State.Running=false`） | `docker inspect <c> --format '{{.State.Running}}'` | 查存活实例 `/readyz` 与入口 `/readyz` | 必要时 `compose start <c>` 重加入 |
| 入口 503 但进程活 | `curl :58087/statusz` 看 `backends` | 分别查 `node-a/b:58088/58089/readyz` | 修依赖或网络 |
| 重加入 `process_start_id` 不变 | 查是否误复用卷/容器 | 复核 `newWebUILocalProcessStartID()` | 重建容器 |
| 连接振荡 | `grep` entry-ready.log 翻转频率 | 查 postgres/redis/clamav 健康 | 调 `ProbeInterval` 或扩容 |
| 积压增长 | `SELECT count(*) FROM inbox WHERE state<>'terminal'` | 查存活实例 worker/delivery 是否消费 | 扩容或查 reclaim 异常 |
| 共享依赖故障 | 两节点同时 `/readyz` 503 | 查 PG/Redis 状态 | 依赖级 HA，非本包范围 |

常用命令：

```bash
# 查看入口健康集合
curl -s http://127.0.0.1:58087/statusz | jq .
# 查看某节点身份与 owner
curl -s http://127.0.0.1:58088/statusz | jq '{instance_id,process_start_id,owners}'
# 确认受害容器持续停止
docker inspect <container> --format '{{.State.Running}}'
# 重新加入
docker compose --profile wecom-ha-local start wecom-ha-node-a
# 查看积压
psql "$DSN" -c "SELECT count(*) FROM inbox WHERE state<>'terminal';"
```

## 九、容量测算示例

假设两 Bot 合并峰值 10 msg/s，平均任务耗时 2s，模型并发上限 8：

- 单实例吞吐 ≈ `并发 / 耗时 = 8 / 2 = 4 msg/s`（瓶颈在模型并发）。
- **任一实例宕机后，存活实例需独自承接 10 msg/s，但单实例仅 4 msg/s → 必积压**。因此单实例容量必须 ≥ 峰值（10 msg/s），而非峰值/2。
- 推导：所需单实例模型并发 ≈ `峰值 × 耗时 = 10 × 2 = 20`。即单实例至少配置模型并发 20 才能在一实例宕机时不积压。

> 此例说明：低流量演练（如 0.1 msg/s）只能证明可用性，绝不能外推为容量承诺。上线前按真实峰值做如上测算，并把单实例容量目标设为"独承接全峰值"。

## 十、四类恢复机制的运维对应

| 子用例 | 运维动作 | 判定归属 |
|---|---|---|
| 存活实例接管 | 强杀受害实例，`restart:"no"`，观察存活实例承接 | 本包主验证 |
| 自动重启/替换 | 若生产启用 restart 策略，受害实例被平台重建 | 单独子用例，不混入接管结论 |
| 计划内优雅停止 | `SIGTERM` → readiness false → drain → exit | 维护窗口，验证 drain 安全 |
| 共享依赖故障 | 依赖宕机，两实例同受影响 | 依赖级 HA，非本包范围 |

> 切勿把"Docker 自动重启拉起 N1"当作"N2 接管"的证据——两者必须分开验证与归因。

## 十一、日常巡检清单

| 频率 | 检查项 | 命令 / 方法 | 通过判据 |
|---|---|---|---|
| 每日 | 入口健康集合 | `curl -s http://127.0.0.1:58087/statusz` | 至少一个 backend `healthy:true` |
| 每日 | 入口自身 ready | `curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:58087/readyz` | `200` |
| 每日 | 两节点身份与 owner | `curl -s http://127.0.0.1:58088/statusz`、`...:58089/statusz` | `instance_id` 分别为 `wecom-ha-node-a` / `wecom-ha-node-b`；`owners` 七项齐全 |
| 每日 | 容器重启计数 | `docker inspect --format '{{.RestartCount}}'` | 无增长（`restart:"no"` 下应恒为 0） |
| 每日 | 积压 | Redis stream pending 数、oldest pending 年龄；`inbox` 非终态行数 | 稳定、不单调增长 |
| 每周 | 投递台账状态分布 | `SELECT tenant_id, state, count(*) FROM delivery_ledger GROUP BY 1,2` | 无长期 `sending`、无增长中的 `ambiguous` |
| 每周 | 重复回复检查 | 同一 `(tenant_id, delivery_key, segment_no)` 计数 | 恒为 1 |
| 每月 | 单实例接管演练 | `bash scripts/e2e/single-host-multicontainer.sh` | 判定表全通过 |
| 每季度 | 真实 Bot 原会话验收 | 外部账户 + `TRPC_WECOM_REAL_ACCEPTANCE=recorded` | 客户端与服务端记录一致 |

> ⚠️ 巡检中若发现 `process_start_id` 在无人工操作的情况下发生变化，应立即按"重加入异常"（§6.3）排查——这意味着节点在非计划重启。

## 十二、变更影响矩阵

| 变更 | 直接影响 | 必须同步 |
|---|---|---|
| 改 `TRPC_WEBUI_LOCAL_INSTANCE_ID` | owner/consumer/lease/claim 命名全部改变 | 🔴 两节点必须仍是不同值；改后需重做基线 |
| 改 `TRPC_WECOM_HA_ENTRY_BACKENDS` | 入口转发目标集合 | 必须仍 ≥2、去重、`http`；改后确认 `/statusz.backends` |
| 改 `TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL` | 故障检测窗口 | 保持 `[100ms, 1m]`；改后重测接管时限 |
| 启用 `restart` 策略 | 受害实例会被自动重建 | 🔴 与"存活实例接管"验收互斥，必须拆分为独立子用例 |
| 改节点宿主端口（58088/58089） | 仅观测用途 | 🔴 确认未被登记为企业微信回调地址 |
| 新增第三个 Bot | 路由与凭据面扩大 | 需同步 binding/route 初始化与 `(Corp,Agent)` 唯一性校验；重新确认回调登记 |
| 调整 worker `LeaseTTL` / `ReclaimInterval` | 接管时限 | 同步告警阈值；重做 `T_CONNECT`/`T_READY` 验收 |
| 调整 `ClaimTTL` | 发送者崩溃后的接管时限 | 确保 `ClaimRenewInterval < ClaimTTL`；重测重复回复检查 |
| 调整模型的并发/超时 | 单实例容量 | 重做容量测算（§9）；确认一实例宕机时不积压 |
| 升级镜像 | 全部行为 | 重跑基线 + 单实例接管 + 重加入 + 反向故障四段 |

## 十三、演练准入 / 退出清单

**准入（缺一不可）：**

- [ ] 镜像 digest、commit SHA、`config_version` 已固定并记录
- [ ] 两个 `(Corp ID, Agent ID)` 元组确认不同
- [ ] `restart: "no"` 已确认（避免自动重建掩盖接管）
- [ ] 观察窗口与采样间隔（`W_BASELINE`、`TRPC_SINGLE_HOST_STABILITY_SECONDS`、`DELTA_POLL`）已固定
- [ ] 证据目录已准备（独立于代码仓库）
- [ ] 明确本轮 `TRPC_WECOM_REAL_ACCEPTANCE` 取值（`assumed` 或 `recorded`）

**退出（必须全部满足）：**

- [ ] 单实例强杀后，受害容器两次 `docker inspect` 均为 `State.Running=false`
- [ ] 入口 `/readyz` 在故障后仍 200，且 `/statusz` 只剩存活后端 healthy
- [ ] 存活节点在预设时限内 `/readyz` 200 且承接职责
- [ ] 重新加入后 `process_start_id` 与基线不同
- [ ] 稳定窗口内无旧 reply 重发、无 ownership 抖动、无积压单调增长
- [ ] 反向强杀 N2 后同样成立
- [ ] 证据目录完整（含各阶段 `/statusz` 三份快照与 compose 诊断）
- [ ] 结论中如实标注 `assumed` / `recorded`，且未把演练外推为容量承诺
