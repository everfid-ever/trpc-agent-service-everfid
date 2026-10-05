# 单主机多容器实例可用性 — 全景导读

## 0. 一句话

在同一主机部署两个独立应用实例 `wecom-ha-node-a` / `wecom-ha-node-b`；一个实例被非优雅终止后，另一个实例继续通过**原企业微信入口**服务两个 Bot，并在退出实例以新进程身份重新加入后仍保持任务、会话和回复稳定。

本包验证的是**应用进程/容器故障**下的实例级高可用，不证明宿主机、电源、Docker daemon、共享磁盘、共享数据库或整机网络故障下的可用性。

> **怎么读：** 本文 🟢 全文不含代码，方框与流程图仅作示意，可逐字读完。读不懂某个反引号名字（如 `process_start_id`）时查 §2 术语表。只想了解结论，读 §0、§3 三阶段业务故事、§4 范围与不包含、§5 关键不变量即可。

## 1. 场景

企业内部通过企业微信（WeCom）接入两个 Bot：

- **主 Bot** `local-wecom`：企业微信控制台登记的回调地址指向稳定入口 `wecom-ha-entry`。
- **次 Bot** `local-wecom-secondary`：Corp ID / Agent ID 与主 Bot 不同，**共用同一个公开 origin**，仅靠 durable `route key` 区分。

两个 Bot 各自有独立会话与用户。正常运行时，两个应用实例都能服务两个 Bot 的输入、执行任务、投递回复。当某个实例被 `SIGKILL` 强杀，存活实例应在不修改任何回调地址、不重启入口的前提下，自动接管被中断的职责。

```text
企业微信控制台
   │  (两个 Bot 回调 URL，均指向同一公开 origin)
   ▼
wecom-ha-entry  :58087   ← 唯一允许暴露给 HTTPS tunnel / 企业微信控制台的端口
   │  每秒探测 /readyz，只把 callback 转发给健康后端
   ├──────────────►  wecom-ha-node-a  :8080   (TRPC_WEBUI_LOCAL_INSTANCE_ID=wecom-ha-node-a)
   └──────────────►  wecom-ha-node-b  :8080   (TRPC_WEBUI_LOCAL_INSTANCE_ID=wecom-ha-node-b)
                         │               │
                         └──── 共享 ─────┴──►  PostgreSQL / Redis / Qdrant / ClamAV / otel-collector
```

> 关键约束：node-a / node-b 的宿主机端口（默认 `58088` / `58089`，容器内固定 `:8080`）**仅用于本地观测**，**不得登记为企业微信回调地址**。对外只暴露 `wecom-ha-entry`（`58087`）。

## 2. 术语表

| 术语 | 含义 |
|---|---|
| 实例（instance） | 一个运行中的 `wecom-local` 容器，由其 `TRPC_WEBUI_LOCAL_INSTANCE_ID`（部署槽位）与 `process_start_id`（本次启动身份）共同标识 |
| 部署槽位（slot） | 如 `wecom-ha-node-a`，固定字符串，用于日志/metrics/lease 区分 |
| `process_start_id` | 每次进程启动随机生成的 8 字节 hex（16 字符），与槽位拼接成 owner 名 |
| owner / consumer | 写入 lease、ledger claim、日志、Redis consumer group 的标识，形如 `webui-local-worker-wecom-ha-node-a-<psid>` |
| 稳定入口（entry） | `wecom-ha-entry`，独立于应用节点，承载企业微信公开回调，按 readiness 轮转转发 |
| readiness | `/readyz`：db.PingContext + redis.Ping + malware.Probe 全部成功才 200 |
| liveness | `/livez`：仅表示进程未退出，恒 200 |
| route key | 区分两个 Bot 回调的 durable 标识（`local-wecom` / `local-wecom-secondary`），同一公开 origin 下据此路由 |
| 重新加入（rejoin） | 受害实例被显式 `compose start` 后，以新 `process_start_id` 重新成为候选实例 |
| 反向故障（reverse） | 双实例基线重建后，强杀另一实例，对称验证接管 |
| 单故障域 | 宿主机、Docker daemon、共享磁盘、共享 PostgreSQL / Redis 同处一个失败边界 |

## 3. 三阶段业务故事

### 阶段一：双实例基线

1. `wecom-ha-bootstrap` 一次性初始化共享 tenant / config fixture（依赖 postgres / redis / qdrant healthy）。
2. `wecom-ha-node-a` 与 `wecom-ha-node-b` 启动，均配置两个 Bot、共享同一权威库与队列，但部署身份不同。
3. 入口探测两个后端 `/readyz` 均为健康，公开地址稳定。
4. 稳定窗口内：两个 Bot 向两实例重叠发送请求，核对 tenant / session / 最终投递，确认无连接振荡、无 owner 高频迁移、无队列积压。

### 阶段二：强杀 N1，N2 接管

1. 禁用自动恢复（Compose `restart: "no"`），对 `wecom-ha-node-a` 容器执行 `docker kill --signal=KILL`。
2. N1 无法续租、不能 ACK、不能 finish delivery；入口探测其 `/readyz` 失败，将其从健康后端集合摘除。
3. 公开入口地址**不变**，仍只转发给存活的 `wecom-ha-node-b`。
4. N2 reclaim N1 遗留的 pending work / reply，取得更高 fence 或过期 claim，继续原会话服务。
5. 两个 Bot 在原会话发新标记消息，验证正确且唯一的最终回复、正确的 tenant 与历史代号。

### 阶段三：N1 重新加入 + 反向故障

1. 接管验收完成后，显式 `compose start wecom-ha-node-a`。N1 生成**新** `process_start_id`，通过 readiness 后重新成为候选实例。
2. 完整稳定窗口：不应发送旧 reply、不应把 stale fence 写回、不应造成连接振荡或 backlog 增长。
3. 重建双实例基线，强杀 `wecom-ha-node-b`，独立记录实际连接 / worker / delivery owner，确认入口只剩重新加入的 N1。
4. 对称故障证明系统不是只对某个固定主节点有效。

## 4. 范围与不包含

### 本方案覆盖

- 同一主机内多个应用实例共享数据库、队列与协调服务运行。
- 通过独立节点身份、端口及本地状态隔离避免实例冲突。
- 单实例被非优雅终止后，由存活实例自动接管 Bot 连接、任务执行及回复投递职责。
- 覆盖实例重新加入、反向故障以及持续负载下的连接稳定性、任务积压与重复回复检查。

### 明确不包含（不承诺）

- **跨主机 / 整机断电 / Docker daemon / 共享磁盘** 故障下的可用性：本拓扑中宿主机、Docker、PostgreSQL、Redis 仍为同一故障域。
- **共享依赖故障**：PostgreSQL 或 Redis 宕机时，两实例同时失去权威状态，不在本能力范围内（属于"四类恢复机制"中的"共享依赖故障"子用例）。
- **模型调用次数恒为 1**：P2 接管可重跑模型，最终回复去重靠 delivery ledger 与 `client_request_id`，不保证模型只算一次。
- **下游无法按 `client_request_id` 去重时的 exactly-once**：此时诚实标记为 `ambiguous`，由对账收敛。
- **把 Docker 自动重启当作接管**：`restart: "no"` 已禁用，强杀后容器必须持续停止；否则"存活实例接管"不可证。
- **把健康恢复 / 连接恢复 / 队列恢复单独当作完整业务 RTO**：必须结合用户最终可见回复综合判断。

## 5. 关键不变量（贯穿全包）

| # | 不变量 | 证据 |
|---|---|---|
| I1 | tenant 只能由验签后的 binding 推出 | `/statusz` owner、ingress 记录、delivery adapter 解析 |
| I2 | 每个 provider 输入只有一个 durable 事实 | `inbox` 唯一键 + `claim_inbox` + `prepare_dispatch` |
| I3 | 同一会话同一时刻只有一个有效提交者 | Redis lease + 递增 fence + `commit_turn` 的 `p_fence < last_fence` 拒绝 |
| I8 | 公开 callback 地址不随实例切换 | 入口 `/statusz` backends healthy 集合 |
| I9 | 重新加入不复用旧 owner | `process_start_id` 前后对比 |
| I10 | 进程活着不等于可服务 | `/readyz` 覆盖 db + redis + malware |

> 完整不变量矩阵见 `FULL-GUIDE.md` 第 9 节与 `REFERENCE-IMPLEMENTATION.md` 的不变量表。

## 6. 与相邻能力包的关系

本目录是仓库 `docs/feature/reliability-failover/` 下三个能力包之一，三者互补但不重叠：

| 包 | 验证焦点 | 与本包的区别 |
|---|---|---|
| `conversation-continuity` | 故障恢复后两个 Bot 在**原会话**继续正确对话 | 关注会话层语义，不要求多实例拓扑 |
| `inflight-task-takeover` | 在途任务 P1–P4 的接管与幂等投递 | 关注单任务生命周期的暂停/恢复点，可叠加在本包之上 |
| `single-host-multicontainer`（本包） | 单主机双实例的**拓扑级**接管、重加入与反向故障 | 关注实例生命周期与入口稳定性 |

实际演练时，本包证明"实例挂了另一个能顶上"；若还要证明"故障瞬间正在跑的那条任务不丢、不重、最终送达"，需配合 `inflight-task-takeover` 的 P1–P4 屏障；若还要证明"两个 Bot 的对话历史连贯"，需配合 `conversation-continuity` 的会话校验。三者组合才是完整的"故障不影响用户"证据链。

## 7. 快速自检清单（读者视角）

读完本文你应能回答：

- [ ] 两个实例靠什么区分身份？（`TRPC_WEBUI_LOCAL_INSTANCE_ID` + `process_start_id`）
- [ ] 为什么企业微信只登记入口地址？（公开地址不随实例切换，I8）
- [ ] node-a/node-b 的 `58088`/`58089` 能不能登记为微信回调？（不能，仅观测）
- [ ] `/livez` 与 `/readyz` 差在哪？（live=进程活；ready=依赖齐备可服务）
- [ ] 强杀后为什么必须 `restart:"no"`？（否则"存活实例接管"不可证）
- [ ] 重新加入为什么不算旧 owner 复活？（新 `process_start_id`，I9）
- [ ] 什么故障本包不保证？（宿主机/Docker/磁盘/共享 PG/Redis，单故障域）

若任一答不上，回到对应章节重读。
