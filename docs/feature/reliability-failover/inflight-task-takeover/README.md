# 在途任务接管与幂等投递 — 文档包

> **对应项目描述：企微 AI Bot 多租户可靠性平台** — 主线二「**在途任务恢复**」。
> 描述原文对照：*覆盖消息已持久化未执行、模型或工具执行中、结果已提交未发送等关键故障窗口；通过任务租约、执行权回收、Fencing Token、Inbox/Outbox 和 Delivery Ledger 实现原始消息自动续处理或安全重试，区分模型调用次数、任务尝试次数与最终回复次数，保证用户无需重发且每个输入只产生一次可见最终回复。*
> ⚠️ 末句的"一次可见最终回复"在下游不可按 `client_request_id` 去重时只能承诺**至少一次 + 诚实告警**，见顶层 `README.md` §0.5。

> 子包定位：`reliability-failover / inflight-task-takeover`
> 一句话：当一条用户消息已经**被系统可靠接收**、但还没产生**最终可见回复**时，执行或发送节点故障，存活节点必须自动继续处理同一条原始消息；用户不重发，系统不串租户、不丢会话、不重复产生最终回复或外部业务效果。

本文档包是**自足的复现文档**。一个从未见过本仓库源码的工程师，仅凭本目录下的 Markdown，即可从零实现等价系统并通过同等验收。所有表名、字段名、SQL 函数名、环境变量名、Go 标识符、错误码均逐字对应真实实现（见 [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md) 的「事实来源」一节），不引用「见仓库某文件」作为唯一说明。

> **怎么读（按角色）：**
> - **非代码读者** → 只读 [OVERVIEW.md](./OVERVIEW.md) 🟢（§0 有"技术名 → 大白话"对照表）+ [FULL-GUIDE.md](./FULL-GUIDE.md) 的 §2 五个持久化事实、§3–§6 四个窗口、§10 边界。**可整篇跳过** `REFERENCE-IMPLEMENTATION.md`、`CODE-APPENDIX.md`、`implementation-walkthrough.md`、`design.md`。
> - **工程实现者** → [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)（完整 DDL / Lua / 接口 / 算法）→ [design.md](./design.md) → [implementation-walkthrough.md](./implementation-walkthrough.md) → [CODE-APPENDIX.md](./CODE-APPENDIX.md)。库架构、迁移演进、隔离级别与行锁、加密存储、只增表清理责任见 [../DATABASE-DESIGN.md](../DATABASE-DESIGN.md)。
> - **运维 / 值班 / 验收** → [release-and-operations.md](./release-and-operations.md) 与 [testing-and-acceptance.md](./testing-and-acceptance.md)。
> 跨包角色总路径见仓库同级 [../README.md](../README.md) §0。

---

## 1. 文档地图

| 文档 | 用途 | 何时读 |
|---|---|---|
| [OVERVIEW.md](./OVERVIEW.md) | 场景、术语表（五个持久化事实）、P1–P4 业务故事、范围与不包含 | 先读，建立心智模型 |
| [FULL-GUIDE.md](./FULL-GUIDE.md) | 完整独立说明：五个持久化事实、P1–P4 的前提/风险/方案/必须接受的事实、barrier、幂等分层、演练顺序、验收与边界 | 设计评审、写实现前通读 |
| [design.md](./design.md) | 架构图、消息生命周期状态机、四个故障窗口的数据边界、任务租约/执行权回收、Outbox/Ledger、时序图 | 理解结构与时序 |
| [implementation-walkthrough.md](./implementation-walkthrough.md) | 逐模块实现走读：barrier 接口 → 四个挂点 → preprocess dispatch → worker 执行与 commit → relay → delivery | 写代码时对照 |
| [CODE-APPENDIX.md](./CODE-APPENDIX.md) | 逐段摘录关键实现代码并解释状态变化与故障语义 | 怀疑某条不变量是否成立时查 |
| [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md) | **新增**。自足复现材料：完整 DDL、Redis 契约、Go 接口、四挂点算法、Ledger CAS SQL、配置全表、端到端复现步骤、验证清单 | 从零实现或复现验收的权威材料 |
| [testing-and-acceptance.md](./testing-and-acceptance.md) | 完整测试方案：参数、P1–P4 逐步命令与预期、脚本行为逐条解释、三计数核对、证据字段、判定表、反例清单 | 做验收、写 CI |
| [release-and-operations.md](./release-and-operations.md) | 配置、部署、发布顺序、观测指标、回滚约束、故障处置手册 | 上线与值班 |

---

## 2. 已实现范围声明（务必先读）

本包覆盖的故障窗口与保证：

- **P1 已持久化未执行**：原始输入 + 预处理任务已 durable，执行任务尚未发出。故障后由存活节点的未完成任务扫描/Outbox 重放自动继续 dispatch。
- **P2 模型或工具执行中**：worker 已领取、已发起模型/工具调用，terminal result 尚未提交。故障后原队列消息不 ACK；存活节点 reclaim 同一条消息、取更高 fence、重试可重跑的工作。
- **P2 子窗口 · 工具调用执行中（`allow` 工具）**：工具执行进度**无任何 durable 记录**，接管后以同一输入**重开一轮**（模型重新推理、可能再次调用该工具），不做断点续跑。详见 `FULL-GUIDE.md` §4.5 与 `REFERENCE-IMPLEMENTATION.md` §10。
- **P2 子窗口 · 工具调用执行中（`ask` 工具）**：有工具级账本 `tool_attempt`（`effect_unknown` → `succeeded`/`failed`）+ 加密结果 `tool_result_payload`；接管后**不重跑**，采用已存结果或诚实以"效果不确定"终结。
- **P3 结果已提交未发送**：terminal outcome、会话推进、reply outbox 已原子保存，下游尚未调用。恢复者**绝不重跑模型**，只扫描未发布 outbox、重新发布并发送已保存结果。
- **P4 下游已接受本地未确认**：发送节点已取得下游接受证据（如 provider message ID），但在把 ledger 写为 `sent` 之前死亡。新节点按 `client_request_id` 去重/对账后可收敛；若下游无法去重或查询，则保留 `ambiguous` 并告警。

**明确不承诺（诚实边界，详见 OVERVIEW §范围与不包含）：**

1. 不承诺跨主机 / 整机断电 / Docker daemon / 共享磁盘 / 共享 PostgreSQL·Redis 故障下的可用性（同为单一故障域）。
2. 不承诺模型调用次数恒为 1；P2 接管可重跑模型（模型内部生成状态通常不可跨节点恢复）。
3. 不承诺下游无法按 `client_request_id` 去重或查询时 P4 的绝对 exactly-once；此时只能「至少一次 + `ambiguous` 告警」。
4. 不把 Docker 自动重启（`restart: "no"` 已禁用）当作存活节点接管。
5. 不把健康恢复 / 连接恢复 / 队列恢复单独当作完整业务 RTO。

---

## 3. 15 分钟复现直达（最少步骤）

> 以下命令全部可照抄。环境变量、SQL、接口定义见 [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)。

**前置（一次性）：**

```bash
# 1) 准备好本地密钥文件（真实仓库要求存在，否则脚本直接退出）
test -s deploy/compose/secrets/deepseek-api-key
test -s deploy/compose/secrets/wecom.env

# 2) 确认 docker 与 docker compose v2、curl 可用
docker --version
docker compose version
command -v curl
```

**复现 P2（最典型的「执行中故障」）：**

```bash
# 在仓库根目录执行；脚本会拉起隔离 compose 项目，等两节点 ready
TRPC_INFLIGHT_TENANT_ID=<tenant-a-id> \
  bash scripts/e2e/inflight-takeover.sh p2
```

脚本运行后：

1. 提示「现在通过选定的 Bot 发送一条新的、带唯一标记的消息」。
2. 脚本轮询两节点 `/statusz`，命中 `enabled:true` + `point:"p2_..."` + `hit:true` 即确定受害 / 存活节点。
3. 对受害容器 `docker kill --signal=KILL`（强杀，不优雅退出）。
4. 对存活节点 `POST /test/failover/barrier/release`（带 `X-TRPC-Local-Token`），恢复测试暂停。
5. 真实 WeCom 对话仍按 `TRPC_WECOM_REAL_ACCEPTANCE=assumed`（默认）作为「前置已通过」。

**验收快查（等同 [testing-and-acceptance.md](./testing-and-acceptance.md) 的 P2 断言）：**

- 同一条 `request_id` 在故障前已有 durable 证据（inbox / preprocess_job / execution_record）。
- 受害节点停止期间，存活节点完成同 `request_id` 的 terminal commit，且旧 fence owner 被 `stale fence` 拒绝。
- 最终回复**只出现一次**（每 segment 一行，`sent` 后只允许 ACK，不允许重发）。
- 对照租户 Tenant B 全程正常，不串 tenant/session。

> 其余 P1 / P3 / P4 仅把上例的 `p2` 换成 `p1` / `p3` / `p4`，语义与断言见 [testing-and-acceptance.md](./testing-and-acceptance.md)。

---

## 4. 三个计数：必须量化区分

贯穿全包的验收核心。任何一个「重复 / 丢失」判断都先落到这三个计数上：

| 计数 | 允许 >1？ | 来源字段 | 含义 |
|---|---|---|---|
| **模型调用次数** | **允许**（P2 接管可重跑） | 不落库，由执行重试驱动 | 一次 terminal commit 之前模型可被多个 owner 调用多次 |
| **任务尝试次数** | 受控 | `execution_record.park_attempt` / `delivery_ledger.attempt` | 输入序号 / 回复片段的重试次数，有上限（`park_execution` 的 `p_max_attempts` ≤ 64，delivery `MaxAttempts` 默认 8） |
| **最终回复次数** | **必须 = 1** | `delivery_ledger` 每 segment 一行，`state='sent'` 后不可重发 | 每个可见的最终回复片段只产生一次 |

**铁律：** 上述三者互不等价。一次 terminal commit 可能来自多次模型调用；一次最终回复可能背后有多次 `delivery_ledger.attempt`；但最终回复一旦 `sent`，只许 ACK，不许重发。

---

## 5. 与本仓库其它两个子包的关系

| 子包 | 关注 |
|---|---|
| `conversation-continuity` | 故障恢复后**两个 Bot 在原会话继续正确对话**（语义连续） |
| `single-host-multicontainer` | 同主机 N1/N2 共存、单实例被 SIGKILL、重新加入 |
| **`inflight-task-takeover`（本包）** | 故障发生在「消息已收 / 正在执行 / 已提交未发 / 已接受未确认」四窗口时的接管与幂等投递 |

本包不替代另两个包；三者可组合演练。
