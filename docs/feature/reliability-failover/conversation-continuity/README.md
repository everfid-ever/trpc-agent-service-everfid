# 故障恢复后继续对话 — 交付文档包（索引）

> **对应项目描述：企微 AI Bot 多租户可靠性平台** — 主线一「节点故障后的**会话连续性**」。
> 描述原文对照：*基于租户 ID、Bot Binding、会话 ID 与消息 ID 建立持久化会话链路；在应用节点故障后，通过连接与 Worker 自动接管恢复两个 Bot 的消息处理能力，确保用户在原会话中继续对话时可正确回忆上下文，且租户归属和最终回复不发生串扰。*
> ⚠️ 描述中的"连接接管"在本架构为**回调式**（不存在长连接转移），准确含义见 `FULL-GUIDE.md` §7.1。

> **文档状态：** 已实现本地双 Bot / 双节点故障接管组合。`wecom-ha-local` profile 提供一次性 bootstrap、两个无自动重启的应用节点（`restart: "no"`）和稳定 callback 入口；`scripts/e2e/wecom-ha-failover.sh` 会强杀其中一个节点并核对入口与存活节点。真实 WeCom 对话由外部账户完成，本包默认以 `TRPC_WECOM_REAL_ACCEPTANCE=assumed` 记为通过前置。

本包定义两个企业微信 Bot、两个租户和两个应用节点在**单应用节点非优雅终止后**，两个 Bot 仍在**原会话**继续正确对话的能力：正确回忆上下文、租户归属不串扰、最终回复每输入仅一条且不重发。

> **怎么读（按角色）：**
> - **非代码读者** → [../STANDALONE-SOLUTION-GUIDE.md](../STANDALONE-SOLUTION-GUIDE.md) 🟢 的 §5（能力一）+ 本包 [OVERVIEW.md](./OVERVIEW.md) 🟢 + [FULL-GUIDE.md](./FULL-GUIDE.md) 的 §3–§8、§12 脑裂分析、§13 四类恢复机制、§15 判据。**可整篇跳过** `REFERENCE-IMPLEMENTATION.md`、`CODE-APPENDIX.md`、`implementation-walkthrough.md`。
> - **工程实现者** → [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)（完整 DDL / Lua / 接口 / 算法）→ [design.md](./design.md) → [implementation-walkthrough.md](./implementation-walkthrough.md) → [CODE-APPENDIX.md](./CODE-APPENDIX.md)。库架构、迁移演进、隔离级别与行锁、加密存储、只增表清理责任见 [../DATABASE-DESIGN.md](../DATABASE-DESIGN.md)。
> - **运维 / 值班 / 验收** → [release-and-operations.md](./release-and-operations.md) 与 [testing-and-acceptance.md](./testing-and-acceptance.md)。
> 跨包角色总路径见 [../README.md](../README.md) §0。

---

## 一、15 分钟复现直达（最少步骤概览）

以下步骤足以在本机复现「单节点故障后两 Bot 仍能继续对话」的自动化可观察部分（不含真实手机端对话；真实对话见 `testing-and-acceptance.md` §6）。

```bash
# 1. 进入仓库根
cd /Users/everfid/code/trpc-agent-service

# 2. 准备本地密钥文件（脚本会校验其存在且非空）
#    deploy/compose/secrets/deepseek-api-key
#    deploy/compose/secrets/wecom.env
#    wecom.env 需包含两组 Bot 凭据：WECOM_* 与 WECOM_SECONDARY_*，
#    且两组 (Corp ID, Agent ID) 必须不同。

# 3. 启动本地双节点 + 稳定入口（profile: wecom-ha-local）
docker compose -f deploy/compose/docker-compose.local.yml \
  --profile wecom-ha-local up -d --build

# 4. 运行强杀演练（自动创建独立 Compose project，结束后清理容器与卷）
bash scripts/e2e/wecom-ha-failover.sh
```

脚本会依次确认两个节点的 `/readyz`、两个实例的 `/statusz`（含 `instance_id` 与 `worker` owner）、稳定入口 `/livez`；随后对 `wecom-ha-node-a` 执行 `docker kill --signal=KILL`，再确认入口 `/livez` 与 node-b `/readyz` 仍可用，并两次确认受害容器未自动重启。

**真实对话接管验收（外部前置）：** 在 node-a 被强杀、node-b 接管后，通过两个 Bot 在原会话各发一条带唯一标记的新消息，核对各自正确回忆历史代号、仅向本租户回复一次、服务端投递记录唯一（详见 `testing-and-acceptance.md`）。

---

## 二、已实现范围声明

| 项 | 状态 | 说明 |
|---|---|---|
| 双 Bot 可信路由（tenant 不串） | ✅ | `channel_public_route` → `channel_binding_locator` → verify → Tenant Context |
| 原会话持久化链路 | ✅ | `(tenant_id, agent_app_id, session_id)` + inbox 四元组去重 |
| 单节点非优雅故障后自动接管 | ✅ | lease 到期 + Redis stream reclaim + 更高 fence 提交 |
| 上下文回忆不丢、最终回复不重发 | ✅ | `commit_turn` 单事务 + `delivery_ledger` 条件 CAS |
| 真实 WeCom 双 Bot 原会话对话 | ⚠️ 外部验收前置 | 默认 `assumed`，手动 `recorded` |
| 在途任务 P1–P4 精确接管 | ❌ 见相邻文档包 | `inflight-task-takeover` |
| 同主机多容器重新加入 / 反向故障 | ❌ 见相邻文档包 | `single-host-multicontainer` |
| 跨主机 / 整机 / 共享依赖故障 | ❌ 不承诺 | 同单一故障域 |

---

## 三、文档地图

| 文档 | 读者 | 内容 | 篇幅目标 |
|---|---|---|---|
| `README.md` | 所有人 | 本文：索引 + 15 分钟复现 + 范围 + 地图 | ≥60 行 |
| `OVERVIEW.md` | 不读代码的接手者 | 场景、术语表、端到端业务故事、范围与不包含 | ≥120 行 |
| `FULL-GUIDE.md` | 设计 / 评审 | 可信路由、会话键、lease/fence、原子提交、接管时序、观测口径、设计边界 | ≥350 行 |
| `design.md` | 架构师 | 架构图、四元组持久化链路、数据模型、会话状态机、时序图、模块划分 | ≥300 行 |
| `implementation-walkthrough.md` | 实现者 | 启动装配→callback→inbox→dispatch→worker→commit→relay→delivery 逐模块走读 | ≥300 行 |
| `CODE-APPENDIX.md` | Review / 审计 | 关键实现代码摘录与「改变了什么状态、为何抗单节点故障」逐段解释 | ≥400 行 |
| `REFERENCE-IMPLEMENTATION.md` | **从零复现者** | **新增**：完整 DDL、Redis Lua 契约、Go 接口、核心算法伪代码、配置全表、启动顺序、验证清单 | ≥500 行 |
| `testing-and-acceptance.md` | QA / SRE | 测试方案、参数、命令、脚本行为、证据字段、通过判定表、反例、真实验收 | ≥300 行 |
| `release-and-operations.md` | 运维 / 发布 | 配置、部署、发布顺序、观测指标、告警、回滚、故障处置手册 | ≥250 行 |

> 所有引用仓库路径统一写成「（仓库对应位置，可选核对：`path`）」，文档内均给出可直接照抄的材料，不依赖仓库存在即可实现等价系统。

## 四、核心不变量速览（详细见 `FULL-GUIDE.md` / `REFERENCE-IMPLEMENTATION.md`）

- **I1** tenant 只能由验签后的 binding 推出。
- **I2** 每个 provider 输入只有一个 durable 事实（`inbox` 唯一键 + `claim_inbox` + `prepare_dispatch`）。
- **I3** 同一会话同一时刻只有一个有效提交者（Redis lease + 递增 fence + `commit_turn` 的 `p_fence < last_fence` 拒绝）。
- **I4** terminal 结果与待发回复原子提交（单事务写 `session_commit` + `session_head` + `outbox`；`session_event` 只在调用方传入 events 时写入，生产 durable turn 传 null，会话转写由官方会话后端持有，见 [../DATABASE-DESIGN.md](../DATABASE-DESIGN.md) §1.1）。
- **I5** 未 ACK 之前不得 ACK；ACK 必须在 terminal commit 之后。
- **I6** 两个发送者不能同时发同一回复片段（`delivery_ledger` 条件更新）。
- **I9** 重新加入不复用旧 owner（`ProcessStartID` 每次启动新 8 字节 hex）。
- **I10** 进程活着不等于可服务（`/readyz` 覆盖 db + redis + malware）。
