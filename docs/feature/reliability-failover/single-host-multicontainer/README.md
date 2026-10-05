# 单主机多容器实例可用性 — 交付文档包

> **对应项目描述：企微 AI Bot 多租户可靠性平台** — 主线三「**单机多实例可用性**」。
> 描述原文对照：*支持同一主机内多个应用实例共享数据库、队列与协调服务运行，通过独立节点身份、端口及本地状态隔离避免实例冲突；在单个实例被非优雅终止后，由存活实例自动接管 Bot 连接、任务执行及回复投递职责，并覆盖实例重新加入、反向故障和持续负载下的连接稳定性、任务积压与重复回复检查。*
> ⚠️ 描述中的"Bot 连接"在本架构为**回调式**（不存在长连接转移），准确含义见 `FULL-GUIDE.md` §3.1。

> 本包是一份**自足的复现文档**：一个从未见过本仓库源码的工程师，仅凭本目录下的 Markdown，即可搭出等价拓扑并通过同等验收。
> 所有环境变量名、脚本变量名、容器/服务名、端口默认值、Go 标识符、HTTP 路径，均以实现代码为准；引用仓库路径统一写为「（仓库对应位置，可选核对：`path`）」，不作为唯一说明。

> **怎么读（按角色）：**
> - **非代码读者** → [../STANDALONE-SOLUTION-GUIDE.md](../STANDALONE-SOLUTION-GUIDE.md) 🟢 的 §7（能力三）+ 本包 [OVERVIEW.md](./OVERVIEW.md) 🟢 + [FULL-GUIDE.md](./FULL-GUIDE.md) 的 §1–§11、§19 三类退化模式、§20 重新加入序列。**可整篇跳过** `REFERENCE-IMPLEMENTATION.md`、`CODE-APPENDIX.md`、`implementation-walkthrough.md`。
> - **工程实现者** → [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)（入口实现 / Compose / 契约 / 清单）→ [design.md](./design.md) → [implementation-walkthrough.md](./implementation-walkthrough.md) → [CODE-APPENDIX.md](./CODE-APPENDIX.md)。库架构、迁移演进、隔离级别与行锁、加密存储、只增表清理责任见 [../DATABASE-DESIGN.md](../DATABASE-DESIGN.md)。
> - **运维 / 值班 / 验收** → [release-and-operations.md](./release-and-operations.md) 与 [testing-and-acceptance.md](./testing-and-acceptance.md)。
> 跨包角色总路径见 [../README.md](../README.md) §0。

## 0. 文档状态

已实现本地单主机双实例（`wecom-ha-node-a` / `wecom-ha-node-b`）生命周期演练。两条 Bot（主 `local-wecom` 与次 `local-wecom-secondary`）通过一个**稳定入口** `wecom-ha-entry` 接入；入口使用仓库内健康探测 + 轮转转发，节点拥有独立 `process_start_id` 与 owner 身份，共享 PostgreSQL / Redis / Qdrant / ClamAV，且 `restart: "no"` 禁用自动重建。脚本 `scripts/e2e/single-host-multicontainer.sh`（仓库对应位置，可选核对：`scripts/e2e/single-host-multicontainer.sh`）覆盖：N1 强杀 → 存活 N2 接管 → N1 以**新进程身份**重新加入 → 反向强杀 N2。真实 WeCom 会话按 `TRPC_WECOM_REAL_ACCEPTANCE=assumed`（默认）作为前置已通过。

## 1. 15 分钟复现直达（最少步骤）

> 以下每一步的代码/配置都必须在文档内给出；此处只列**顺序骨架**。完整材料见 `REFERENCE-IMPLEMENTATION.md`。

1. **准备密钥**（仓库对应位置，可选核对：`deploy/compose/secrets/`）：放置 `deepseek-api-key` 与 `wecom.env` 两个文件（非空）。缺任一个，演练脚本会直接退出。
2. **拉起拓扑**：
   ```bash
   bash scripts/e2e/single-host-multicontainer.sh
   ```
   该脚本自动执行 `docker compose --profile wecom-ha-local up --detach --build`，等待 `node-a` / `node-b` 的 `/readyz` 与入口 `entry` 的 `/readyz`，并确认两个后端在入口 `/statusz` 中均为 `healthy:true`。
3. **基线快照**：脚本记录 `node-a` 的 `process_start_id`（从 `/statusz` 的 `process_start_id` 字段解析）。
4. **强杀 N1**：脚本对 `wecom-ha-node-a` 容器执行 `docker kill --signal=KILL`，随后断言入口只剩 `wecom-ha-node-b` 健康、受害容器持续 `State.Running=false`。
5. **重新加入**：脚本 `compose start wecom-ha-node-a`，断言新 `process_start_id` 与基线不同。
6. **反向强杀 N2**：脚本对 `wecom-ha-node-b` 执行 `docker kill --signal=KILL`，断言入口只剩重新加入后的 `wecom-ha-node-a` 健康、受害容器持续停止。
7. **退出即清理**：默认 `TRPC_SINGLE_HOST_KEEP_ENVIRONMENT=false`，脚本 `trap` 在 EXIT 时 `compose down --volumes --remove-orphans` 并保留诊断目录（含每阶段 `*-node-a.json` / `*-node-b.json` / `*-entry.json`）。

如果一切通过，终端输出 `single-host multi-container drill completed; diagnostics retained at <tmpdir>`，脚本返回 0。

## 2. 已实现范围声明

| 已覆盖 | 说明 |
|---|---|
| 双实例稳定共存 | 同一主机两份 `wecom-local`，共享权威库与队列，各自独立身份 |
| 稳定公开入口 | `wecom-ha-entry` 唯一暴露端口（默认 `58087`），不随实例切换 |
| 非优雅终止接管 | `docker kill --signal=KILL` 后，存活实例接管网内 Bot 连接、任务执行、回复投递 |
| 重加入 | 受害实例显式 `start`，生成新 `process_start_id`，不复用旧 owner |
| 反向故障 | 对另一实例再执行一次强杀，对称验证 |
| 生命周期可观测 | 每阶段 `/statusz`、`/readyz`、容器 `State.Running` 断言与日志留存 |

> 不承诺：宿主机断电、Docker daemon、磁盘、共享 PostgreSQL / Redis 故障下的可用性（同处单一故障域）；跨主机 HA 需另行设计。详见 `OVERVIEW.md` 第 3 节与 `FULL-GUIDE.md` 第 11 节。

## 3. 文档地图

| 文档 | 内容 | 何时读 |
|---|---|---|
| `README.md`（本文件） | 索引、15 分钟复现骨架、范围声明 | 第一站 |
| `OVERVIEW.md` | 场景、术语表、三阶段业务故事、包含/不包含 | 想先理解"为什么要这样设计" |
| `FULL-GUIDE.md` | 完整独立说明：双实例、入口、live/ready、身份重加入、强杀时序、四类恢复、通过标准；**持续负载下的三类退化模式**、部署最小清单与重新加入序列、结论表述模板 | 想一次性读懂全部概念与退化判据 |
| `design.md` | 拓扑图、转发状态机、探测时序、身份模型、端口网络语义、编排设计、隔离/共享边界；**入口并发竞态分析**、健康判定矩阵、网络与安全边界 | 想看架构、竞态与安全边界 |
| `implementation-walkthrough.md` | 逐模块实现走读（入口配置→探测→转发→statusz；节点身份 owner；编排启动；脚本断言） | 想对照代码逐段理解 |
| `CODE-APPENDIX.md` | 逐段摘录关键实现代码并解释语义 | 想看真实代码片段 |
| `REFERENCE-IMPLEMENTATION.md` | **自足复现材料**：完整入口实现、完整 Compose、全部环境变量与默认值、端口分配、节点 `/statusz` 契约、健康判定矩阵、分步命令、验证清单 | 想从零照抄落地 |
| `testing-and-acceptance.md` | 完整测试方案：参数、逐步命令、脚本逐条解释、证据字段、`/statusz` 三快照、通过判定表、反例、持续负载检查 | 想跑验收或半自动判断 |
| `release-and-operations.md` | 配置、部署、发布顺序、观测指标与告警、回滚约束、故障处置手册 | 想上线与排障 |

## 4. 关键常量速查

| 项 | 值 |
|---|---|
| 节点服务名 | `wecom-ha-node-a` / `wecom-ha-node-b` |
| 入口服务名 | `wecom-ha-entry` |
| 一次性初始化 | `wecom-ha-bootstrap`（`webui-local-bootstrap`） |
| 节点默认宿主机端口 | `58088`（a）/ `58089`（b），容器内固定 `:8080` |
| 入口默认宿主机端口 | `58087`，容器内固定 `:8080` |
| 入口后端 | `http://wecom-ha-node-a:8080,http://wecom-ha-node-b:8080` |
| 入口探测周期默认 | `1s`（允许 `[100ms, 1m]`） |
| 请求体上限 | `2 MiB`（`2 << 20`） |
| `process_start_id` | 每次启动随机 8 字节 → 16 字符 hex |
| owner 命名 | `webui-local-<component>-<instance-id>-<process-start-id>` |
