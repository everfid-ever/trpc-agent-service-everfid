# 企微 AI Bot 多租户可靠性平台 — 交付文档索引

> **阅读原则：** 三个能力包都已扩写为**自足的复现文档包**。一个从未见过本仓库源码的工程师，仅凭对应目录下的 Markdown，即可实现等价系统并通过同等验收；每个包内的 `REFERENCE-IMPLEMENTATION.md` 提供完整 DDL、Redis Lua 契约、Go 接口、核心算法、配置全表、启动顺序与验证清单。文档中引用仓库路径统一写成「（仓库对应位置，可选核对：`path`）」，**不作为唯一说明**。

**文档状态：** 双 tenant/Bot bootstrap、双节点 runtime、稳定且 readiness-aware 的 WeCom callback 入口、非优雅终止 smoke、P1–P4 可控故障点、ownership/状态观测，以及单主机 N1/N2 的强杀、重新加入和反向故障演练均在仓库内实现并有对应演练脚本。真实 WeCom 会话验收由外部凭据和手机端会话决定，默认按 `TRPC_WECOM_REAL_ACCEPTANCE=assumed` 记为外部前置。

---

## 〇、平台定位与保护对象（先读这节再看能力）

### 0.1 一句话定位

**本平台是企业微信侧 AI Bot（LLM Agent）服务的多租户可靠性层**：Bot 负责对话推理与工具执行，本平台负责让它在**应用节点故障、在途任务中断、单主机单实例失效**这三种情况下仍然正确、可恢复、不串租户、不重复交付。

主题围绕三条主线，分别对应三个交付包：

| 主线 | 交付包 | 一句话 |
|---|---|---|
| 节点故障后的**会话连续性** | `conversation-continuity/` | 节点挂了，用户在**原会话**继续问，系统仍答得出上下文、不串租户、只回一条 |
| **在途任务恢复** | `inflight-task-takeover/` | 故障前那条"还没回完"的原消息，由存活节点自动接着办，用户不用重发 |
| **单机多实例可用性** | `single-host-multicontainer/` | 同一台机器上跑多个实例，一个被强杀另一个顶上，还能重新加入与反向接管 |

### 0.2 被保护的对象是什么（Bot 的业务形态）

可靠性保护的不是"一个 HTTP 接口"，而是一台**带模型、工具、技能、知识、记忆与产物的 LLM Agent**。理解这一点，才能理解为什么下面这些约束是必需的：

| Bot 的组成部分 | 承载方式 | 与可靠性的关系（为什么要保护它） |
|---|---|---|
| **模型推理** | 模型 Profile（本地为 DeepSeek 等） | 推理内部状态无法跨节点恢复 → P2 接管**允许重跑模型**，但只允许一次有效提交 |
| **工具执行** | 工具目录（tenant 范围内可用的工具集） | 工具可能**写外部系统** → 必须业务幂等键 + 对账，禁止盲目重试 |
| **技能（Skill）** | 版本化技能包，落盘到技能暂存根 | 暂存是**唯一的共享可写本地状态** → 写入协议必须并发收敛（见 `single-host-multicontainer/design.md` §9.2） |
| **知识检索（RAG）** | 版本化 Knowledge 清单 + tenant 作用域向量检索 | 知识与**租户强绑定** → 跨节点接管后必须仍按 tenant 过滤，不得串库 |
| **记忆（Memory）** | tenant / app / user 作用域的会话记忆 | 记忆是**可追加事实** → 故障后不得重复写入 |
| **产物（Artifact）** | 受生命周期与保留策略管理的对象 | 产物的**幂等创建**与引用一致性依赖"一次提交"语义 |

> 因此文档中反复出现的"**一次有效提交 + 一次业务效果**"，保护的是上面这张表：模型可重跑、任务可重试，但**知识与记忆不得串租户、工具副作用不得重复、产物引用不得只落一半**。

### 0.3 三条能力与平台其他部分的关系（边界）

本平台**不负责** Bot 的能力本身（回答质量、工具丰富度、知识治理策略），只负责**这些能力在故障下不被破坏**。因此：

- 文档以"业务事实 → 谁写入 → 故障后由谁继续"的方式描述每条链路，不讨论 Prompt、模型选型或知识切分算法。
- 与 Bot 能力直接相关的组件（Qdrant 向量检索、Memory 后端、Artifact Store、模型 Provider）在本文档中作为**共享依赖或权威存储**出现，只交代它们在可靠性语义中的位置（是否需要租户过滤、是否需要幂等、是否可重放），不展开其内部实现。

### 0.4 术语对齐（项目描述用词 ↔ 本文档用词）

| 项目描述中的用词 | 本文档用词 | 说明 |
|---|---|---|
| 企微 | 企业微信（WeCom） | 同一平台；文档统一用"企业微信" |
| AI Bot | Bot / Agent | 文档按语境区分："**Bot**"指企业微信侧的应用身份（Corp + Agent ID），"**Agent**"指 LLM 智能体运行时，二者通过 **Bot Binding** 关联 |
| 单机多实例 | 单主机多容器实例 | 同义；文档强调"同一宿主机"这一故障域边界 |
| 节点故障 | 非优雅终止（SIGKILL）/ 网络隔离 | 文档区分两类，恢复机制不同（见 `conversation-continuity/FULL-GUIDE.md` §12） |
| 连接接管 | 稳定入口的 readiness 摘除与转发 + 消费者组回收 | ⚠️ 本架构为回调式，**不存在长连接转移**（见 `single-host-multicontainer/FULL-GUIDE.md` §3.1） |
| 任务租约 | `lease`（Redis，带 TTL 的执行许可） | — |
| Fencing Token | `fence`（单调递增接管权序号） | — |
| 交付 / 投递 | Delivery / 投递台账（`delivery_ledger`） | — |
| 用户无需重发 | "no user resend" 验收项 | — |

### 0.5 对外表述的诚实边界（⚠️ 与描述措辞的差异，需按场景选用）

项目描述的目标是"**保证**用户无需重发且每个输入只产生一次可见最终回复"。文档对此的准确表述是**分层承诺**：

| 层面 | 承诺强度 | 依据 |
|---|---|---|
| 输入不丢、用户无需重发 | ✅ 单应用节点故障下**保证** | `inbox` durable + 队列 reclaim + 共享权威态 |
| 一次有效提交、一次业务效果 | ✅ **保证** | `commit_turn` 的四重校验（request_id + input_seq + expected_version + fence） |
| 用户最终只看到一条回复 | ⚠️ **在下游可去重/可查询的边界内保证**；若下游不可去重，只承诺**至少一次投递尝试**并诚实标记 `ambiguous` + 告警 | `delivery_ledger` 的 `client_request_id` 去重 + `reconcile` |

**为什么保留这个差异而不是把措辞"对齐"成绝对口径：** "请求已发出、响应丢失"是分布式系统的客观窗口——系统无法凭空判断下游是否已受理。把这一条写成绝对恰好一次属于**无法证伪的过度承诺**，且验收时会因下游不可去重而无法通过。

**建议的对外表述（可直接用于项目介绍 / 汇报）：**

> 在单应用节点故障场景下，已持久接收的消息**无需用户重发**，系统保证**一次有效的结果提交与一次业务效果**；用户最终可见回复**在下游支持请求级去重的边界内恰好一次**，超出该边界时系统不谎报成功，而是显式标记为不确定并告警。

---

## 0. 怎么读（按角色选路径）

> 同一套文档里有三种内容密度：**业务叙述**（无代码）、**机制说明**（含少量伪代码/表名）、**实现材料**（DDL / Go / SQL / Lua，供照抄）。
> 下表告诉你每类读者读什么、**可以跳过什么**。带 🟢 的文档完全不含代码，可以全文读完。

| 角色 | 建议顺序 | 可以安全跳过 |
|---|---|---|
| **非代码读者**（管理、产品、业务、交接对象） | ① [STANDALONE-SOLUTION-GUIDE.md](./STANDALONE-SOLUTION-GUIDE.md) 🟢 全文 → ② 三个包的 `OVERVIEW.md` 🟢（先看"技术名 → 大白话"对照表，再看业务故事）→ ③ 本文 §〇（平台定位）、§四 能力边界、§五 三类计数 | 所有含 Go / SQL / Lua / YAML 代码块的章节；`REFERENCE-IMPLEMENTATION.md`、`CODE-APPENDIX.md`、`design.md`、`implementation-walkthrough.md`、`testing-and-acceptance.md`、`release-and-operations.md`、`DATABASE-DESIGN.md` 可整篇跳过 |
| **工程实现者** | ① [REFERENCE-RUNTIME-SPEC.md](./REFERENCE-RUNTIME-SPEC.md)（跨包契约）→ ② [DATABASE-DESIGN.md](./DATABASE-DESIGN.md)（库架构 / 迁移 / 隔离级别 / 加密 / 索引 / 生命周期）→ ③ 目标包的 `README.md` → `design.md` → `implementation-walkthrough.md` → ④ `REFERENCE-IMPLEMENTATION.md`（照抄落地）→ ⑤ `CODE-APPENDIX.md`（对照真实实现） | 无（全部相关） |
| **DBA / 数据平台** | ① [DATABASE-DESIGN.md](./DATABASE-DESIGN.md) 全文（尤其 §2 迁移演进、§8 只增表与清理责任、§10 备份与恢复语义）→ ② 各包 `release-and-operations.md` 的回滚约束 | 纯业务叙述章节 |
| **运维 / SRE / 值班** | ① 目标包的 `release-and-operations.md`（观测、告警、回滚、处置、对账）→ ② `testing-and-acceptance.md`（判定表与结论模板）→ ③ [DATABASE-DESIGN.md](./DATABASE-DESIGN.md) §8/§9（只增表、连接池）→ ④ `FULL-GUIDE.md` 的边界与不承诺章节 | `REFERENCE-IMPLEMENTATION.md` 的 DDL 部分、`CODE-APPENDIX.md` |
| **验收 / QA** | ① 目标包 `testing-and-acceptance.md` → ② `OVERVIEW.md` §范围与不包含 → ③ 对应演练脚本的说明章节 | `design.md` 的竞态分析、`implementation-walkthrough.md` |

**非代码读者需要知道的一件事：** 文档里出现的 `` `inbox` ``、`` `delivery_ledger` ``、`` `session_commit` `` 等反引号名字是**系统内部账本的名字**，不是需要你理解的代码。每个 `OVERVIEW.md` 都提供了"技术名 → 大白话"对照表；读不下去时回到那张表即可。

**读完非代码路径后，你应该能独立回答这 7 个问题：**

1. 一条消息要经历哪几个阶段，每个阶段"已经算数"的判据是什么？
2. 节点在任意一个阶段挂掉，系统分别会怎么做？为什么不能一概而论？
3. 为什么"又起了一个容器"不等于"能接管"？
4. 什么情况下用户可能需要重新发消息？（正解：单应用节点故障下**不需要**）
5. 为什么"模型只调用了一次"不是一个正确的验收标准？
6. 为什么有些情况下系统只能保证"至少一次"，不能承诺"恰好一次"？
7. 旧节点"诈尸"回来，为什么不能覆盖新节点的结果？

---

## 一、三个能力包

| 交付包 | 解决的问题 | 包内入口 | 复现材料 |
|---|---|---|---|
| **故障恢复后继续对话** | 基于租户 ID、Bot Binding、会话 ID 与消息 ID 建立持久化会话链路；单应用节点故障后，连接与 Worker 自动接管，两个 Bot 在原会话继续正确对话，上下文不丢、租户归属与最终回复不串扰 | [conversation-continuity/README.md](./conversation-continuity/README.md) | [REFERENCE-IMPLEMENTATION.md](./conversation-continuity/REFERENCE-IMPLEMENTATION.md) |
| **在途任务接管与幂等投递** | 覆盖"已持久化未执行 / 模型或工具执行中 / 结果已提交未发送 / 下游已接受未确认"四个故障窗口；通过任务租约、执行权回收、Fencing Token、Inbox/Outbox 与 Delivery Ledger 自动续处理或安全重试；区分模型调用次数、任务尝试次数与最终回复次数 | [inflight-task-takeover/README.md](./inflight-task-takeover/README.md) | [REFERENCE-IMPLEMENTATION.md](./inflight-task-takeover/REFERENCE-IMPLEMENTATION.md) |
| **单主机多容器实例高可用** | 同一主机内多实例共享数据库、队列与协调服务，通过独立节点身份、端口与本地状态隔离避免冲突；单实例被非优雅终止后由存活实例接管 Bot 连接、任务执行与回复投递；覆盖重新加入、反向故障与持续负载下的稳定性检查 | [single-host-multicontainer/README.md](./single-host-multicontainer/README.md) | [REFERENCE-IMPLEMENTATION.md](./single-host-multicontainer/REFERENCE-IMPLEMENTATION.md) |

---

## 二、跨包共享文档

| 文档 | 读者 | 内容 |
|---|---|---|
| [STANDALONE-SOLUTION-GUIDE.md](./STANDALONE-SOLUTION-GUIDE.md) | 非代码读者 / 管理层 / 交接对象 | 可独立分发的完整方案说明：业务背景、角色、状态机、接管时序、技术选择理由、证据与边界；**不依赖代码仓库** |
| [TEACHING-GUIDE.md](./TEACHING-GUIDE.md) | 工程实现者 | 原理、代码走读与演练方式的统一入口：权威状态分离、租约/fence、P1–P4、双实例入口，含阅读顺序与审阅清单 |
| [REFERENCE-RUNTIME-SPEC.md](./REFERENCE-RUNTIME-SPEC.md) | 需要跨包实现契约的人 | 三个包共同依赖的最小运行时：部署单元、配置契约（含装配值与兜底值的区分）、原子写入规则、Redis 键与 Lua 契约、角色接口、错误语义契约、能力边界 |
| [DATABASE-DESIGN.md](./DATABASE-DESIGN.md) | 后端实现者 / DBA / 运维 | **数据库设计与实现**：双库架构（业务库 + 合规库）、schema 基线与迁移演进（`schema_migrations` checksum 漂移拒绝）、**隔离级别与行锁策略**、锁序纪律、**消息正文的 AES-256-GCM 加密存储与 AAD 租户绑定**、`SECURITY DEFINER` + `search_path` 安全约定、触发器职责（含 `session_event` 拆包与 **wakeup 事件来源**）、关键部分索引、**只增表清单与清理责任**、连接池实现事实（应用节点未设上限）、备份与恢复边界 |

---

## 三、每个包内的文档结构

| 文档 | 内容 |
|---|---|
| `README.md` | 索引 + 「15 分钟复现直达」最少步骤 + 已实现范围声明 + 文档地图 |
| `OVERVIEW.md` | 场景、术语表、端到端业务故事、范围与不包含（不读代码也能理解） |
| `FULL-GUIDE.md` | 完整原理与因果链：为什么不能只做 X、故障场景分析、判据与边界 |
| `design.md` | 架构、数据模型（真实字段）、状态机、时序图、模块划分、错误语义、并发竞态分析 |
| `implementation-walkthrough.md` | 逐模块实现走读，每步给出 durable truth 与"此刻被杀会怎样" |
| `CODE-APPENDIX.md` | 关键实现代码逐段摘录 + 状态变化与故障语义解释 + 设计结论↔代码证据索引 |
| `REFERENCE-IMPLEMENTATION.md` | **从零复现材料**：完整 DDL / Lua / Go 接口 / 算法 / 配置全表 / 启动顺序 / 验证清单 |
| `testing-and-acceptance.md` | 参数、逐步命令、脚本逐条解释、证据字段、通过判定表、稳定性与负载检查、反例清单、真实验收步骤 |
| `release-and-operations.md` | 配置门禁、部署、发布顺序、观测指标、告警、回滚约束、分场景故障处置手册 |

---

## 四、能力边界（三个包共同声明）

- ✅ 已持久接收的消息不会因**单个应用节点故障**而需要用户重新发送。
- ✅ 共享状态中的 tenant、会话与业务结果能够被存活节点读取；同一会话的旧执行者不能用更小 fence 覆盖新执行者。
- ✅ 已提交结果不会因发送节点故障而重跑模型；重加入以**新进程身份**参与协调。
- ❌ **不承诺**跨主机、整机断电、宿主机网络隔离、Docker daemon、共享磁盘或共享 PostgreSQL/Redis 故障下的可用性（同属单一故障域）。
- ❌ **不承诺**模型调用次数恒为 1（P2 接管可重跑模型）。
- ❌ **不承诺**下游无法按 `client_request_id` 去重或查询时 P4 的绝对恰好一次（诚实 `ambiguous` + 告警）。
- ❌ **不把** Docker 自动重启（编排已设 `restart: "no"`）或健康/连接/队列恢复单独当作完整业务接管与 RTO。

---

## 五、三类计数必须分开表述

| 计数 | 承载位置 | 会否因接管增加 |
|---|---|---|
| 模型调用次数 | 执行期模型调用 | ✅ 会（P2 接管允许重跑） |
| 任务尝试次数 | `execution_record.park_attempt`、`delivery_ledger.attempt`、`reconcile_attempt` | ✅ 会（reclaim / retry 增长） |
| 最终回复次数 | `delivery_ledger` 每 segment 一行，`state='sent'` 后不可重发 | ❌ 不会（幂等约束保证） |

**验收目标应表述为：** 一次有效 terminal commit + 一次业务效果 + 用户无需重发 + 每个输入只产生一次可见最终回复。而不是"模型只调用了一次"。

---

## 六、演练脚本索引

| 脚本 | 覆盖 | 包 |
|---|---|---|
| `scripts/e2e/wecom-ha-failover.sh` | 双 Bot/双节点基线 + 单节点 SIGKILL + 入口与存活节点可用性 + 受害节点未自动重启 | conversation-continuity |
| `scripts/e2e/inflight-takeover.sh` | 按 P1–P4 精确命中边界 → 强杀真实 owner → 释放存活节点 → 观察收敛 | inflight-task-takeover |
| `scripts/e2e/single-host-multicontainer.sh` | 双实例共存 → 强杀 N1 → 接管 → 新进程身份重加入 → 反向强杀 N2 | single-host-multicontainer |

三个脚本均不伪造企业微信用户消息；真实双 Bot 原会话与最终可见回复是外部验收前置（`TRPC_WECOM_REAL_ACCEPTANCE=assumed` 默认，`recorded` 表示已保存外部证据）。

---

## 七、需求 → 文档覆盖矩阵（可逐条核验）

> **平台层对齐见 §〇**：平台定位（§0.1）、被保护对象即 Bot 的业务形态（§0.2）、与 Bot 能力的边界（§0.3）、描述用词 ↔ 文档用词对照（§0.4）、对外表述的诚实边界（§0.5）。
> 本节只覆盖三条能力各自的子项。

下表把三项能力各自的需求逐条拆开，标明**在哪篇文档的哪一节**可以读到实现说明。带 ⭐ 的是本轮为填补覆盖空白而新增的章节。

### 能力一：故障恢复后继续对话（`conversation-continuity`）

| 需求子项 | 覆盖位置 | 是否可"照着实现" |
|---|---|---|
| 租户 ID / Bot Binding / 会话 ID / 消息 ID 的**持久化会话链路** | `design.md` §二（四元组链路 + 各阶段主键/唯一键）、§三（真实字段）、`REFERENCE-IMPLEMENTATION.md` §2（完整 DDL）、`FULL-GUIDE.md` §4 | ✅ 有完整 DDL 与唯一键语义 |
| 故障后**连接**接管 | `FULL-GUIDE.md` §7 ⭐**§7.1 术语澄清**（回调式架构下"连接接管"= 公开地址不变 + 新回调转健康节点 + Bot 身份本就是共享配置）、`implementation-walkthrough.md` §一 | ✅ 澄清了不存在"长连接转移"这一误解 |
| 故障后 **Worker** 接管 | `design.md` §四、`implementation-walkthrough.md` §五、`CODE-APPENDIX.md` §5–§6、`REFERENCE-IMPLEMENTATION.md` §6.1 | ✅ 含 `handle` 13 步与条件更新 |
| 恢复**两个 Bot** 的消息处理能力 | `design.md` §三（双 Bot 校验）、`CODE-APPENDIX.md` §2、`REFERENCE-IMPLEMENTATION.md` §7 | ✅ 含 `(Corp,Agent)` 互异校验 |
| 原会话中**正确回忆上下文** | `design.md` §二、`implementation-walkthrough.md` §六（会话读取路径）、`FULL-GUIDE.md` §15（四层一致判据 + 跨租户污染 SQL）、⭐`DATABASE-DESIGN.md` §1.1（**转写权威是官方会话后端 `session_events`，不是平台 `session_event`**） | ✅ 含可执行的核对 SQL 与表族澄清 |
| **租户归属不串扰** | `FULL-GUIDE.md` §3、`design.md` §九（SQLSTATE 语义）、`CODE-APPENDIX.md` §11（候选令牌四阶段，tenant 只在 promote 后带出） | ✅ 含"请求体 tenant 不可信"的实现依据 |
| **最终回复不串扰** | `FULL-GUIDE.md` §8（三类计数）、§9（P3/P4 对账）、`design.md` §四.3（台账状态机） | ✅ |

### 能力二：在途任务接管与幂等投递（`inflight-task-takeover`）

| 需求子项 | 覆盖位置 | 是否可"照着实现" |
|---|---|---|
| **P1** 已持久化未执行 | `FULL-GUIDE.md` §3、`OVERVIEW.md` §2/§3、`design.md` §三、`implementation-walkthrough.md` §二、`CODE-APPENDIX.md` §3 | ✅ 含挂点代码与命中前/后状态 |
| **P2** 模型或工具执行中 | 同上 §4 / §三 / §三 / §5 / §4 | ✅ |
| ⭐**P2 子窗口：工具调用执行中崩溃**（"工具执行记录保住了吗？接管后会不会接着跑？"） | ⭐`FULL-GUIDE.md` §4.5（崩溃瞬间 durable 清单、接管 9 步、"继续执行"的准确语义）+ ⭐`REFERENCE-IMPLEMENTATION.md` §10（转写权威、事件持久化判定式、`[orphan_tool_call]` 清洗规则、核对 SQL、复现要点） | ✅ 明确回答：**工具执行进度无记录、不做断点续传；以同一输入重开一轮，模型据转写痕迹重新决策**；`ask` 工具例外（`tool_attempt` 账本，不重跑只对账） |
| ⭐**工具级 durable 记录的边界（`allow` vs `ask`）** | ⭐`REFERENCE-IMPLEMENTATION.md` §1.10（`tool_attempt`/`tool_result_payload` 逐字 DDL + 状态机）、§10.5（`ConsumeGrant` → 执行 → `PutToolResult` → `FinishToolAttempt` 顺序与接管分支） | ✅ 含"`effect_unknown` 宁可承认不确定也不重跑"的判定 |
| ⭐**有副作用工具的重复执行风险** | ⭐`FULL-GUIDE.md` §4.5.5（恢复粒度是"一轮"不是"一步"；副作用不回滚；必须自带业务幂等键；两条升级路径——改 `ask` 或 Graph checkpoint，并说明 checkpoint 为何也救不了自动接管：平台不传 resume 坐标 + 每次重跑落在新 lineage） | ✅ 明确列为不承诺项，避免验收误判 |
| **P3** 结果已提交未发送 | 同上 §5 / §三 / §三 / §四 / §5 | ✅ |
| **P4** 下游已接受未确认 | 同上 §6、`design.md` §六.2、`CODE-APPENDIX.md` §5、`FULL-GUIDE.md` §18 下游能力矩阵 | ✅ 含诚实边界判定表 |
| **任务租约** | `design.md` §四（acquire/renew/release 语义）、`REFERENCE-IMPLEMENTATION.md` §3（Lua 逐字） | ✅ |
| **执行权回收** | `implementation-walkthrough.md` §九（reclaim 与 ACK 时序）、`design.md` §九 | ✅ |
| **Fencing Token** | `design.md` §四、`FULL-GUIDE.md` §17（为何 Redis 重启不会让旧 owner 翻案）、`REFERENCE-IMPLEMENTATION.md` §3（`ensureFence`） | ✅ |
| **Inbox / Outbox** | `REFERENCE-IMPLEMENTATION.md` §1.1/§1.4（DDL）、§2.1/§2.2（`claim_inbox`/`prepare_dispatch`）、⭐**§6.5 Outbox 发布生命周期**（claim/publish/续租/重试四条条件更新 + 可达状态表，含"通用 relay 不写 `dead_letter`"的实现事实） | ✅ 本轮补上了此前缺失的发布状态机 |
| **Delivery Ledger** | `REFERENCE-IMPLEMENTATION.md` §1.8（DDL）、§6.1–§6.4（CAS SQL）、`CODE-APPENDIX.md` §5–§6 | ✅ |
| **自动续处理 / 安全重试** | ⭐**`FULL-GUIDE.md` §2.1 可重试性判定表**：9 种故障位置 → 恢复类别（安全重试 / 续处理 / 不可重试需对账）+ 允许与禁止的动作 + 为什么 | ✅ 本轮补上了此前只有一句原则、缺判定表的问题 |
| **区分三种计数** | ⭐`FULL-GUIDE.md` §2.1 末尾对照表、`testing-and-acceptance.md` §4（核对 SQL）、`design.md` §5.3 | ✅ |
| **用户无需重发** | `FULL-GUIDE.md` §10、`testing-and-acceptance.md` §7（判定表与最小证据闭环） | ✅ |
| **每个输入只产生一次可见最终回复** | `REFERENCE-IMPLEMENTATION.md` §1.8、`testing-and-acceptance.md` §4/§6（每 segment `sent` 必须 = 1） | ✅ |

### 能力三：单主机多容器实例高可用（`single-host-multicontainer`）

| 需求子项 | 覆盖位置 | 是否可"照着实现" |
|---|---|---|
| 同一主机多实例**共享数据库、队列、协调服务** | `FULL-GUIDE.md` §2/§13、`design.md` §九、`REFERENCE-IMPLEMENTATION.md` §3（可直接照抄的 Compose） | ✅ |
| **独立节点身份** | `design.md` §二/§六、`REFERENCE-IMPLEMENTATION.md` §2（`/statusz` 契约）、§4 | ✅ |
| **独立端口** | `design.md` §七（端口网络语义表）、`REFERENCE-IMPLEMENTATION.md` §5 | ✅ |
| **本地状态隔离避免实例冲突** | ⭐**`design.md` §9.1 本地状态逐项交代**（三类本地状态 + 冲突风险 + 消除机制）、⭐**§9.2 技能暂存为何并发不冲突**（内容寻址路径 + 临时目录原子改名 + 幂等校验 + 发布前 rehash，并如实写明"并发改名失败需重试收敛"这一边界） | ✅ 本轮修正了此前"只读/并发安全"的未经验证断言 |
| 存活实例接管 **Bot 连接** | ⭐**`FULL-GUIDE.md` §3.1 术语澄清**（回调式架构下"连接接管"的三件可核查之事；全渠道无长连接） | ✅ 本轮补上了概念澄清 |
| 存活实例接管 **任务执行** | `FULL-GUIDE.md` §7、`design.md` §五、`implementation-walkthrough.md` §八/§十二 | ✅ |
| 存活实例接管 **回复投递** | `FULL-GUIDE.md` §7、`design.md` §五、`release-and-operations.md` §7.3 | ✅ |
| **实例重新加入** | `FULL-GUIDE.md` §8/§20（9 步正确序列，第 ⑤ 步身份比对）、`implementation-walkthrough.md` §九 | ✅ |
| **反向故障** | `FULL-GUIDE.md` §8/§20、`testing-and-acceptance.md` §2、`CODE-APPENDIX.md` §10 | ✅ |
| 持续负载下**连接稳定性**检查 | `testing-and-acceptance.md` §7.1（采样方法 + 判据 + 反例）、`FULL-GUIDE.md` §19.1（机制根因） | ✅ |
| 持续负载下**任务积压**检查 | `testing-and-acceptance.md` §7.2、`FULL-GUIDE.md` §19.2 | ✅ |
| 持续负载下**重复回复**检查 | `testing-and-acceptance.md` §7.3/§7.4（含"分片 vs 真重复"判据）、`FULL-GUIDE.md` §19.3 | ✅ |

### 明确不在三个包承诺范围内的部分（避免误判"文档不全"）

| 事项 | 为什么不在包内 | 文档如何交代 |
|---|---|---|
| 真实企业微信侧的最终可见回复、平台重传阈值 | 依赖外部账号与手机端，仓库内无法自证 | 以 `TRPC_WECOM_REAL_ACCEPTANCE=assumed/recorded` 显式声明；`testing-and-acceptance.md` 给出人工验收步骤 |
| 跨主机 / 整机断电 / Docker daemon / 共享磁盘 / 共享 PostgreSQL·Redis 故障 | 与实例同处一个故障域，属另一层设计 | 每个包的 `FULL-GUIDE.md` 边界章节、`release-and-operations.md` §边界均声明"不承诺" |
| 下游不支持按 `client_request_id` 去重时的 P4 恰好一次 | 分布式系统客观限制，非实现不足 | `FULL-GUIDE.md` §18 下游能力矩阵 + 诚实 `ambiguous` + 告警 |
| 模型调用次数恒为 1 | P2 接管设计上允许多次 | 每个包均声明；`FULL-GUIDE.md` §2.1 明确"顺序即策略" |

### 数据库相关设计与实现的覆盖位置

| 数据库主题 | 覆盖位置 | 说明 |
|---|---|---|
| 表 / 字段 / CHECK / 唯一约束 DDL | 各包 `REFERENCE-IMPLEMENTATION.md` 的 DDL 章节 | 逐字 |
| SQL 函数（存储过程）语义 | 各包 `REFERENCE-IMPLEMENTATION.md` 的"关键 SQL 函数"章节 | `claim_inbox` / `prepare_dispatch` / `commit_turn` / `park_execution` / `guard_outbox_idempotency` 等 |
| 条件更新（CAS） | 各包 `REFERENCE-IMPLEMENTATION.md`（Delivery Ledger、Outbox） | 逐字四条 SQL |
| 事务边界 | `conversation-continuity/FULL-GUIDE.md` §14、`DATABASE-DESIGN.md` §3 | 单事务内完成哪些写入 |
| **隔离级别与行锁策略** | ⭐`DATABASE-DESIGN.md` §3 | 默认 READ COMMITTED + 显式行锁；为什么不用 SERIALIZABLE |
| **锁序纪律（防死锁）** | ⭐`DATABASE-DESIGN.md` §3.3 | 控制面 → inbox → session_head → execution_record → outbox |
| **双库架构（业务库 + 合规库）** | ⭐`DATABASE-DESIGN.md` §1 | 为何分离、为何必须分别 Runner |
| **Schema 基线、迁移演进、checksum 漂移拒绝、PG 16** | ⭐`DATABASE-DESIGN.md` §2 | 含本地验证与重置入口 |
| **消息正文加密存储（AES-256-GCM + AAD 租户绑定 + 密钥轮换）** | ⭐`DATABASE-DESIGN.md` §4 | 第二道租户隔离 |
| **`SECURITY DEFINER` + `search_path='pg_catalog'` 安全约定** | ⭐`DATABASE-DESIGN.md` §5 | 防提权与 search_path 劫持 |
| **触发器职责（含 `session_event` 拆包、wakeup 事件来源）** | ⭐`DATABASE-DESIGN.md` §6 | 解释了 `wakeup` kind 由谁产生 |
| 索引与其服务的查询 | `DATABASE-DESIGN.md` §7 | 强调部分索引的设计纪律 |
| **只增表清单与清理责任（运维盲区）** | ⭐`DATABASE-DESIGN.md` §8 | 可靠性表无自动清理路径，须运维决定策略 |
| **连接池：本平台节点未显式设置（无上限、连接不过期）** | ⭐`DATABASE-DESIGN.md` §9 | 与其他角色设 16 的印象相反；池耗尽先表现为 `lease_lost`，且故障时可用连接数低于静态规划 |
| **备份/恢复与恢复后对账语义** | ⭐`DATABASE-DESIGN.md` §10 | 含"恢复后 fence 会回退但会被校准、结果需与下游对账" |

> **自检方法：** 若你想验证"读完能否理解实现"，可任选上表一行，按标注的文档与章节阅读，然后尝试回答："这一步的 durable truth 是什么？此刻进程被杀会怎样？如何证明它生效？"——若三问都能答出，说明该子项覆盖完整。
